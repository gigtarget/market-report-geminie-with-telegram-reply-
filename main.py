import asyncio
import csv
import fcntl
import logging
import os
import tempfile
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import math
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import yfinance as yf
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from nse_fiidii import FiiDiiData, get_fii_dii_data
from openai_news import fetch_india_market_news_openai
from post_market_highlights import build_post_market_highlights
from templates import classify_market, get_opening_line, initialize_templates_store

IST = ZoneInfo("Asia/Kolkata")
FETCH_PERIOD = "10d"
FETCH_INTERVAL = "1d"
CACHE_TTL_SECONDS = 120

INDEX_TICKERS: Dict[str, str] = {
    "Nifty 50": "^NSEI",
    "Sensex": "^BSESN",
    "Nifty Bank": "^NSEBANK",
}

# Extra context for the report (Yahoo Finance symbols).
VIX_TICKER = "^INDIAVIX"  # INDIA VIX

# NSE sector indices on Yahoo Finance (keep this list stable for consistent daily output).
SECTOR_TICKERS: Dict[str, List[str]] = {
    "IT": ["^CNXIT"],
    "PSU Banks": ["^CNXPSUBANK"],
    "Private Banks": ["^CNXPRIVAT", "^NIFTYPVTBANK"],
    "Realty": ["^CNXREALTY"],
    "FMCG": ["^CNXFMCG"],
    "Energy": ["^CNXENERGY"],
    "Auto": ["^CNXAUTO"],
    "Pharma": ["^CNXPHARMA"],
    "Metal": ["^CNXMETAL"],
    "Infra": ["^CNXINFRA"],
}

_POLLING_STARTED = False
_POLLING_LOCK_HANDLE = None
_POLLING_LOCK_PATH = os.path.join(tempfile.gettempdir(), "telegram_bot_poller.lock")


@dataclass
class IndexSnapshot:
    name: str
    close: float
    previous_close: float
    change: float
    percent_change: float


@dataclass
class VixSnapshot:
    value: float
    percent_change: float


@dataclass
class SectorMove:
    sector: str
    percent_change: float


@dataclass
class BreadthSnapshot:
    total: int
    advances: int
    declines: int
    unchanged: int
    coverage_note: Optional[str] = None


@dataclass
class KeyLevels:
    name: str
    method: str
    pivot: float
    r1: float
    s1: float
    r2: float
    s2: float


@dataclass
class MarketReport:
    session_date: date
    indices: List[IndexSnapshot]
    last_timestamp_ist: datetime
    generated_at_utc: datetime
    market_closed: bool
    from_cache: bool = False
    warning: Optional[str] = None
    vix: Optional[VixSnapshot] = None
    vix_warning: Optional[str] = None
    sector_moves: Optional[List[SectorMove]] = None
    sector_warning: Optional[str] = None
    fii_dii: Optional[FiiDiiData] = None
    fii_dii_warning: Optional[str] = None
    top_gainers: List["StockMover"] | None = None
    bottom_performers: List["StockMover"] | None = None
    movers_warning: Optional[str] = None
    breadth: Optional[BreadthSnapshot] = None
    key_levels: Optional[Dict[str, KeyLevels]] = None
    drivers: Optional[List[str]] = None
    news_lines: List[str] | None = None
    news_warning: Optional[str] = None
    liveblog_highlights: Optional[List[str]] = None
    liveblog_warning: Optional[str] = None


@dataclass
class StockMover:
    symbol: str
    close: float
    previous_close: float
    change: float
    percent_change: float


@dataclass
class NewsDigest:
    lines: List[str]
    warning: Optional[str] = None


_REPORT_CACHE: Dict[str, Optional[object]] = {"report": None, "timestamp": None}


def _format_number(value: float) -> str:
    return f"{value:,.2f}"


def _format_change(value: float) -> str:
    return f"{value:+,.2f}"


def _breadth_read_line(breadth: BreadthSnapshot) -> str:
    adv = breadth.advances
    dec = breadth.declines
    if dec == 0:
        ratio = float("inf")
        ratio_display = "∞"
    else:
        ratio = adv / dec
        ratio_display = f"{ratio:.2f}"

    if ratio >= 1.25:
        label = "mildly positive"
    elif ratio >= 0.90:
        label = "neutral"
    else:
        label = "weak"

    return f"Breadth read: A/D = {adv}/{dec} ({ratio_display}) → {label} internals"


def _load_nifty_100_tickers() -> Tuple[List[str], Optional[str]]:
    csv_path = Path(__file__).with_name("ind_nifty100list.csv")
    tickers: List[str] = []
    warning: Optional[str] = None

    try:
        with csv_path.open(newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            if not reader.fieldnames:
                warning = "NIFTY 100 list is empty or missing headers; skipping movers."
                return [], warning

            symbol_key = None
            for field in reader.fieldnames:
                if field and field.strip().lower() == "symbol":
                    symbol_key = field
                    break

            if not symbol_key:
                warning = "NIFTY 100 CSV does not contain a 'Symbol' column; skipping movers."
                return [], warning

            symbols = set()
            for row in reader:
                raw_symbol = row.get(symbol_key, "")
                symbol = raw_symbol.strip().upper()
                if symbol:
                    symbols.add(symbol)

            if not symbols:
                warning = "NIFTY 100 list is empty; skipping movers."
                return [], warning

            tickers = [f"{symbol}.NS" for symbol in sorted(symbols)]
    except FileNotFoundError:
        warning = "NIFTY 100 list not found; skipping movers."
    except Exception as exc:  # noqa: BLE001
        logging.warning("Failed to read NIFTY 100 list: %s", exc)
        warning = "Unable to read NIFTY 100 list; skipping movers."

    return tickers, warning


def _determine_summary(indices: List[IndexSnapshot]) -> str:
    positives = sum(1 for idx in indices if idx.change > 0)
    negatives = sum(1 for idx in indices if idx.change < 0)

    if positives and not negatives:
        return "Markets closed higher across major indices."
    if negatives and not positives:
        return "Markets closed lower across major indices."
    if positives and negatives:
        return "Indian markets ended mixed at the close."
    return "Major indices were little changed at the close."


def _pct_change(close: float, prev_close: float) -> float:
    if prev_close == 0:
        return 0.0
    return (close - prev_close) / prev_close * 100


def _normalize_sector_key(value: str) -> str:
    return " ".join(value.split()).strip().lower()


def _fetch_vix_snapshot() -> Tuple[Optional[VixSnapshot], Optional[str]]:
    try:
        history = _fetch_history(VIX_TICKER, period="6d", interval="1d")
        clean = history.dropna(subset=["Close"])
        if len(clean) < 2:
            return None, "Volatility (INDIA VIX): unavailable."
        close = float(clean.iloc[-1]["Close"])
        prev_close = float(clean.iloc[-2]["Close"])
        return VixSnapshot(value=close, percent_change=_pct_change(close, prev_close)), None
    except Exception as exc:  # noqa: BLE001
        logging.warning("VIX fetch failed: %s", exc)
        return None, "Volatility (INDIA VIX): unavailable."


def _fetch_sector_moves() -> Tuple[Optional[List[SectorMove]], Optional[str]]:
    expected_sectors = list(SECTOR_TICKERS.keys())
    normalized_expected = {_normalize_sector_key(name): name for name in expected_sectors}
    sector_returns: Dict[str, Optional[float]] = {name: None for name in expected_sectors}
    missing: List[str] = []

    for sector, tickers in SECTOR_TICKERS.items():
        display_name = normalized_expected.get(_normalize_sector_key(sector), sector)
        percent_change: Optional[float] = None
        for ticker in tickers:
            try:
                history = _fetch_history(ticker, period="6d", interval="1d")
                clean = history.dropna(subset=["Close"])
                if len(clean) < 2:
                    continue
                close = float(clean.iloc[-1]["Close"])
                prev_close = float(clean.iloc[-2]["Close"])
                percent_change = _pct_change(close, prev_close)
                break
            except Exception as exc:  # noqa: BLE001
                logging.warning("Sector fetch failed for %s (%s): %s", sector, ticker, exc)

        if percent_change is None or not math.isfinite(percent_change):
            missing.append(display_name)
            sector_returns[display_name] = None
        else:
            sector_returns[display_name] = percent_change

    moves = [
        SectorMove(sector=sector, percent_change=percent_change)
        for sector, percent_change in sector_returns.items()
        if percent_change is not None and math.isfinite(percent_change)
    ]

    total = len(expected_sectors)
    coverage_count = sum(
        1 for percent_change in sector_returns.values()
        if percent_change is not None and math.isfinite(percent_change)
    )
    coverage_line = f"Sector coverage: {coverage_count}/{total}"
    if missing:
        coverage_line += f" (missing: {', '.join(missing)})"

    if not moves:
        return None, coverage_line

    moves.sort(key=lambda item: item.percent_change, reverse=True)
    return moves, coverage_line


def _sector_trend_block(moves: Optional[List[SectorMove]], coverage_line: Optional[str]) -> List[str]:
    # Always print whatever data we have (even if partial).
    if not moves:
        lines = ["Sector Trend: unavailable."]
        if coverage_line:
            lines.append(coverage_line)
        return lines

    strength = [m.sector for m in moves if m.percent_change >= 0.50]
    weakness = [m.sector for m in moves if m.percent_change <= -0.50]
    neutral = [m.sector for m in moves if -0.50 < m.percent_change < 0.50]

    def _fmt(items: List[str]) -> str:
        return ", ".join(items[:6]) if items else "None"

    lines: List[str] = []
    lines.append("Sector Trend:")
    if coverage_line:
        lines.append(coverage_line)

    lines.extend([
        f"• Strength: {_fmt(strength)}",
        f"• Weakness: {_fmt(weakness)}",
        f"• Neutral: {_fmt(neutral)}",
        "",
        "Sector Moves (%):",
    ])

    # Print each sector with signed % (sorted already: strongest -> weakest)
    for m in moves:
        lines.append(f"• {m.sector}: {m.percent_change:+.2f}%")

    return lines


def _weakest_sector(moves: Optional[List[SectorMove]]) -> Optional[str]:
    if not moves:
        return None
    return sorted(moves, key=lambda item: item.percent_change)[0].sector


def _strongest_sector(moves: Optional[List[SectorMove]]) -> Optional[str]:
    if not moves:
        return None
    return sorted(moves, key=lambda item: item.percent_change, reverse=True)[0].sector


def _vix_line(vix: Optional[VixSnapshot], warning: Optional[str]) -> str:
    if warning:
        return warning
    if not vix:
        return "Volatility (INDIA VIX): unavailable."
    arrow = "↑" if vix.percent_change > 0 else "↓" if vix.percent_change < 0 else "→"
    return f"Volatility (INDIA VIX): {vix.value:.2f} ({arrow} {vix.percent_change:+.2f}%)."


def _report_header(report: "MarketReport") -> str:
    label = "Post Market Report" if report.market_closed else "Post Market Report"
    return f"{label}: {report.session_date.strftime('%Y-%m-%d')}"


def _market_structure_line(
    report: "MarketReport",
    weakest_sector: Optional[str],
    vix: Optional[VixSnapshot],
) -> str:
    indices_pct = {idx.name: idx.percent_change for idx in report.indices}
    nifty_pct = indices_pct.get("Nifty 50", 0.0)

    breadth_bias = "mixed"
    if report.breadth:
        if report.breadth.advances > report.breadth.declines:
            breadth_bias = "positive"
        elif report.breadth.advances < report.breadth.declines:
            breadth_bias = "negative"
    elif report.top_gainers and report.bottom_performers:
        if len(report.bottom_performers) > len(report.top_gainers):
            breadth_bias = "negative"
        elif len(report.top_gainers) > len(report.bottom_performers):
            breadth_bias = "positive"

    fii_net = 0.0
    if report.fii_dii and report.fii_dii.fii:
        fii_net = float(report.fii_dii.fii.net)

    flow_bias = "flat FII"
    if fii_net > 0:
        flow_bias = "FII buying"
    elif fii_net < 0:
        flow_bias = "FII selling"

    vol_tag = ""
    if vix:
        if vix.percent_change >= 2.0:
            vol_tag = " + volatility rising"
        elif vix.percent_change <= -2.0:
            vol_tag = " + volatility cooling"

    if nifty_pct < 0 and fii_net < 0 and breadth_bias == "negative":
        core = "Distribution day — weak close with FII selling and broad softness"
    elif nifty_pct > 0 and fii_net > 0 and breadth_bias == "positive":
        core = "Accumulation day — firm close with FII support and broad participation"
    elif nifty_pct < 0:
        core = "Soft day — sellers controlled into the close"
    elif nifty_pct > 0:
        core = "Constructive day — buyers defended the close"
    else:
        core = "Indecision day — rangebound close"

    sector_tag = f"; key drag: {weakest_sector}" if weakest_sector else ""
    return f"Market Structure: {core}{sector_tag} ({flow_bias}{vol_tag})."


def _tomorrows_focus(report: "MarketReport", weakest_sector: Optional[str]) -> List[str]:
    bullets: List[str] = []

    def _nifty_rule(levels: KeyLevels) -> str:
        return (
            f"Nifty rule: Hold S1 (~{levels.s1:,.0f}) and reclaim Pivot (~{levels.pivot:,.0f})"
            f" → aim R1 (~{levels.r1:,.0f}); below S1 → watch S2 (~{levels.s2:,.0f})."
        )

    def _banknifty_rule(levels: KeyLevels) -> str:
        return (
            f"BankNifty rule: Hold S1 (~{levels.s1:,.0f}) and reclaim Pivot (~{levels.pivot:,.0f})"
            f" → aim R1 (~{levels.r1:,.0f}); below S1 → watch S2 (~{levels.s2:,.0f})."
        )

    if report.key_levels:
        nifty_levels = report.key_levels.get("Nifty 50")
        bank_levels = report.key_levels.get("Nifty Bank")

        if nifty_levels:
            bullets.append(_nifty_rule(nifty_levels))
        if bank_levels:
            bullets.append(_banknifty_rule(bank_levels))

    breadth_threshold = 60
    if report.breadth and report.breadth.total:
        breadth_threshold = max(1, int(round(max(60, report.breadth.total * 0.6))))

    sector_focus = weakest_sector or "lagging sectors"
    bullets.append(
        f"Confirmation: Breadth (Adv ≥ {breadth_threshold}) and {sector_focus} turning green"
        " → follow-through; else caution stays."
    )

    bullets = bullets[:5]
    if not bullets:
        return ["What to Watch Next Session:", "• Key sectors and heavyweight stocks for follow-through."]

    return ["What to Watch Next Session:"] + [f"• {b}" for b in bullets]


def _round_level(index_name: str, value: float) -> float:
    base = 50
    if index_name in ("Nifty Bank", "Sensex"):
        base = 100
    return round(value / base) * base


def _compute_pivot_levels(name: str, history_df, market_closed: bool) -> Optional[KeyLevels]:
    try:
        clean = history_df.dropna(subset=["High", "Low", "Close"])
    except Exception as exc:  # noqa: BLE001
        logging.warning("Pivot calc failed for %s: %s", name, exc)
        return None

    if clean.empty:
        return None

    if market_closed or len(clean) == 1:
        latest_row = clean.iloc[-1]
    else:
        if len(clean) < 2:
            return None
        latest_row = clean.iloc[-2]

    high = float(latest_row["High"])
    low = float(latest_row["Low"])
    close = float(latest_row["Close"])

    pivot = (high + low + close) / 3
    r1 = 2 * pivot - low
    s1 = 2 * pivot - high
    r2 = pivot + (high - low)
    s2 = pivot - (high - low)

    return KeyLevels(
        name=name,
        method="Prev-day pivots",
        pivot=_round_level(name, pivot),
        r1=_round_level(name, r1),
        s1=_round_level(name, s1),
        r2=_round_level(name, r2),
        s2=_round_level(name, s2),
    )


def _build_key_levels(histories: Dict[str, object], market_closed: bool) -> Optional[Dict[str, KeyLevels]]:
    levels: Dict[str, KeyLevels] = {}

    for name, history in histories.items():
        try:
            pivots = _compute_pivot_levels(name, history, market_closed)
            if pivots:
                levels[name] = pivots
        except Exception as exc:  # noqa: BLE001
            logging.warning("Key level build failed for %s: %s", name, exc)

    return levels or None


def _build_drivers(report: "MarketReport", weakest_sector: Optional[str]) -> List[str]:
    drivers: List[str] = []

    if weakest_sector:
        drivers.append(f"Sector drag: {weakest_sector} was the biggest drag.")

    if report.bottom_performers:
        laggard = sorted(report.bottom_performers, key=lambda item: item.percent_change)[0]
        drivers.append(
            f"Stock drag: {laggard.symbol} led the downside ({laggard.percent_change:+.2f}%)."
        )

    if report.fii_dii and report.fii_dii.fii:
        fii_net = float(report.fii_dii.fii.net)
        flow_tag = "buying" if fii_net > 0 else "selling" if fii_net < 0 else "flat"
        drivers.append(f"Flows: FII net {flow_tag} (Net: {_format_number(fii_net)}).")

    if len(drivers) < 3 and report.vix:
        if report.vix.percent_change > 1.0:
            drivers.append("Macro: Rising VIX increased swings.")
        elif report.vix.percent_change < -1.0:
            drivers.append("Macro: VIX easing kept moves controlled.")
        else:
            drivers.append("Macro: VIX flat kept moves controlled.")

    return drivers[:3]


def _build_day_summary(
    report: "MarketReport",
    weakest_sector: Optional[str],
    strongest_sector: Optional[str],
) -> List[str]:
    lines: List[str] = []
    lines.append(_market_structure_line(report, weakest_sector, report.vix))

    flow_bits: List[str] = []
    if report.fii_dii and report.fii_dii.fii:
        fii_net = float(report.fii_dii.fii.net)
        flow_state = "buying" if fii_net > 0 else "selling" if fii_net < 0 else "flat"
        flow_bits.append(f"FII {flow_state} ({_format_number(fii_net)})")

    if report.vix:
        arrow = "↑" if report.vix.percent_change > 0 else "↓" if report.vix.percent_change < 0 else "→"
        flow_bits.append(f"VIX {arrow} {report.vix.percent_change:+.2f}% ({report.vix.value:.2f})")
    elif report.vix_warning:
        flow_bits.append("VIX unavailable")

    if flow_bits:
        lines.append("Flows/Vol: " + " | ".join(flow_bits))
    elif strongest_sector:
        lines.append(f"Flows/Vol: data thin; note {strongest_sector} leadership focus.")

    return lines[:2]


def format_report(report: MarketReport) -> str:
    opening_line: Optional[str]
    try:
        indices_pct = {idx.name: idx.percent_change for idx in report.indices}
        direction, strength, leader = classify_market(indices_pct, report.market_closed)
        nifty_pct = indices_pct.get("Nifty 50", 0.0)
        sensex_pct = indices_pct.get("Sensex", 0.0)
        banknifty_pct = indices_pct.get("Nifty Bank", 0.0)
        opening_line = get_opening_line(
            report.session_date,
            report.market_closed,
            nifty_pct,
            sensex_pct,
            banknifty_pct,
            leader,
            strength,
            direction,
        )
    except Exception as exc:  # noqa: BLE001
        logging.warning("Falling back to summary line: %s", exc)
        opening_line = _determine_summary(report.indices)

    weakest_sector = _weakest_sector(report.sector_moves)
    strongest_sector = _strongest_sector(report.sector_moves)
    if report.market_closed and opening_line.startswith("Market closed — showing last close"):
        opening_line = _determine_summary(report.indices)
    lines = [_report_header(report)]

    if report.warning:
        lines.append(report.warning)

    lines.extend([
        "",
        opening_line,
        "",
        "Day Summary:",
    ])

    day_summary = _build_day_summary(report, weakest_sector, strongest_sector)
    if day_summary:
        lines.extend([f"• {item}" for item in day_summary])
    else:
        lines.append("• Unavailable.")

    lines.extend([
        "",
        "Market Health (NIFTY 100 breadth):",
    ])
    if report.breadth:
        breadth_line = (
            f"Adv: {report.breadth.advances} | Dec: {report.breadth.declines} | "
            f"Unch: {report.breadth.unchanged}"
        )
        coverage_note = report.breadth.coverage_note or "full"
        breadth_line += f" | Coverage: {coverage_note}"
        lines.append(breadth_line)
        lines.append(_breadth_read_line(report.breadth))
    else:
        lines.append("Breadth unavailable.")

    lines.extend([
        "",
        "Key Levels (next session | prev-day pivots):",
    ])
    nifty_close = next((idx.close for idx in report.indices if idx.name == "Nifty 50"), None)

    if report.key_levels:
        printed_any = False
        for key in ["Nifty 50", "Nifty Bank", "Sensex"]:
            levels = report.key_levels.get(key)
            if levels:
                lines.append(
                    f"{levels.name}: S1 {levels.s1:,.0f} | Pivot {levels.pivot:,.0f} | "
                    f"R1 {levels.r1:,.0f} | S2 {levels.s2:,.0f} | R2 {levels.r2:,.0f}"
                )
                printed_any = True

        for name, levels in report.key_levels.items():
            if name in {"Nifty 50", "Nifty Bank", "Sensex"}:
                continue
            lines.append(
                f"{levels.name}: S1 {levels.s1:,.0f} | Pivot {levels.pivot:,.0f} | "
                f"R1 {levels.r1:,.0f} | S2 {levels.s2:,.0f} | R2 {levels.r2:,.0f}"
            )
            printed_any = True

        if not printed_any:
            lines.append("Key levels unavailable.")
    else:
        lines.append("Key levels unavailable.")

    pivot_line = "Close vs Pivot: Nifty 50 pivot unavailable."
    nifty_levels = report.key_levels.get("Nifty 50") if report.key_levels else None
    if nifty_levels and nifty_close is not None:
        position = "above" if nifty_close >= nifty_levels.pivot else "below"
        bias = "constructive bias" if position == "above" else "cautious bias until reclaimed"
        pivot_line = (
            f"Close vs Pivot: Nifty 50 closed {position} Pivot (~{nifty_levels.pivot:,.0f})"
            f" → {bias}."
        )

    lines.append(pivot_line)

    lines.extend([
        "",
        "Market Indices Snapshot:",
    ])

    for idx in report.indices:
        lines.append(
            f"{idx.name}: {_format_number(idx.close)} "
            f"({_format_change(idx.change)} | {_format_change(idx.percent_change)}%)"
        )
    lines.extend(["", *(_sector_trend_block(report.sector_moves, report.sector_warning))])
    lines.extend(["", _vix_line(report.vix, report.vix_warning)])

    drivers = report.drivers or _build_drivers(report, weakest_sector)
    lines.extend(["", "Why market moved today (Top 3 drivers):"])
    if drivers:
        lines.extend([f"• {driver}" for driver in drivers])
    else:
        lines.append("Drivers unavailable.")

    lines.extend(["", "Top movers (NIFTY 100 | 1D %):"])

    if report.movers_warning:
        lines.append(report.movers_warning)

    if report.top_gainers and report.bottom_performers:
        lines.append("Top 5 Gainers:")
        for mover in report.top_gainers:
            lines.append(
                f"• {mover.symbol}: {_format_number(mover.close)} "
                f"({_format_change(mover.percent_change)}%)"
            )

        lines.append("Bottom 5 Performers:")
        for mover in report.bottom_performers:
            lines.append(
                f"• {mover.symbol}: {_format_number(mover.close)} "
                f"({_format_change(mover.percent_change)}%)"
            )
    else:
        lines.append("Movers data unavailable.")

    if report.fii_dii or report.fii_dii_warning:
        lines.extend(["", "FII/DII (NSE):"])

        if report.fii_dii_warning:
            lines.append(report.fii_dii_warning)

        if report.fii_dii:
            as_on_text = f"As on: {report.fii_dii.as_on}"
            if (
                report.market_closed
                and report.fii_dii.as_on_date
                and report.fii_dii.as_on_date < report.session_date
            ):
                lines.append(
                    f"Market closed — showing last reported FII/DII data (As on: {report.fii_dii.as_on})"
                )
            else:
                lines.append(as_on_text)

            if report.fii_dii.fii:
                lines.append(
                    "FII "
                    f"Buy: {_format_number(report.fii_dii.fii.buy)} | "
                    f"Sell: {_format_number(report.fii_dii.fii.sell)} | "
                    f"Net: {_format_number(report.fii_dii.fii.net)}"
                )
            else:
                lines.append("FII data unavailable")

            if report.fii_dii.dii:
                lines.append(
                    "DII "
                    f"Buy: {_format_number(report.fii_dii.dii.buy)} | "
                    f"Sell: {_format_number(report.fii_dii.dii.sell)} | "
                    f"Net: {_format_number(report.fii_dii.dii.net)}"
                )
            else:
                lines.append("DII data unavailable")

    if report.liveblog_highlights is not None or report.liveblog_warning:
        lines.extend(["", "Market Highlights (Moneycontrol live):"])
        if report.liveblog_highlights:
            lines.extend([f"• {highlight}" for highlight in report.liveblog_highlights])
        elif report.liveblog_warning:
            lines.append(report.liveblog_warning)
        else:
            lines.append("Highlights unavailable today.")

    lines.extend(["", "News (Top 5):"])

    if report.news_warning:
        lines.append(report.news_warning)

    if report.news_lines:
        lines.extend([f"• {line}" for line in report.news_lines])
    elif not report.news_warning:
        lines.append("No news highlights available.")

    # Move "What to Watch" to the end of the report (after News).
    lines.extend(["", *(_tomorrows_focus(report, weakest_sector))])

    return "\n".join(lines)


def _latest_session_date(data) -> date:
    last_timestamp = data.index[-1]
    if hasattr(last_timestamp, "tzinfo") and last_timestamp.tzinfo is None:
        return last_timestamp.date()
    if hasattr(last_timestamp, "tz_convert"):
        session_dt = last_timestamp.tz_convert(IST)
    else:
        session_dt = last_timestamp.astimezone(IST)
    return session_dt.date()


def _ensure_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value
    return value.to_pydatetime()


def _last_timestamp_ist(ts: datetime) -> datetime:
    if ts.tzinfo is not None:
        return ts.astimezone(IST)
    return ts.replace(tzinfo=IST)


def _snapshot_from_history(name: str, history) -> IndexSnapshot:
    if history.empty:
        raise ValueError(f"No history returned for {name}")

    clean_history = history.dropna(subset=["Close"])
    if len(clean_history) < 2:
        raise ValueError(f"Insufficient data points for {name}")

    last_row = clean_history.iloc[-1]
    prev_row = clean_history.iloc[-2]

    close = float(last_row["Close"])
    previous_close = float(prev_row["Close"])
    change = close - previous_close
    percent_change = (change / previous_close * 100) if previous_close != 0 else 0.0

    return IndexSnapshot(
        name=name,
        close=close,
        previous_close=previous_close,
        change=change,
        percent_change=percent_change,
    )


def _fetch_history(ticker: str, period: str, interval: str):
    retries = 2
    delay = 0.5

    for attempt in range(retries + 1):
        start = time.monotonic()
        try:
            history = yf.Ticker(ticker).history(period=period, interval=interval)
            duration = time.monotonic() - start
            row_count = len(history)
            logging.info(
                "Fetched history for %s period=%s interval=%s rows=%s duration=%.3fs",
                ticker,
                period,
                interval,
                row_count,
                duration,
            )
            if not history.empty:
                return history
            logging.warning(
                "Empty history for %s period=%s interval=%s duration=%.3fs",
                ticker,
                period,
                interval,
                duration,
            )
        except Exception as exc:  # noqa: BLE001
            duration = time.monotonic() - start
            logging.warning(
                "History fetch failed for %s period=%s interval=%s duration=%.3fs error=%s",
                ticker,
                period,
                interval,
                duration,
                exc,
            )

        if attempt < retries:
            time.sleep(delay)
            delay *= 3

    raise ValueError(f"No history returned for {ticker} after retries")


def _build_stock_mover(ticker: str) -> Optional[StockMover]:
    try:
        history = _fetch_history(ticker, period="3d", interval="1d")
    except Exception as exc:  # noqa: BLE001
        logging.warning("Skipping mover for %s: %s", ticker, exc)
        return None

    clean_history = history.dropna(subset=["Close"])
    if len(clean_history) < 2:
        logging.warning("Skipping mover for %s due to insufficient data", ticker)
        return None

    prev_close = float(clean_history.iloc[-2]["Close"])
    close = float(clean_history.iloc[-1]["Close"])

    if prev_close == 0:
        logging.warning("Skipping mover for %s due to zero previous close", ticker)
        return None

    change = close - prev_close
    percent_change = change / prev_close * 100
    symbol = ticker.removesuffix(".NS")

    return StockMover(
        symbol=symbol,
        close=close,
        previous_close=prev_close,
        change=change,
        percent_change=percent_change,
    )


def _fetch_top_movers() -> Tuple[List[StockMover], List[StockMover], Optional[BreadthSnapshot], Optional[str]]:
    tickers, warning = _load_nifty_100_tickers()

    if not tickers:
        return [], [], None, warning

    movers: List[StockMover] = []
    for ticker in tickers:
        mover = _build_stock_mover(ticker)
        if mover:
            movers.append(mover)

    if not movers:
        fallback_warning = warning or "No movers data available; skipping movers."
        return [], [], None, fallback_warning

    sorted_movers = sorted(movers, key=lambda item: item.percent_change, reverse=True)
    top_gainers = sorted_movers[:5]
    bottom_performers = sorted(sorted_movers[-5:], key=lambda item: item.percent_change)
    eps = 0.0001
    advances = sum(1 for mover in movers if mover.percent_change > eps)
    declines = sum(1 for mover in movers if mover.percent_change < -eps)
    unchanged = sum(1 for mover in movers if -eps <= mover.percent_change <= eps)
    coverage_note = None
    if len(movers) != len(tickers):
        coverage_note = f"based on {len(movers)}/{len(tickers)} tickers fetched"

    breadth = BreadthSnapshot(
        total=len(movers),
        advances=advances,
        declines=declines,
        unchanged=unchanged,
        coverage_note=coverage_note,
    )

    return top_gainers, bottom_performers, breadth, warning


def _build_news_digest(now_ist: datetime, market_closed: bool) -> NewsDigest:
    try:
        lines = fetch_india_market_news_openai(now_ist)
        if not lines:
            return NewsDigest([], "News (Top 5): Unavailable (OpenAI web search error).")
        return NewsDigest(lines, None)
    except Exception as exc:  # noqa: BLE001
        logging.warning("OpenAI news fetch failed: %s", exc)
        return NewsDigest([], "News (Top 5): Unavailable (OpenAI web search error).")


def _build_fresh_market_report() -> MarketReport:
    start_time = time.monotonic()
    logging.info("Starting market report generation for %s tickers", len(INDEX_TICKERS))

    snapshots: List[IndexSnapshot] = []
    histories: Dict[str, object] = {}
    session_dates: List[date] = []
    last_ts_candidates: List[datetime] = []

    for name, ticker in INDEX_TICKERS.items():
        history = _fetch_history(ticker, FETCH_PERIOD, FETCH_INTERVAL)
        histories[name] = history
        snapshot = _snapshot_from_history(name, history)
        snapshots.append(snapshot)

        session_date = _latest_session_date(history)
        session_dates.append(session_date)

        raw_ts = _ensure_datetime(history.index[-1])
        last_ts_candidates.append(_last_timestamp_ist(raw_ts))

    report_date = max(session_dates) if session_dates else date.today()

    now_ist = datetime.now(tz=IST)
    latest_ts_display = max(last_ts_candidates) if last_ts_candidates else now_ist
    today_ist = now_ist.date()
    market_closed = latest_ts_display.date() < today_ist

    generated_at = datetime.now(timezone.utc)

    vix_snapshot, vix_warning = _fetch_vix_snapshot()
    sector_moves, sector_warning = _fetch_sector_moves()

    fii_dii_data, fii_dii_warning = get_fii_dii_data()
    top_gainers, bottom_performers, breadth, movers_warning = _fetch_top_movers()
    news_digest = _build_news_digest(now_ist, market_closed)
    liveblog_highlights, liveblog_warning = build_post_market_highlights(now_ist)

    key_levels = _build_key_levels(histories, market_closed)

    weakest_sector = _weakest_sector(sector_moves)
    strongest_sector = _strongest_sector(sector_moves)

    duration = time.monotonic() - start_time
    logging.info("Finished market report generation duration=%.3fs", duration)

    report = MarketReport(
        session_date=report_date,
        indices=snapshots,
        last_timestamp_ist=latest_ts_display,
        generated_at_utc=generated_at,
        market_closed=market_closed,
        vix=vix_snapshot,
        vix_warning=vix_warning,
        sector_moves=sector_moves,
        sector_warning=sector_warning,
        fii_dii=fii_dii_data,
        fii_dii_warning=fii_dii_warning,
        top_gainers=top_gainers,
        bottom_performers=bottom_performers,
        movers_warning=movers_warning,
        breadth=breadth,
        key_levels=key_levels,
        news_lines=news_digest.lines,
        news_warning=news_digest.warning,
        liveblog_highlights=liveblog_highlights,
        liveblog_warning=liveblog_warning,
    )

    drivers = _build_drivers(report, weakest_sector)
    if drivers:
        report = replace(report, drivers=drivers)

    return report


def _cache_report(report: MarketReport) -> None:
    _REPORT_CACHE["report"] = report
    _REPORT_CACHE["timestamp"] = datetime.now(timezone.utc)


def _get_cached_report() -> Optional[MarketReport]:
    cached_report = _REPORT_CACHE.get("report")
    cached_time = _REPORT_CACHE.get("timestamp")

    if not cached_report or not cached_time:
        return None

    if datetime.now(timezone.utc) - cached_time > timedelta(seconds=CACHE_TTL_SECONDS):
        return None

    return cached_report


def _acquire_polling_lock() -> Optional[str]:
    """Best-effort detection of concurrent pollers via a lock file."""

    global _POLLING_LOCK_HANDLE

    try:
        handle = open(_POLLING_LOCK_PATH, "a+")
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            handle.seek(0)
            handle.truncate()
            handle.write(str(os.getpid()))
            handle.flush()
            _POLLING_LOCK_HANDLE = handle
            return None
        except OSError:
            handle.seek(0)
            existing_pid = handle.read().strip() or "unknown"
            handle.close()
            return existing_pid
    except Exception as exc:  # noqa: BLE001
        logging.warning(
            "Unable to check for concurrent pollers (best-effort) error=%s", exc
        )
        return None


def fetch_market_report() -> MarketReport:
    try:
        report = _build_fresh_market_report()
        _cache_report(report)
        return report
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to fetch fresh market report", exc_info=exc)
        cached = _get_cached_report()
        if cached:
            return replace(
                cached,
                from_cache=True,
                warning="Using cached data due to upstream failure",
            )
        raise


def _run_self_tests() -> None:
    """Minimal sanity checks for timezone handling."""
    import pandas as pd

    naive_index = pd.date_range("2024-01-01", periods=1, freq="D")
    tz_aware_index = pd.date_range("2024-01-02 15:30", periods=1, freq="H", tz="UTC")

    naive_df = pd.DataFrame(index=naive_index, data={"Close": [10.0]})
    aware_df = pd.DataFrame(index=tz_aware_index, data={"Close": [11.0]})

    assert _latest_session_date(naive_df) == date(2024, 1, 1)
    assert _latest_session_date(aware_df) == tz_aware_index[-1].tz_convert(IST).date()

    print("Self-tests passed for _latest_session_date")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = (
        "Hi! Send /report to receive the latest Indian market close snapshot "
        "for Nifty 50, Sensex, and Nifty Bank."
    )
    await update.message.reply_text(help_text)


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    loading_message = await update.message.reply_text("Fetching the latest market report...")

    try:
        report = await asyncio.to_thread(fetch_market_report)
        message = format_report(report)
        await update.message.reply_text(message)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to build market report", exc_info=exc)
        await update.message.reply_text(
            "Sorry, I couldn't fetch the market data right now. Please try again shortly."
        )
    finally:
        try:
            await loading_message.delete()
        except Exception:  # noqa: BLE001
            pass


def main() -> None:
    global _POLLING_STARTED

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Avoid logging Telegram request URLs that include the bot token.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    initialize_templates_store()

    if _POLLING_STARTED:
        raise RuntimeError("Telegram polling already running in this process")

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN environment variable is required")

    other_pid = _acquire_polling_lock()
    pid = os.getpid()
    logging.info("Starting Telegram bot polling pid=%s", pid)
    if other_pid:
        logging.warning(
            "Detected possible concurrent Telegram poller pid=%s lock_path=%s",
            other_pid,
            _POLLING_LOCK_PATH,
        )

    _POLLING_STARTED = True

    application = Application.builder().token(token).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("report", report_command))
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "selftest":
        _run_self_tests()
    else:
        main()

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
SECTOR_TICKERS: Dict[str, str] = {
    "IT": "^CNXIT",
    "PSU Banks": "^CNXPSUBANK",
    "Private Banks": "^CNXPRIVAT",
    "Realty": "^CNXREALTY",
    "FMCG": "^CNXFMCG",
    "Energy": "^CNXENERGY",
    "Auto": "^CNXAUTO",
    "Pharma": "^CNXPHARMA",
    "Metal": "^CNXMETAL",
    "Infra": "^CNXINFRA",
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
    moves: List[SectorMove] = []
    failures = 0

    for sector, ticker in SECTOR_TICKERS.items():
        try:
            history = _fetch_history(ticker, period="6d", interval="1d")
            clean = history.dropna(subset=["Close"])
            if len(clean) < 2:
                failures += 1
                continue
            close = float(clean.iloc[-1]["Close"])
            prev_close = float(clean.iloc[-2]["Close"])
            moves.append(SectorMove(sector=sector, percent_change=_pct_change(close, prev_close)))
        except Exception as exc:  # noqa: BLE001
            failures += 1
            logging.warning("Sector fetch failed for %s (%s): %s", sector, ticker, exc)

    if not moves:
        return None, "Sector Trend: unavailable."

    moves.sort(key=lambda item: item.percent_change, reverse=True)
    warning = None
    if failures:
        warning = "Sector Trend: partial data (some sector indices unavailable)."
    return moves, warning


def _sector_trend_block(moves: Optional[List[SectorMove]], warning: Optional[str]) -> List[str]:
    if warning:
        return [warning]
    if not moves:
        return ["Sector Trend: unavailable."]

    strength = [m.sector for m in moves if m.percent_change >= 0.50]
    weakness = [m.sector for m in moves if m.percent_change <= -0.50]
    neutral = [m.sector for m in moves if -0.50 < m.percent_change < 0.50]

    def _fmt(items: List[str]) -> str:
        return ", ".join(items[:6]) if items else "None"

    return [
        "Sector Trend:",
        f"• Strength: {_fmt(strength)}",
        f"• Weakness: {_fmt(weakness)}",
        f"• Neutral: {_fmt(neutral)}",
    ]


def _weakest_sector(moves: Optional[List[SectorMove]]) -> Optional[str]:
    if not moves:
        return None
    return sorted(moves, key=lambda item: item.percent_change)[0].sector


def _vix_line(vix: Optional[VixSnapshot], warning: Optional[str]) -> str:
    if warning:
        return warning
    if not vix:
        return "Volatility (INDIA VIX): unavailable."
    arrow = "↑" if vix.percent_change > 0 else "↓" if vix.percent_change < 0 else "→"
    return f"Volatility (INDIA VIX): {vix.value:.2f} ({arrow} {vix.percent_change:+.2f}%)."


def _market_structure_line(
    report: "MarketReport",
    weakest_sector: Optional[str],
    vix: Optional[VixSnapshot],
) -> str:
    indices_pct = {idx.name: idx.percent_change for idx in report.indices}
    nifty_pct = indices_pct.get("Nifty 50", 0.0)

    # Breadth proxy (cheap but stable): movers vs losers, if available.
    breadth_bias = "mixed"
    if report.top_gainers and report.bottom_performers:
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


def _tomorrows_focus(report: "MarketReport", weakest_sector: Optional[str], vix: Optional[VixSnapshot]) -> List[str]:
    bullets: List[str] = []

    if weakest_sector:
        bullets.append(f"{weakest_sector} follow-through or stabilization (sentiment driver).")

    if report.bottom_performers:
        biggest_loser = report.bottom_performers[0].symbol
        bullets.append(f"{biggest_loser} follow-through (heavyweight pressure check).")

    if report.fii_dii and report.fii_dii.fii:
        fii_net = float(report.fii_dii.fii.net)
        if fii_net < 0:
            bullets.append("FII flow: watch if selling persists on dips / near support.")
        elif fii_net > 0:
            bullets.append("FII flow: watch if buying continues into strength.")
        else:
            bullets.append("FII flow: watch next update for direction.")

    if vix and vix.percent_change >= 2.0 and len(bullets) < 3:
        bullets.append("Volatility: rising VIX can amplify intraday swings.")

    bullets = bullets[:3]
    if not bullets:
        return ["What to Watch Next Session:", "• Key sectors and heavyweight stocks for follow-through."]

    return ["What to Watch Next Session:"] + [f"• {b}" for b in bullets]


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

    generated_ist = report.generated_at_utc.astimezone(IST)
    lines = [
        f"Report generated (IST): {generated_ist.strftime('%A, %Y-%m-%d %H:%M:%S')}",
    ]

    if report.market_closed:
        lines.append("Market closed — showing last close")

    if report.warning:
        lines.append(report.warning)

    lines.extend([
        "",
        opening_line,
        "",
        "Market Indices Snapshot:",
    ])

    for idx in report.indices:
        lines.append(
            f"{idx.name}: {_format_number(idx.close)} "
            f"({_format_change(idx.change)} | {_format_change(idx.percent_change)}%)"
        )

    weakest_sector = _weakest_sector(report.sector_moves)
    lines.extend(["", _market_structure_line(report, weakest_sector, report.vix)])
    lines.extend(["", *(_sector_trend_block(report.sector_moves, report.sector_warning))])
    lines.extend(["", _vix_line(report.vix, report.vix_warning)])

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

    lines.extend(["", *(_tomorrows_focus(report, weakest_sector, report.vix))])

    lines.extend(["", "News (Top 5):"])

    if report.news_warning:
        lines.append(report.news_warning)

    if report.news_lines:
        lines.extend([f"• {line}" for line in report.news_lines])
    elif not report.news_warning:
        lines.append("No news highlights available.")

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


def _fetch_top_movers() -> Tuple[List[StockMover], List[StockMover], Optional[str]]:
    tickers, warning = _load_nifty_100_tickers()

    if not tickers:
        return [], [], warning

    movers: List[StockMover] = []
    for ticker in tickers:
        mover = _build_stock_mover(ticker)
        if mover:
            movers.append(mover)

    if not movers:
        fallback_warning = warning or "No movers data available; skipping movers."
        return [], [], fallback_warning

    sorted_movers = sorted(movers, key=lambda item: item.percent_change, reverse=True)
    top_gainers = sorted_movers[:5]
    bottom_performers = sorted(sorted_movers[-5:], key=lambda item: item.percent_change)

    return top_gainers, bottom_performers, warning


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
    session_dates: List[date] = []
    last_ts_candidates: List[datetime] = []

    for name, ticker in INDEX_TICKERS.items():
        history = _fetch_history(ticker, FETCH_PERIOD, FETCH_INTERVAL)
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
    top_gainers, bottom_performers, movers_warning = _fetch_top_movers()
    news_digest = _build_news_digest(now_ist, market_closed)
    liveblog_highlights, liveblog_warning = build_post_market_highlights(now_ist)

    duration = time.monotonic() - start_time
    logging.info("Finished market report generation duration=%.3fs", duration)

    return MarketReport(
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
        news_lines=news_digest.lines,
        news_warning=news_digest.warning,
        liveblog_highlights=liveblog_highlights,
        liveblog_warning=liveblog_warning,
    )


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

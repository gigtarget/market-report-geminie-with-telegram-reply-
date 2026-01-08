from __future__ import annotations

import csv
import logging
import math
import os
import tempfile
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from indicators import compute_macd, compute_rsi, compute_supertrend
from market_data import ensure_datetime, fetch_history, last_timestamp_ist, latest_session_date
from nse_fiidii import FiiDiiData, get_fii_dii_data
from openai_news import fetch_india_market_news_openai
from post_market_highlights import build_post_market_highlights

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
class IndicatorSnapshot:
    rsi: float
    rsi_label: str
    macd: float
    macd_signal: float
    macd_hist: float
    macd_label: str
    supertrend: float
    supertrend_direction: str


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
    indicators: Optional[Dict[str, IndicatorSnapshot]] = None
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


def _normalize_sector_key(value: str) -> str:
    return " ".join(value.split()).strip().lower()


def _pct_change(close: float, prev_close: float) -> float:
    if prev_close == 0:
        return 0.0
    return (close - prev_close) / prev_close * 100


def _fetch_vix_snapshot() -> Tuple[Optional[VixSnapshot], Optional[str]]:
    try:
        history = fetch_history(VIX_TICKER, period="6d", interval="1d")
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
                history = fetch_history(ticker, period="6d", interval="1d")
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
        coverage_line += f" | Sector index unavailable: {', '.join(missing)}"

    if not moves:
        return None, coverage_line

    moves.sort(key=lambda item: item.percent_change, reverse=True)
    return moves, coverage_line


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


def _build_stock_mover(ticker: str) -> Optional[StockMover]:
    try:
        history = fetch_history(ticker, period="3d", interval="1d")
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


def _build_news_digest(now_ist: datetime) -> NewsDigest:
    try:
        lines = fetch_india_market_news_openai(now_ist)
        if not lines:
            return NewsDigest([], "News (Top 5): Unavailable (OpenAI web search error).")
        return NewsDigest(lines, None)
    except Exception as exc:  # noqa: BLE001
        logging.warning("OpenAI news fetch failed: %s", exc)
        return NewsDigest([], "News (Top 5): Unavailable (OpenAI web search error).")


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


def _build_indicators(histories: Dict[str, object]) -> Optional[Dict[str, IndicatorSnapshot]]:
    indicators: Dict[str, IndicatorSnapshot] = {}

    for name in ("Nifty 50", "Nifty Bank"):
        history = histories.get(name)
        if history is None:
            continue
        clean = history.dropna(subset=["High", "Low", "Close"])
        if clean.empty:
            continue
        try:
            rsi = compute_rsi(clean["Close"])
            macd = compute_macd(clean["Close"])
            supertrend = compute_supertrend(clean["High"], clean["Low"], clean["Close"])
        except Exception as exc:  # noqa: BLE001
            logging.warning("Indicator calc failed for %s: %s", name, exc)
            continue

        indicators[name] = IndicatorSnapshot(
            rsi=rsi.value,
            rsi_label=rsi.label,
            macd=macd.macd,
            macd_signal=macd.signal,
            macd_hist=macd.histogram,
            macd_label=macd.label,
            supertrend=supertrend.value,
            supertrend_direction=supertrend.label,
        )

    return indicators or None


def _build_drivers(report: MarketReport, weakest_sector: Optional[str]) -> List[str]:
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


def _build_fresh_market_report() -> MarketReport:
    start_time = time.monotonic()
    logging.info("Starting market report generation for %s tickers", len(INDEX_TICKERS))

    snapshots: List[IndexSnapshot] = []
    histories: Dict[str, object] = {}
    session_dates: List[date] = []
    last_ts_candidates: List[datetime] = []

    for name, ticker in INDEX_TICKERS.items():
        history = fetch_history(ticker, FETCH_PERIOD, FETCH_INTERVAL)
        histories[name] = history
        snapshot = _snapshot_from_history(name, history)
        snapshots.append(snapshot)

        session_date = latest_session_date(history)
        session_dates.append(session_date)

        raw_ts = ensure_datetime(history.index[-1])
        last_ts_candidates.append(last_timestamp_ist(raw_ts))

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
    news_digest = _build_news_digest(now_ist)
    liveblog_highlights, liveblog_warning = build_post_market_highlights(now_ist)

    key_levels = _build_key_levels(histories, market_closed)
    indicators = _build_indicators(histories)

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
        indicators=indicators,
        news_lines=news_digest.lines,
        news_warning=news_digest.warning,
        liveblog_highlights=liveblog_highlights,
        liveblog_warning=liveblog_warning,
    )

    weakest_sector = _weakest_sector(sector_moves)
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


def _weakest_sector(moves: Optional[List[SectorMove]]) -> Optional[str]:
    if not moves:
        return None
    return sorted(moves, key=lambda item: item.percent_change)[0].sector

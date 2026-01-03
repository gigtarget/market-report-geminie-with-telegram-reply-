import asyncio
import logging
import os
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import yfinance as yf
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

IST = ZoneInfo("Asia/Kolkata")
FETCH_PERIOD = "10d"
FETCH_INTERVAL = "1d"
CACHE_TTL_SECONDS = 120

INDEX_TICKERS: Dict[str, str] = {
    "Nifty 50": "^NSEI",
    "Sensex": "^BSESN",
    "Nifty Bank": "^NSEBANK",
}


@dataclass
class IndexSnapshot:
    name: str
    close: float
    previous_close: float
    change: float
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


_REPORT_CACHE: Dict[str, Optional[object]] = {"report": None, "timestamp": None}


def _format_number(value: float) -> str:
    return f"{value:,.2f}"


def _format_change(value: float) -> str:
    return f"{value:+,.2f}"


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


def format_report(report: MarketReport) -> str:
    summary = _determine_summary(report.indices)

    lines = [
        f"Generated at (UTC): {report.generated_at_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"Market session date (IST): {report.session_date.strftime('%A, %Y-%m-%d')}",
        f"Data last timestamp (IST): {report.last_timestamp_ist.strftime('%Y-%m-%d %H:%M:%S %Z')}",
    ]

    if report.market_closed:
        lines.append("Market closed — showing last close")

    if report.warning:
        lines.append(report.warning)

    lines.extend([
        "",
        summary,
        "",
        "Market Indices Snapshot:",
    ])

    for idx in report.indices:
        lines.append(
            f"{idx.name}: {_format_number(idx.close)} "
            f"({_format_change(idx.change)} | {_format_change(idx.percent_change)}%)"
        )

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

    latest_ts_display = max(last_ts_candidates) if last_ts_candidates else datetime.now(tz=IST)
    today_ist = datetime.now(tz=IST).date()
    market_closed = latest_ts_display.date() < today_ist

    generated_at = datetime.now(timezone.utc)

    duration = time.monotonic() - start_time
    logging.info("Finished market report generation duration=%.3fs", duration)

    return MarketReport(
        session_date=report_date,
        indices=snapshots,
        last_timestamp_ist=latest_ts_display,
        generated_at_utc=generated_at,
        market_closed=market_closed,
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN environment variable is required")

    application = Application.builder().token(token).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("report", report_command))

    logging.info("Starting Telegram bot polling")
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "selftest":
        _run_self_tests()
    else:
        main()

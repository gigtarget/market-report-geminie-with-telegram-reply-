import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Dict, List
from zoneinfo import ZoneInfo

import yfinance as yf
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

IST = ZoneInfo("Asia/Kolkata")

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
        f"As of market close (IST): {report.session_date.strftime('%A, %Y-%m-%d')}",
        "",
        summary,
        "",
        "Market Indices Snapshot:",
    ]

    for idx in report.indices:
        lines.append(
            f"{idx.name}: {_format_number(idx.close)} "
            f"({_format_change(idx.change)} | {_format_change(idx.percent_change)}%)"
        )

    return "\n".join(lines)


def _latest_session_date(data) -> date:
    last_timestamp = data.index[-1]
    if last_timestamp.tzinfo is None:
        last_timestamp = last_timestamp.tz_localize("UTC")
    session_dt = last_timestamp.tz_convert(IST)
    return session_dt.date()


def _fetch_index_snapshot(name: str, ticker: str) -> IndexSnapshot:
    history = yf.Ticker(ticker).history(period="10d", interval="1d", tz="Asia/Kolkata")

    if history.empty:
        raise ValueError(f"No history returned for {ticker}")

    clean_history = history.dropna(subset=["Close"])
    if len(clean_history) < 2:
        raise ValueError(f"Insufficient data points for {ticker}")

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


def fetch_market_report() -> MarketReport:
    snapshots: List[IndexSnapshot] = []
    session_dates: List[date] = []

    for name, ticker in INDEX_TICKERS.items():
        snapshot = _fetch_index_snapshot(name, ticker)
        snapshots.append(snapshot)

        history = yf.Ticker(ticker).history(period="2d", interval="1d", tz="Asia/Kolkata")
        if history.empty:
            raise ValueError(f"Unable to determine session date for {ticker}")
        session_dates.append(_latest_session_date(history))

    report_date = max(session_dates) if session_dates else date.today()
    return MarketReport(session_date=report_date, indices=snapshots)


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
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
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
    main()

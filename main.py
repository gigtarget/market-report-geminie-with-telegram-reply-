import asyncio
import fcntl
import io
import logging
import os
import tempfile
from datetime import date, datetime, time, timezone
from typing import Optional

from telegram import InputFile, Update
from telegram.ext import Application, CommandHandler, ContextTypes, Defaults

from market_data import latest_session_date
from report_builder import fetch_market_report
from report_format import format_report
from templates import initialize_templates_store

import pytz

IST = pytz.timezone("Asia/Kolkata")

_POLLING_STARTED = False
_POLLING_LOCK_HANDLE = None
_POLLING_LOCK_PATH = os.path.join(tempfile.gettempdir(), "telegram_bot_poller.lock")
TELEGRAM_TEXT_LIMIT = 3500


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


def _run_self_tests() -> None:
    """Minimal sanity checks for timezone handling."""
    import pandas as pd

    naive_index = pd.date_range("2024-01-01", periods=1, freq="D")
    tz_aware_index = pd.date_range("2024-01-02 15:30", periods=1, freq="H", tz="UTC")

    naive_df = pd.DataFrame(index=naive_index, data={"Close": [10.0]})
    aware_df = pd.DataFrame(index=tz_aware_index, data={"Close": [11.0]})

    assert latest_session_date(naive_df) == date(2024, 1, 1)
    assert latest_session_date(aware_df) == tz_aware_index[-1].tz_convert(IST).date()

    print("Self-tests passed for latest_session_date")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = (
        "Hi! Send /report to receive the latest Indian market close snapshot "
        "for Nifty 50, Sensex, and Nifty Bank."
    )
    await update.message.reply_text(help_text)


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    loading_message = await update.message.reply_text("Fetching the latest market report...")

    try:
        await _send_report(
            send_text=update.message.reply_text,
            send_document=lambda buffer, filename: update.message.reply_document(
                InputFile(buffer, filename=filename),
                caption="Full market report (auto-attached due to length)",
            ),
        )
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


async def chatid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    await update.message.reply_text(f"chat_id: {chat.id}")


async def _send_report(send_text, send_document) -> None:
    report = await asyncio.to_thread(fetch_market_report)
    message = format_report(report)
    if len(message) <= TELEGRAM_TEXT_LIMIT:
        await send_text(message)
        return

    filename = f"market_report_{report.session_date.strftime('%Y%m%d')}.txt"
    buffer = io.BytesIO(message.encode("utf-8"))
    buffer.seek(0)
    try:
        await send_document(buffer, filename)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to send report document", exc_info=exc)


async def scheduled_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.info(
        "scheduled_report fired | UTC=%s | IST=%s",
        datetime.now(timezone.utc).isoformat(),
        datetime.now(IST).isoformat(),
    )
    chat_id = context.job.data.get("chat_id") if context.job and context.job.data else None
    if not chat_id:
        logging.warning("Scheduled report skipped because chat_id is missing")
        return

    try:
        await _send_report(
            send_text=lambda text: context.bot.send_message(chat_id=chat_id, text=text),
            send_document=lambda buffer, filename: context.bot.send_document(
                chat_id=chat_id,
                document=InputFile(buffer, filename=filename),
                caption="Full market report (auto-attached due to length)",
            ),
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to send scheduled market report", exc_info=exc)


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
    logging.info(
        "Bot started | UTC=%s | IST=%s",
        datetime.now(timezone.utc).isoformat(),
        datetime.now(IST).isoformat(),
    )
    if other_pid:
        logging.warning(
            "Detected possible concurrent Telegram poller pid=%s lock_path=%s",
            other_pid,
            _POLLING_LOCK_PATH,
        )

    _POLLING_STARTED = True

    defaults = Defaults(tzinfo=IST)
    application = Application.builder().token(token).defaults(defaults).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("report", report_command))
    application.add_handler(CommandHandler("chatid", chatid_command))

    logging.info(
        "Scheduler timezone set | IST=%s | UTC=%s",
        datetime.now(IST).isoformat(),
        datetime.now(timezone.utc).isoformat(),
    )

    raw_report_chat_id = os.getenv("TELEGRAM_REPORT_CHAT_ID")
    try:
        report_chat_id = int(raw_report_chat_id) if raw_report_chat_id else None
    except ValueError:
        report_chat_id = None

    if report_chat_id is None:
        if raw_report_chat_id:
            logging.error(
                "TELEGRAM_REPORT_CHAT_ID must be a numeric chat_id; daily schedule disabled"
            )
        else:
            logging.error(
                "TELEGRAM_REPORT_CHAT_ID not set; daily market report will not be scheduled. "
                "Send /chatid in the target chat to get the id and set it in Railway Variables."
            )
    else:
        job = application.job_queue.run_daily(
            scheduled_report,
            time=time(18, 30),
            data={"chat_id": report_chat_id},
            name="daily_market_report",
        )
        logging.info("Daily market report scheduled for 18:10 IST to chat_id=%s", report_chat_id)
        if job and getattr(job, "next_t", None):
            next_run = job.next_t.astimezone(IST)
            logging.info(
                "Next scheduled run at %s IST", next_run.strftime("%Y-%m-%d %H:%M:%S")
            )
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "selftest":
        _run_self_tests()
    else:
        main()

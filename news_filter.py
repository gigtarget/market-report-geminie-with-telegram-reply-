import csv
import logging
import re
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from news_fetch import NewsItem

IST = ZoneInfo("Asia/Kolkata")

INDIA_KEYWORDS = [
    "nifty",
    "sensex",
    "bank nifty",
    "nse",
    "bse",
    "fii",
    "dii",
    "rbi",
    "sebi",
    "gift nifty",
    "india vix",
    "vix",
]

GLOBAL_LINKED_KEYWORDS = [
    "india",
    "indian",
    "rupee",
    "rbi",
    "indian equities",
    "india markets",
]


def _load_symbol_set() -> Tuple[Set[str], Optional[str]]:
    csv_path = Path(__file__).with_name("ind_nifty100list.csv")
    symbols: Set[str] = set()
    warning: Optional[str] = None

    try:
        with csv_path.open(newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            if not reader.fieldnames:
                warning = "NIFTY 100 list empty for news filter."
                return symbols, warning

            symbol_key = None
            for field in reader.fieldnames:
                if field and field.strip().lower() == "symbol":
                    symbol_key = field
                    break

            if not symbol_key:
                warning = "NIFTY 100 list missing 'Symbol' column for news filter."
                return symbols, warning

            for row in reader:
                raw_symbol = row.get(symbol_key, "")
                symbol = raw_symbol.strip().upper()
                if symbol:
                    symbols.add(symbol)
    except FileNotFoundError:
        warning = "NIFTY 100 list not found for news filter."
    except Exception as exc:  # noqa: BLE001
        warning = f"Failed reading NIFTY 100 list for news filter: {exc}"
        logging.warning(warning)

    return symbols, warning


def filter_by_time(items: List[NewsItem], now_ist: datetime) -> List[NewsItem]:
    if now_ist.timetz() < time(9, 15, tzinfo=IST):
        start = datetime.combine(now_ist.date() - timedelta(days=1), time(15, 30), tzinfo=IST)
    else:
        start = datetime.combine(now_ist.date(), time(9, 15), tzinfo=IST)

    filtered: List[NewsItem] = []
    for item in items:
        if not item.published_at:
            logging.info("Dropping item without published_at title=%s", item.title)
            continue
        try:
            published_ist = item.published_at.astimezone(IST)
        except Exception:  # noqa: BLE001
            continue

        if published_ist >= start:
            filtered.append(item)
        else:
            logging.info("Dropping stale item title=%s published_at=%s", item.title, published_ist)

    logging.info("Time filter kept %s of %s items", len(filtered), len(items))
    return filtered


def _contains_keyword(text: str, keywords: List[str]) -> bool:
    lower_text = text.lower()
    return any(keyword in lower_text for keyword in keywords)


def _contains_symbol(text: str, symbols: Set[str]) -> bool:
    for symbol in symbols:
        pattern = rf"\b{re.escape(symbol)}\b"
        if re.search(pattern, text, flags=re.IGNORECASE):
            return True
    return False


def relevance_filter(items: List[NewsItem], now_ist: datetime) -> Tuple[List[NewsItem], List[NewsItem], Optional[str]]:
    symbols, warning = _load_symbol_set()
    india_items: List[NewsItem] = []
    global_addons: List[NewsItem] = []

    for item in items:
        text_blob = " ".join(filter(None, [item.title, item.summary or ""]))
        if _contains_keyword(text_blob, INDIA_KEYWORDS) or _contains_symbol(text_blob, symbols):
            india_items.append(item)
            continue

        if _contains_keyword(text_blob, GLOBAL_LINKED_KEYWORDS):
            global_addons.append(item)

    logging.info(
        "Relevance filter india_items=%s global_candidates=%s", len(india_items), len(global_addons)
    )
    return india_items, global_addons, warning


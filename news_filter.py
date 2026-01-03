import logging
from datetime import datetime, time, timedelta
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

from news_fetch import NewsItem

IST = ZoneInfo("Asia/Kolkata")


def filter_by_session_time(items: List[NewsItem], now_ist: datetime, market_closed: bool) -> List[NewsItem]:
    if now_ist.timetz() < time(9, 15, tzinfo=IST):
        start = datetime.combine(now_ist.date() - timedelta(days=1), time(15, 30), tzinfo=IST)
    elif now_ist.timetz() < time(15, 30, tzinfo=IST) and not market_closed:
        start = datetime.combine(now_ist.date(), time(9, 15), tzinfo=IST)
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

    logging.info("Session time filter kept %s of %s items", len(filtered), len(items))
    return filtered


def relevance_filter(items: List[NewsItem], now_ist: datetime) -> Tuple[List[NewsItem], List[NewsItem], Optional[str]]:
    india_items: List[NewsItem] = []
    global_addons: List[NewsItem] = []

    for item in items:
        if item.category == "global":
            global_addons.append(item)
        else:
            india_items.append(item)

    logging.info(
        "Relevance filter (category only) india_items=%s global_candidates=%s", len(india_items), len(global_addons)
    )
    return india_items, global_addons, None


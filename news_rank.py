import logging
from datetime import datetime
from typing import List, Sequence, Tuple

from news_fetch import NewsItem


TIER1_DOMAINS = [
    "economictimes.indiatimes.com",
    "www.moneycontrol.com",
    "www.business-standard.com",
    "www.livemint.com",
    "www.reuters.com",
]

IMPACT_KEYWORDS = [
    "policy",
    "rates",
    "inflation",
    "cpi",
    "gdp",
    "earnings",
    "guidance",
    "order",
    "contract",
    "merger",
    "acquisition",
    "sebi",
    "probe",
    "ban",
    "rupee",
    "crude",
]


def _score_item(item: NewsItem, now_ist: datetime) -> float:
    if not item.published_at:
        return -1.0

    hours_old = (now_ist - item.published_at.astimezone(now_ist.tzinfo)).total_seconds() / 3600
    freshness = max(0.0, 120.0 - hours_old * 10)
    text = f"{item.title} {item.summary or ''}".lower()
    impact = sum(5 for keyword in IMPACT_KEYWORDS if keyword in text)
    source_boost = 10 if item.source_domain.lower() in TIER1_DOMAINS else 0

    return freshness + impact + source_boost


def rank_and_select(india_items: Sequence[Tuple[NewsItem, str]], global_items: Sequence[Tuple[NewsItem, str]], now_ist: datetime):
    scored_india = [
        (item, story_id, _score_item(item, now_ist))
        for item, story_id in india_items
        if item.published_at
    ]
    scored_global = [
        (item, story_id, _score_item(item, now_ist))
        for item, story_id in global_items
        if item.published_at
    ]

    top_india = sorted(scored_india, key=lambda x: x[2], reverse=True)[:5]
    top_global = sorted(scored_global, key=lambda x: x[2], reverse=True)[:2]

    logging.info("Selected india=%s global=%s", len(top_india), len(top_global))
    return top_india, top_global


import calendar
import hashlib
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

import feedparser

from news_dedupe import normalize_title
from news_fetch import NewsItem


FEED_URL = "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"

FORBIDDEN_PHRASES = [
    "stocks in news",
    "tech view",
    "stock radar",
    "trade setup",
    "gainers and losers",
    "buzzing stocks",
    "things to know before",
    "what changed for",
    "top gainers",
    "top losers",
    "block deal",
    "bulk deal",
    "mutual fund",
    "rakesh jhunjhunwala",
    "warren buffett",
    "ipo",
    "f&o ban",
    "price target",
    "why is",
    "should you buy",
    "do you own",
    "demand for gold",
    "usd/inr",
    "rupee falls",
    "forex reserve",
    "cryptocurrency",
    "bitcoin",
    "ethereum",
    "silver rate",
    "gold price",
    "mcx",
    "ipos to open",
    "ipo opens",
    "ipo closes",
    "ipo subscription",
    "ipo subscribed",
    "ipo subscribed",
    "grey market premium",
    "anchor book",
    "listing gain",
    "11 things",
    "10 things",
    "5 things",
    "things to watch",
    "hot stocks",
    "buy or sell",
    "stocks to buy",
    "how to invest",
    "in charts",
    "chart check",
    "daily voice",
    "podcast",
    "video",
    "audio",
    "photo",
    "infographic",
    "in pics",
    "in charts",
    "highlights",
    "live updates",
    "blog",
    "portfolio",
    "adani in focus",
    "tcs share price",
    "infy share price",
    "hdfc bank share price",
    "nifty 50",
    "sensex",
    "bank nifty",
    "pre-open",
    "preopen",
    "sgx nifty",
    "gift nifty",
    "commodities corner",
    "usd inr",
    "intraday",
    "futures and options",
    "f&o",
    "result preview",
    "result review",
    "results preview",
    "results review",
    "q1 preview",
    "q2 preview",
    "q3 preview",
    "q4 preview",
    "q1 results",
    "q2 results",
    "q3 results",
    "q4 results",
    "rights issue",
    "share split",
    "bonus issue",
    "sharekhan",
    "motilal oswal",
    "axis securities",
    "kotak securities",
    "geojit",
    "centrum",
    "hdfc securities",
    "yes securities",
    "angel one",
    "icici securities",
    "morgan stanley",
    "jefferies",
    "macquarie",
    "goldman sachs",
    "citibank",
    "citi research",
    "ubs",
    "hsbc",
    "clsa",
    "bernstein",
    "nomura",
    "jpmorgan",
    "dsp",
    "edelweiss",
    "best stock ideas",
    "top ideas",
    "stock talk",
    "fno",
    "option chain",
    "oi spurt",
    "option data",
]


def _story_id_from_title(title: str) -> str:
    normalized = normalize_title(title)
    return hashlib.sha1(normalized.encode("utf-8", errors="ignore")).hexdigest()


def _parse_published(entry) -> Optional[datetime]:
    published_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if not published_parsed:
        return None

    try:
        ts = calendar.timegm(published_parsed)
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:  # noqa: BLE001
        return None


def _is_forbidden(title: str) -> bool:
    lowered = title.lower()
    return any(phrase in lowered for phrase in FORBIDDEN_PHRASES)


def get_et_market_articles(limit: int = 30) -> List[NewsItem]:
    feed = feedparser.parse(FEED_URL)
    items: List[NewsItem] = []

    for entry in feed.entries:
        if len(items) >= limit:
            break

        title = (entry.get("title") or "").strip()
        if not title:
            continue

        if title.endswith("?"):
            continue

        if len(title.split()) < 5:
            continue

        if _is_forbidden(title):
            continue

        summary_raw = entry.get("summary") or entry.get("description")
        summary = summary_raw.strip() if summary_raw else None

        item = NewsItem(
            title=title,
            link=(entry.get("link") or "").strip(),
            source_domain="economictimes.indiatimes.com",
            published_at=_parse_published(entry),
            category="india",
            summary=summary,
        )

        items.append(item)

    logging.info("Fetched %s ET Markets articles", len(items))
    return items


@dataclass
class RankedNews:
    story_id: str
    item: NewsItem


def get_relevant_market_news(
    items: List[NewsItem],
    sent_store,
    top_n: int = 30,
    relevant_n: int = 5,
) -> tuple[list[str], list[str]]:
    truncated = items[:top_n]
    fetched_count = len(truncated)

    unique: list[RankedNews] = []
    seen_ids = set()
    for item in truncated:
        story_id = _story_id_from_title(item.title)
        if story_id in seen_ids:
            continue
        seen_ids.add(story_id)
        unique.append(RankedNews(story_id=story_id, item=item))

    filtered = [rn for rn in unique if not sent_store.is_sent(rn.story_id)]
    deduped_count = len(filtered)

    logging.info(
        "News pipeline counts fetched=%s filtered_unique=%s deduped=%s",
        fetched_count,
        len(unique),
        deduped_count,
    )

    selected_ids = [rn.story_id for rn in filtered[:relevant_n]]
    selected_items = [rn.item for rn in filtered[:relevant_n]]

    bullets = _summarize_with_gemini(selected_items, relevant_n)
    if not bullets:
        bullets = _fallback_bullets(selected_items)

    logging.info(
        "News pipeline selected_count=%s output_lines=%s",
        len(selected_items),
        len(bullets),
    )

    return bullets, selected_ids


def _summarize_with_gemini(items: List[NewsItem], relevant_n: int) -> List[str]:
    if not items:
        return []

    try:
        import google.generativeai as genai

        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            return []

        model_name = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name)

        prompt_lines = [
            "Select the 5 most important items for Indian stock market traders and rewrite as crisp one-liners explaining what happened and why it matters.",
            "Rules: return one bullet per line, avoid URLs, keep it concise and factual.",
            "Items:",
        ]

        for idx, item in enumerate(items, start=1):
            body = item.summary or ""
            prompt_lines.append(f"{idx}. {item.title} — {body}")

        response = model.generate_content("\n".join(prompt_lines))
        text = response.text if hasattr(response, "text") else ""

        bullets: List[str] = []
        for line in text.splitlines():
            stripped = line.strip("•-* \t")
            if not stripped:
                continue
            bullets.append(_strip_urls(stripped))

        return bullets[:relevant_n]
    except Exception as exc:  # noqa: BLE001
        logging.warning("Gemini summarization failed: %s", exc)
        return []


def _fallback_bullets(items: List[NewsItem]) -> List[str]:
    bullets: List[str] = []
    for item in items:
        summary = item.summary or ""
        if summary:
            bullets.append(_strip_urls(f"{item.title} — {summary}"))
        else:
            bullets.append(_strip_urls(item.title))
    return bullets


def _strip_urls(text: str) -> str:
    return re.sub(r"https?://\S+", "", text).strip()


__all__ = [
    "get_et_market_articles",
    "get_relevant_market_news",
]

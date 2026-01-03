"""Post-market highlights built from Moneycontrol liveblog items."""

from __future__ import annotations

import logging
import os
from datetime import datetime, time
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

from moneycontrol_liveblog import NewsItem, fetch_moneycontrol_liveblog

IST = ZoneInfo("Asia/Kolkata")

DROP_PHRASES = [
    "volume shockers",
    "positive break-out stocks",
    "most active stocks",
]

ACTION_KEYWORDS = [
    "earnings",
    "acquisition",
    "merger",
    "buyback",
    "sebi",
    "bse",
    "nse",
    "surges",
    "soars",
    "plunges",
    "slumps",
    "jumps",
    "slides",
    "order",
    "contract",
    "guidance",
]


def _should_drop(item: NewsItem) -> bool:
    title_lower = item.title.lower()
    return any(phrase in title_lower for phrase in DROP_PHRASES)


def _score_item(item: NewsItem) -> float:
    body_length = len(item.summary or "")
    keywords = sum(5 for keyword in ACTION_KEYWORDS if keyword in (item.summary or "").lower())
    title_keywords = sum(5 for keyword in ACTION_KEYWORDS if keyword in item.title.lower())
    return body_length + keywords + title_keywords


def _filter_post_market_items(items: List[NewsItem], now_ist: datetime) -> List[NewsItem]:
    start = datetime.combine(now_ist.date(), time(9, 15), tzinfo=IST)
    end = datetime.combine(now_ist.date(), time(15, 30), tzinfo=IST)

    filtered: List[NewsItem] = []
    closing_bell: Optional[NewsItem] = None

    for item in items:
        if not item.published_at:
            continue

        published_ist = item.published_at.astimezone(IST)
        if start <= published_ist <= end:
            if not _should_drop(item):
                filtered.append(item)
            continue

        if published_ist > end and "closing bell" in item.title.lower():
            if closing_bell is None or published_ist > closing_bell.published_at.astimezone(IST):
                closing_bell = item

    if closing_bell:
        filtered.append(closing_bell)

    return filtered


def _select_items(items: List[NewsItem]) -> List[NewsItem]:
    ranked = sorted(items, key=_score_item, reverse=True)
    return ranked[:15]


def _summarize_with_gemini(items: List[NewsItem]) -> List[str]:
    import google.generativeai as genai

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured")

    model_name = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)

    prompt_lines = [
        "You are summarizing Moneycontrol's Stock Market LIVE Updates for India equities.",
        "Use the provided intraday blocks (title + body).",
        "Produce 6-10 concise bullet 'Post-market Highlights' for the trading session.",
        "Rules: focus on equities actions, avoid URLs, avoid repetition, mention tickers if given,",
        "one bullet per line, no extra preamble.",
        "Items:",
    ]

    for idx, item in enumerate(items, start=1):
        body = item.summary or ""
        prompt_lines.append(f"{idx}. {item.title} — {body}")

    prompt = "\n".join(prompt_lines)
    response = model.generate_content(prompt)
    text = response.text if hasattr(response, "text") else ""

    bullets: List[str] = []
    for line in text.splitlines():
        stripped = line.strip("•-* \t")
        if not stripped:
            continue
        bullets.append(stripped)

    return bullets[:10]


def build_post_market_highlights(now_ist: datetime) -> Tuple[Optional[List[str]], Optional[str]]:
    if now_ist.timetz() < time(15, 30, tzinfo=IST):
        return None, None

    liveblog_url = os.getenv("MONEYCONTROL_LIVEBLOG_URL")
    if not liveblog_url:
        return None, "Highlights unavailable today."

    try:
        items = fetch_moneycontrol_liveblog(liveblog_url)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Failed to fetch liveblog: %s", exc)
        return None, "Highlights unavailable today."

    filtered = _filter_post_market_items(items, now_ist)
    if not filtered:
        return None, "Highlights unavailable today."

    selected = _select_items(filtered)
    try:
        bullets = _summarize_with_gemini(selected)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Gemini summarization failed: %s", exc)
        return None, "Highlights unavailable today."

    if not bullets:
        return None, "Highlights unavailable today."

    return bullets, None

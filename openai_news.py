"""News retrieval via OpenAI Responses API with web search."""

from __future__ import annotations

import logging
import os
import re
from difflib import SequenceMatcher
from datetime import datetime
from typing import List

from openai import OpenAI

PROMPT_TEMPLATE = """
You are writing a post-market news section for Indian equity traders.
Anchor the window to the Indian market session: from the last market close (IST) to now ({now_ist} IST).
Using web search, return 8-10 non-redundant items from this window.
Prefer items that impacted today’s session movers/sectors.
Each bullet must be one line: “WHAT happened — WHY it matters”.
No predictions, no hype, no generic filler. Exclude creator/compliance/education/distribution stories unless they visibly moved indices or a sector/stock. No URLs. No source tags.
"""


def _normalize_bullets(text: str) -> List[str]:
    bullets: List[str] = []
    for line in text.splitlines():
        cleaned = line.strip().lstrip("•-*\t ")
        if not cleaned:
            continue
        bullets.append(cleaned)
    return bullets


def _strip_sources(text: str) -> str:
    cleaned = text
    for token in ("Reuters", "Economic Times", "Moneycontrol", "Bloomberg", "CNBC"):
        cleaned = cleaned.replace(token, "")
    return cleaned


def _remove_urls(text: str) -> str:
    return re.sub(r"https?://\S+|www\.\S+", "", text)


def _remove_parenthetical_sources(text: str) -> str:
    cleaned = re.sub(r"\([^)]*\)", "", text)
    cleaned = re.sub(r"\[[^]]*\]", "", cleaned)
    return cleaned


def _cleanup_bullet(text: str) -> str:
    cleaned = text.replace("**", "")
    cleaned = _remove_urls(cleaned)
    cleaned = _remove_parenthetical_sources(cleaned)
    cleaned = _strip_sources(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -—\t")
    return cleaned


def _ensure_format(text: str) -> str:
    if " — " in text:
        return text
    for separator in (" - ", " – ", " — ", " —", "— "):
        if separator in text:
            parts = [part.strip() for part in text.split(separator, 1)]
            if len(parts) == 2 and all(parts):
                return f"{parts[0]} — {parts[1]}"
    if "—" in text:
        parts = [part.strip() for part in text.split("—", 1)]
        if len(parts) == 2 and all(parts):
            return f"{parts[0]} — {parts[1]}"
    if ". " in text:
        parts = [part.strip() for part in text.split(". ", 1)]
        if len(parts) == 2 and all(parts):
            return f"{parts[0]} — {parts[1]}"
    if ": " in text:
        parts = [part.strip() for part in text.split(": ", 1)]
        if len(parts) == 2 and all(parts):
            return f"{parts[0]} — {parts[1]}"
    return text


def _tokenize(text: str) -> set[str]:
    tokens = re.sub(r"[^a-z0-9 ]", " ", text.lower()).split()
    return {token for token in tokens if len(token) > 2}


def _similarity(left: str, right: str) -> float:
    left_tokens = _tokenize(left)
    right_tokens = _tokenize(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
    ratio = SequenceMatcher(None, left.lower(), right.lower()).ratio()
    return max(overlap, ratio)


def _dedupe_bullets(items: List[str]) -> List[str]:
    deduped: List[str] = []
    for item in items:
        if not deduped:
            deduped.append(item)
            continue
        replaced = False
        for idx, existing in enumerate(deduped):
            if _similarity(existing, item) >= 0.82:
                existing_words = len(existing.split())
                item_words = len(item.split())
                if item_words > existing_words + 1:
                    deduped[idx] = item
                replaced = True
                break
        if not replaced:
            deduped.append(item)
    return deduped


def _final_validate(items: List[str]) -> None:
    forbidden = ("http", "www", "**", "Reuters", "Economic Times", "Moneycontrol")
    joined = "\n".join(items)
    for token in forbidden:
        if token in joined:
            raise ValueError(f"Forbidden token found in news bullets: {token}")
    if len(items) != 5:
        raise ValueError("news_count must equal 5")


def fetch_india_market_news_openai(now_ist: datetime) -> List[str]:
    """Fetch 5 latest India market news bullets using OpenAI web search."""

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is required")

    client = OpenAI(api_key=api_key)
    prompt = PROMPT_TEMPLATE.format(now_ist=now_ist.strftime("%Y-%m-%d %H:%M"))

    try:
        response = client.responses.create(
            model="gpt-5.2",
            tools=[{"type": "web_search"}],
            input=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:  # noqa: BLE001
        logging.warning("OpenAI responses request failed: %s", exc)
        raise

    output_text = ""
    try:
        output_text = getattr(response, "output_text", "") or ""

        if not output_text and hasattr(response, "output") and response.output:
            collected: List[str] = []
            item_types: List[str] = []
            for item in response.output:
                item_types.append(type(item).__name__)
                contents = getattr(item, "content", None) or []
                for content_item in contents:
                    text_value = getattr(content_item, "text", None)
                    if text_value:
                        collected.append(text_value)
            if collected:
                output_text = "\n".join(collected)
            elif item_types:
                logging.debug("OpenAI response output types: %s", item_types)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Unable to parse OpenAI response content: %s", exc)

    if not output_text:
        logging.warning("OpenAI response contained no text output")
        return []

    bullets = _normalize_bullets(output_text)
    cleaned: List[str] = []
    for bullet in bullets:
        scrubbed = _cleanup_bullet(bullet)
        if not scrubbed:
            continue
        formatted = _ensure_format(scrubbed)
        if formatted:
            cleaned.append(formatted)

    deduped = _dedupe_bullets(cleaned)
    if len(deduped) < 5:
        logging.warning("Received fewer than 5 cleaned news bullets after dedupe")
    final_items = deduped[:5]
    _final_validate(final_items)
    return final_items

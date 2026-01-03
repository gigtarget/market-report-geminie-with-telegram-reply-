"""News retrieval via OpenAI Responses API with web search."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import List

from openai import OpenAI

PROMPT = """
You are writing a post-market news section for Indian stock market traders.
Using web search, return exactly 5 non-redundant items from the last 24 hours.
Each bullet must be one line: “WHAT happened — WHY it matters”.
Focus on India (NSE/BSE, Nifty/Sensex/Bank Nifty, RBI/SEBI, earnings, largecap moves). Avoid generic advice.
No URLs, no citations, no extra text.
"""


def _normalize_bullets(text: str) -> List[str]:
    bullets: List[str] = []
    for line in text.splitlines():
        cleaned = line.strip().lstrip("•-*\t ")
        if not cleaned:
            continue
        bullets.append(cleaned)
        if len(bullets) >= 5:
            break
    return bullets


def fetch_india_market_news_openai(now_ist: datetime) -> List[str]:
    """Fetch up to 5 latest India market news bullets using OpenAI web search."""

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is required")

    client = OpenAI(api_key=api_key)

    try:
        response = client.responses.create(
            model="gpt-5.2",
            tools=[{"type": "web_search"}],
            input=[{"role": "user", "content": PROMPT}],
        )
    except Exception as exc:  # noqa: BLE001
        logging.warning("OpenAI responses request failed: %s", exc)
        raise

    output_text = ""
    try:
        if hasattr(response, "output") and response.output:
            first_block = response.output[0]
            if hasattr(first_block, "content") and first_block.content:
                first_content = first_block.content[0]
                if hasattr(first_content, "text") and first_content.text:
                    output_text = first_content.text
    except Exception as exc:  # noqa: BLE001
        logging.warning("Unable to parse OpenAI response content: %s", exc)

    if not output_text:
        logging.warning("OpenAI response contained no text output")
        return []

    bullets = _normalize_bullets(output_text)
    if len(bullets) < 5:
        logging.warning("Received fewer than 5 news bullets from OpenAI")
    return bullets

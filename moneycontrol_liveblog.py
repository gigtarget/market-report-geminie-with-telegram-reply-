"""Fetch and parse Moneycontrol liveblog pages without heavy dependencies."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from typing import List, Optional
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")


@dataclass
class NewsItem:
    title: str
    link: str
    source_domain: str
    published_at: Optional[datetime]
    category: str
    summary: Optional[str] = None


class _TextExtractor(HTMLParser):
    """Lightweight HTML-to-text extractor.

    Collects text while ignoring script/style content and inserts newlines for common
    block elements to preserve ordering of liveblog entries.
    """

    def __init__(self) -> None:
        super().__init__()
        self._parts: List[str] = []
        self._skip_stack: List[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: D401 - HTMLParser API
        if tag in {"script", "style"}:
            self._skip_stack.append(tag)
            return

        if tag in {"br", "p", "div", "li", "section", "article", "h1", "h2", "h3"}:
            self._parts.append("\n")

    def handle_endtag(self, tag: str) -> None:  # noqa: D401 - HTMLParser API
        if self._skip_stack and self._skip_stack[-1] == tag:
            self._skip_stack.pop()
            return

        if tag in {"p", "div", "li", "section", "article"}:
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:  # noqa: D401 - HTMLParser API
        if self._skip_stack:
            return
        if data:
            self._parts.append(data)

    def get_lines(self) -> List[str]:
        text = unescape("".join(self._parts))
        text = text.replace("\xa0", " ")
        lines: List[str] = []
        for raw_line in text.splitlines():
            cleaned = " ".join(raw_line.split()).strip("•-· ")
            if cleaned:
                lines.append(cleaned)
        return lines


def _parse_timestamp(line: str) -> Optional[datetime]:
    match = re.search(
        r"([A-Za-z]+\s+\d{1,2},\s*\d{4})\s*[·\-]?\s*(\d{1,2}:\d{2})\s*IST",
        line,
    )
    if not match:
        return None

    date_part, time_part = match.groups()
    for fmt in ("%B %d, %Y %H:%M", "%B %d, %Y %I:%M"):
        try:
            naive = datetime.strptime(f"{date_part} {time_part}", fmt)
            return naive.replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def _clean_body(body_lines: List[str]) -> str:
    cleaned_lines: List[str] = []
    seen: set[str] = set()
    for line in body_lines:
        normalized = " ".join(line.split())
        normalized = re.sub(r"\bRead\s+More\b.*", "", normalized, flags=re.IGNORECASE).strip()
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        cleaned_lines.append(normalized)

    return " ".join(cleaned_lines).strip()


def fetch_moneycontrol_liveblog(url: str, timeout: int = 10) -> List[NewsItem]:
    """Fetch and parse Moneycontrol "Stock Market LIVE Updates" liveblog pages."""

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
        )
    }

    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()

    parser = _TextExtractor()
    parser.feed(response.text)
    lines = parser.get_lines()

    items: List[NewsItem] = []
    idx = 0
    while idx < len(lines):
        ts_candidate = _parse_timestamp(lines[idx])
        if not ts_candidate:
            idx += 1
            continue

        idx += 1
        title: Optional[str] = None
        while idx < len(lines) and not title:
            if lines[idx].strip():
                title = lines[idx].strip()
            idx += 1

        body_lines: List[str] = []
        while idx < len(lines):
            next_timestamp = _parse_timestamp(lines[idx])
            if next_timestamp:
                break
            if lines[idx].strip().replace(" ", "") == "-330":
                idx += 1
                break
            body_lines.append(lines[idx])
            idx += 1

        body = _clean_body(body_lines)
        if not title and not body:
            continue

        item = NewsItem(
            title=title or body[:80],
            link=url,
            source_domain="moneycontrol.com",
            published_at=ts_candidate,
            category="india_liveblog",
            summary=body or None,
        )
        items.append(item)

    logging.info("Parsed %s liveblog blocks from %s", len(items), url)
    return items

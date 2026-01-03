import logging
import re
import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import List, Optional, Tuple
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

import requests

DEFAULT_TIMEOUT = 10


RSS_SOURCES: List[Tuple[str, str]] = [
    # India-focused market and macro sources
    ("https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", "india"),
    ("https://www.moneycontrol.com/rss/latestnews.xml", "india"),
    ("https://www.business-standard.com/rss/markets-106.rss", "india"),
    ("https://www.livemint.com/rss/markets", "india"),
    # Global macro with India relevance
    ("https://feeds.reuters.com/reuters/INtopNews", "global"),
]


@dataclass
class NewsItem:
    title: str
    link: str
    source_domain: str
    published_at: Optional[datetime]
    category: str
    summary: Optional[str] = None


def _parse_datetime(value: str) -> Optional[datetime]:
    try:
        dt = parsedate_to_datetime(value)
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:  # noqa: BLE001
        return None


def _extract_text(element: Optional[ET.Element], tag_options: List[str]) -> Optional[str]:
    if not element:
        return None

    for tag in tag_options:
        child = element.find(tag)
        if child is not None and child.text:
            return child.text.strip()
    return None


def _clean_summary(raw: str) -> Optional[str]:
    if not raw:
        return None

    # Remove HTML tags and collapse whitespace
    without_tags = re.sub(r"<[^>]+>", " ", raw)
    unescaped = unescape(without_tags)
    collapsed = " ".join(unescaped.split())
    return collapsed or None


def _parse_item(item: ET.Element, source_url: str, category: str) -> Optional[NewsItem]:
    title = _extract_text(item, ["title"]) or ""
    link = _extract_text(item, ["link"]) or ""

    if not title or not link:
        return None

    domain = urlparse(link).netloc or urlparse(source_url).netloc

    published_text = _extract_text(item, ["pubDate", "published", "updated", "{http://purl.org/dc/elements/1.1/}date"])
    published_at = _parse_datetime(published_text) if published_text else None

    summary_raw = _extract_text(item, ["description", "summary"])
    summary = _clean_summary(summary_raw) if summary_raw else None

    return NewsItem(
        title=title.strip(),
        link=link.strip(),
        source_domain=domain,
        published_at=published_at,
        category=category,
        summary=summary,
    )


def fetch_rss_items() -> List[NewsItem]:
    items: List[NewsItem] = []

    for feed_url, category in RSS_SOURCES:
        try:
            response = requests.get(
                feed_url,
                headers={"User-Agent": f"news-fetcher/1.0 ({socket.gethostname()})"},
                timeout=DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to fetch RSS feed %s error=%s", feed_url, exc)
            continue

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as exc:
            logging.warning("Invalid RSS XML from %s error=%s", feed_url, exc)
            continue

        channel = root.find("channel") or root
        for item in channel.findall("item") or []:
            parsed = _parse_item(item, feed_url, category)
            if parsed:
                items.append(parsed)

        for entry in channel.findall("{http://www.w3.org/2005/Atom}entry"):
            parsed = _parse_item(entry, feed_url, category)
            if parsed:
                items.append(parsed)

    logging.info("Fetched %s news items from RSS", len(items))
    return items


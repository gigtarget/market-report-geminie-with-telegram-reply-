import logging
import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

import requests

DEFAULT_TIMEOUT = 10


RSS_SOURCES = [
    # India-focused market and macro sources
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://www.moneycontrol.com/rss/latestnews.xml",
    "https://www.business-standard.com/rss/markets-106.rss",
    "https://www.livemint.com/rss/markets",
    # Global macro with India relevance
    "https://feeds.reuters.com/reuters/INtopNews",
]


@dataclass
class NewsItem:
    title: str
    link: str
    source_domain: str
    published_at: Optional[datetime]
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


def _parse_item(item: ET.Element, source_url: str) -> Optional[NewsItem]:
    title = _extract_text(item, ["title"]) or ""
    link = _extract_text(item, ["link"]) or ""

    if not title or not link:
        return None

    domain = urlparse(link).netloc or urlparse(source_url).netloc

    published_text = _extract_text(item, ["pubDate", "published", "updated", "{http://purl.org/dc/elements/1.1/}date"])
    published_at = _parse_datetime(published_text) if published_text else None

    summary = _extract_text(item, ["description", "summary"])

    return NewsItem(
        title=title.strip(),
        link=link.strip(),
        source_domain=domain,
        published_at=published_at,
        summary=summary.strip() if summary else None,
    )


def fetch_rss_items() -> List[NewsItem]:
    items: List[NewsItem] = []

    for feed_url in RSS_SOURCES:
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
            parsed = _parse_item(item, feed_url)
            if parsed:
                items.append(parsed)

        for entry in channel.findall("{http://www.w3.org/2005/Atom}entry"):
            parsed = _parse_item(entry, feed_url)
            if parsed:
                items.append(parsed)

    logging.info("Fetched %s news items from RSS", len(items))
    return items


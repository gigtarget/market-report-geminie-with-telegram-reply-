import hashlib
import logging
import re
from difflib import SequenceMatcher
from typing import Iterable, List, Tuple
from urllib.parse import parse_qsl, urlparse, urlunparse

from news_fetch import NewsItem


CANONICAL_QUERY_DROP = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
}


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    query_pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k not in CANONICAL_QUERY_DROP]
    cleaned_query = "&".join(f"{k}={v}" for k, v in query_pairs)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, cleaned_query, ""))


def story_id_from_item(item: NewsItem) -> str:
    canonical = canonicalize_url(item.link)
    if canonical:
        base = canonical
    else:
        normalized_title = normalize_title(item.title)
        base = f"{normalized_title}|{item.source_domain}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


_PUNCT_RE = re.compile(r"[\W_]+", re.UNICODE)


def normalize_title(title: str) -> str:
    lowered = title.lower()
    stripped = _PUNCT_RE.sub(" ", lowered)
    return " ".join(stripped.split())


def filter_seen(items: Iterable[Tuple[NewsItem, str]], sent_store) -> List[Tuple[NewsItem, str]]:
    unseen: List[Tuple[NewsItem, str]] = []
    for item, story_id in items:
        if sent_store.is_sent(story_id):
            logging.info("Dropping already-sent story_id=%s title=%s", story_id, item.title)
            continue
        unseen.append((item, story_id))
    return unseen


def dedupe_similar(items: List[Tuple[NewsItem, str]], tier1_domains: List[str], similarity_threshold: float = 0.88) -> List[Tuple[NewsItem, str]]:
    if not items:
        return []

    tier1_set = {domain.lower() for domain in tier1_domains}

    def sort_key(pair: Tuple[NewsItem, str]):
        item, _ = pair
        ts = item.published_at.timestamp() if item.published_at else 0
        return (
            item.source_domain.lower() in tier1_set,
            ts,
        )

    sorted_items = sorted(items, key=sort_key, reverse=True)
    kept: List[Tuple[NewsItem, str]] = []
    normalized_titles: List[str] = []

    for item, story_id in sorted_items:
        title_norm = normalize_title(item.title)
        is_duplicate = False
        for existing_title in normalized_titles:
            similarity = SequenceMatcher(None, title_norm, existing_title).ratio()
            if similarity >= similarity_threshold:
                logging.info(
                    "Dropping near-duplicate title similarity=%.3f title=%s", similarity, item.title
                )
                is_duplicate = True
                break

        if not is_duplicate:
            kept.append((item, story_id))
            normalized_titles.append(title_norm)

    return kept


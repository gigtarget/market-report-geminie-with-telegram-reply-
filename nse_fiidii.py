import csv
import io
import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

BASE_PAGE = "https://www.nseindia.com/"
REPORT_PAGE = "https://www.nseindia.com/reports/fii-dii"
CSV_API_URL = "https://www.nseindia.com/api/fiidiiTradeReact?csv=true"
CACHE_TTL = timedelta(minutes=10)

_CACHE: Dict[str, Optional[object]] = {"data": None, "timestamp": None}

# NOTE:
# - Do NOT include "br" in Accept-Encoding (brotli can break decoding in some deploys).
# - Keep this "browser-ish" but not too strict.
_COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": REPORT_PAGE,
}

SAFE_PREVIEW_LENGTH = 200


@dataclass
class ParticipantFlow:
    buy: float
    sell: float
    net: float


@dataclass
class FiiDiiData:
    as_on: str
    as_on_date: Optional[date]
    fii: Optional[ParticipantFlow]
    dii: Optional[ParticipantFlow]
    from_cache: bool = False


def _normalize_header(value: str) -> str:
    """
    Normalize header names reliably:
    - remove UTF-8 BOM
    - collapse whitespace including newlines/tabs
    - lowercase
    """
    value = (value or "").lstrip("\ufeff")
    value = re.sub(r"\s+", " ", value).strip().lower()
    return value


def _safe_preview(text: str, length: int = SAFE_PREVIEW_LENGTH) -> str:
    return (text or "")[:length].replace("\n", " ").replace("\r", " ").strip()


def _validate_csv_payload(content: str) -> None:
    snippet = (content or "").lstrip()
    if not snippet:
        raise ValueError("Empty response body from NSE")
    # Only reject obvious non-CSV payloads
    if snippet.startswith("<") or snippet.startswith("{"):
        raise ValueError(f"Unexpected NSE payload {_safe_preview(snippet)}")


def _clean_float(value: str) -> Optional[float]:
    if value is None:
        return None
    cleaned = value.replace(",", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:  # noqa: BLE001
        return None


def _find_column(
    fieldnames: List[str],
    keywords: List[str],
    require_all: bool = False,
) -> Optional[str]:
    """
    Fuzzy find column by keywords.
    - require_all=False => any keyword match
    - require_all=True  => all keywords must match
    """
    keywords_norm = [k.lower() for k in keywords]
    for name in fieldnames:
        normalized = _normalize_header(name)
        if require_all:
            if all(k in normalized for k in keywords_norm):
                return name
        else:
            if any(k in normalized for k in keywords_norm):
                return name
    return None


def _find_value_column(fieldnames: List[str], keyword: str) -> Optional[str]:
    """
    Prefer columns like "BUY VALUE (₹ Crores)" over just "BUY".
    """
    best: Optional[str] = None
    best_score = -1
    k = keyword.lower()

    for name in fieldnames:
        normalized = _normalize_header(name)
        if k not in normalized:
            continue

        score = 1
        # Prefer value columns
        if "value" in normalized:
            score += 2
        if "crore" in normalized or "₹" in name:
            score += 1

        if score > best_score:
            best = name
            best_score = score

    return best


def _create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(_COMMON_HEADERS)
    return session


def _decode_response_content(response: requests.Response) -> str:
    raw = response.content or b""
    if not raw:
        return ""

    decoded = raw.decode("utf-8", errors="replace")
    replacement_count = decoded.count("\ufffd")
    if replacement_count:
        replacement_ratio = replacement_count / max(len(decoded), 1)
        if replacement_count > 10 and replacement_ratio > 0.02:
            fallback = raw.decode("latin-1", errors="replace")
            if fallback.count("\ufffd") < replacement_count:
                decoded = fallback

    # Strip BOM if present
    return decoded.lstrip("\ufeff")


def _parse_date(value: str) -> Optional[date]:
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except Exception:  # noqa: BLE001
            continue
    return None


def _maybe_fix_missing_newlines(content: str) -> str:
    """
    NSE sometimes returns a CSV where record separators appear as: ...") "DII",...
    (i.e., quote, whitespace, quote) due to transport/formatting or logging artifacts.
    If we see almost no newlines, try inserting them between records.
    This is a best-effort fix and only runs when newline count is suspiciously low.
    """
    if not content:
        return content
    # If we have at least header newline + rows, leave it.
    if content.count("\n") >= 2:
        return content

    # If everything is on one line, insert newlines between records safely.
    # Replace: `" "DII"` -> `"\n"DII"` (same for FII/FPI etc.)
    # We only insert before a starting quote that begins a known category row.
    pattern = r'"\s+(?="(?:DII|FII/FPI|FII|FPI)")'
    fixed = re.sub(pattern, '"\n', content)
    return fixed


def _parse_csv(content: str) -> FiiDiiData:
    content = (content or "").lstrip("\ufeff")
    _validate_csv_payload(content)

    content = _maybe_fix_missing_newlines(content)

    reader = csv.DictReader(io.StringIO(content))
    raw_fieldnames = reader.fieldnames or []
    if not raw_fieldnames:
        raise ValueError("CSV response missing header")

    # Build normalized header list and force DictReader to use them
    normalized_fieldnames = [_normalize_header(name) for name in raw_fieldnames]
    reader.fieldnames = normalized_fieldnames
    fieldnames = normalized_fieldnames

    # Find columns in normalized space
    date_col = _find_column(fieldnames, ["date"])
    cat_col = _find_column(fieldnames, ["category"]) or _find_column(fieldnames, ["client", "type"])
    buy_col = _find_value_column(fieldnames, "buy") or _find_column(fieldnames, ["buy"])
    sell_col = _find_value_column(fieldnames, "sell") or _find_column(fieldnames, ["sell"])
    net_col = _find_value_column(fieldnames, "net") or _find_column(fieldnames, ["net"])

    if not all([date_col, cat_col, buy_col, sell_col, net_col]):
        raise ValueError(
            f"Required columns not found in CSV. "
            f"date_col={date_col} cat_col={cat_col} buy_col={buy_col} sell_col={sell_col} net_col={net_col} "
            f"headers={fieldnames}"
        )

    rows = list(reader)
    if not rows:
        raise ValueError("CSV response has no data rows")

    dated_rows = []
    for row in rows:
        date_value = (row.get(date_col) or "").strip()
        parsed_date = _parse_date(date_value)
        dated_rows.append((parsed_date, date_value, row))

    # Sort by parsed date; None dates go first
    dated_rows.sort(key=lambda item: (item[0] or datetime.min.date()))
    latest_parsed_date, latest_date_str, _ = dated_rows[-1]

    fii_row = None
    dii_row = None

    for parsed_date, original_date, row in dated_rows:
        # Only match rows for latest date (if date exists). If date parse failed, keep scanning.
        if latest_parsed_date is not None and parsed_date != latest_parsed_date:
            continue

        participant = _normalize_header(row.get(cat_col, ""))
        # NSE uses "FII/FPI"
        if fii_row is None and ("fii" in participant or "fpi" in participant):
            fii_row = row
            latest_date_str = original_date or latest_date_str
        if dii_row is None and "dii" in participant:
            dii_row = row
            latest_date_str = original_date or latest_date_str

    def build_flow(row: Optional[Dict[str, str]]) -> Optional[ParticipantFlow]:
        if not row:
            return None
        buy = _clean_float(row.get(buy_col))
        sell = _clean_float(row.get(sell_col))
        net = _clean_float(row.get(net_col))
        if buy is None or sell is None or net is None:
            return None
        return ParticipantFlow(buy=buy, sell=sell, net=net)

    data = FiiDiiData(
        as_on=latest_date_str,
        as_on_date=latest_parsed_date,
        fii=build_flow(fii_row),
        dii=build_flow(dii_row),
    )

    logging.info(
        "Parsed NSE FII/DII data as_on=%s fii_net=%s dii_net=%s",
        data.as_on,
        data.fii.net if data.fii else None,
        data.dii.net if data.dii else None,
    )

    return data


def _fetch_fresh_data() -> FiiDiiData:
    retries = 2
    delay = 0.5
    session = _create_session()

    for attempt in range(retries + 1):
        start_time = time.monotonic()
        response = None

        try:
            logging.info("Starting NSE FII/DII fetch attempt=%s", attempt + 1)

            # Warm-ups (root might 403; keep going)
            warm_home = session.get(BASE_PAGE, timeout=10)
            logging.info(
                "NSE warm-up root status=%s bytes=%s",
                warm_home.status_code,
                len(warm_home.content or b""),
            )
            if warm_home.status_code == 403:
                logging.warning("NSE warm-up root returned 403; continuing")

            warm_resp = session.get(REPORT_PAGE, timeout=10)
            logging.info(
                "NSE warm-up report status=%s bytes=%s",
                warm_resp.status_code,
                len(warm_resp.content or b""),
            )
            if warm_resp.status_code != 200:
                raise ValueError(f"Unexpected report warm-up status={warm_resp.status_code}")

            api_headers = {
                **session.headers,
                # This matters: force CSV accept for API call
                "Accept": "text/csv,*/*;q=0.9",
                "Referer": REPORT_PAGE,
            }

            response = session.get(CSV_API_URL, headers=api_headers, timeout=10)

            duration = time.monotonic() - start_time
            content_length = len(response.content or b"")

            logging.info(
                "NSE FII/DII fetch attempt=%s status=%s bytes=%s duration=%.3fs",
                attempt + 1,
                response.status_code,
                content_length,
                duration,
            )

            if response.status_code in (403, 429):
                raise ValueError(
                    f"Unexpected response status={response.status_code} length={content_length}"
                )

            decoded_text = _decode_response_content(response)

            try:
                return _parse_csv(decoded_text)
            except Exception as parse_exc:  # noqa: BLE001
                preview = _safe_preview(decoded_text)
                logging.warning(
                    "NSE FII/DII parsing failed attempt=%s status=%s content_type=%s content_encoding=%s "
                    "bytes=%s preview=%s error=%s",
                    attempt + 1,
                    response.status_code,
                    response.headers.get("content-type"),
                    response.headers.get("content-encoding"),
                    content_length,
                    preview,
                    parse_exc,
                )
                raise

        except Exception as exc:  # noqa: BLE001
            duration = time.monotonic() - start_time
            status = response.status_code if response is not None else "no-response"
            content_type = response.headers.get("content-type") if response is not None else "unknown"
            content_length = len(response.content or b"") if response is not None else 0
            decoded_text = _decode_response_content(response) if response is not None else ""
            preview = _safe_preview(decoded_text)

            logging.warning(
                "NSE FII/DII fetch failed attempt=%s duration=%.3fs status=%s content_type=%s bytes=%s preview=%s error=%s",
                attempt + 1,
                duration,
                status,
                content_type,
                content_length,
                preview,
                exc,
            )

            if attempt < retries:
                time.sleep(delay)
                delay *= 3
            else:
                raise


def _get_cached() -> Optional[FiiDiiData]:
    cached_data = _CACHE.get("data")
    cached_time = _CACHE.get("timestamp")
    if cached_data and cached_time:
        if datetime.utcnow() - cached_time < CACHE_TTL:
            return cached_data
    return None


def get_fii_dii_data() -> Tuple[Optional[FiiDiiData], Optional[str]]:
    cached = _get_cached()
    try:
        data = _fetch_fresh_data()
        _CACHE["data"] = data
        _CACHE["timestamp"] = datetime.utcnow()
        return data, None
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to fetch NSE FII/DII data", exc_info=exc)
        if cached:
            cached.from_cache = True
            return cached, "Using cached FII/DII due to upstream failure"
        return None, "FII/DII unavailable (NSE blocked/upstream error)"

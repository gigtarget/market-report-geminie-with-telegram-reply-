import csv
import io
import logging
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

_COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
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
    return " ".join(value.strip().lower().split())


def _safe_preview(text: str, length: int = SAFE_PREVIEW_LENGTH) -> str:
    return (text or "")[:length].replace("\n", " ").replace("\r", " ").strip()


def _validate_csv_payload(content: str) -> None:
    snippet = (content or "").lstrip()
    if not snippet:
        raise ValueError("Empty response body from NSE")
    if snippet.startswith("<"):
        raise ValueError(f"Received HTML instead of CSV: {_safe_preview(snippet)}")
    if snippet.startswith("{"):
        raise ValueError(f"Received JSON instead of CSV: {_safe_preview(snippet)}")


def _clean_float(value: str) -> Optional[float]:
    if value is None:
        return None
    cleaned = value.replace(",", "").strip()
    if not cleaned:
        return None
    return float(cleaned)


def _find_column(fieldnames: List[str], keywords: List[str]) -> Optional[str]:
    normalized_map = {_normalize_header(name): name for name in fieldnames}
    for normalized, original in normalized_map.items():
        for keyword in keywords:
            if keyword.lower() in normalized:
                return original
    return None


def _create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(_COMMON_HEADERS)
    return session


def _parse_date(value: str) -> Optional[date]:
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except Exception:  # noqa: BLE001
            continue
    return None


def _parse_csv(content: str) -> FiiDiiData:
    _validate_csv_payload(content)

    reader = csv.DictReader(io.StringIO(content))
    fieldnames = reader.fieldnames or []
    if not fieldnames:
        raise ValueError("CSV response missing header")

    date_col = _find_column(fieldnames, ["date"])
    client_col = _find_column(fieldnames, ["client", "category", "type"])
    buy_col = _find_column(fieldnames, ["buy"])
    sell_col = _find_column(fieldnames, ["sell"])
    net_col = _find_column(fieldnames, ["net"])

    if not all([date_col, client_col, buy_col, sell_col, net_col]):
        raise ValueError("Required columns not found in CSV")

    rows = list(reader)
    if not rows:
        raise ValueError("CSV response has no data rows")

    dated_rows = []
    for row in rows:
        date_value = row.get(date_col, "").strip()
        parsed_date = _parse_date(date_value)
        dated_rows.append((parsed_date, date_value, row))

    dated_rows.sort(key=lambda item: (item[0] or datetime.min.date()))
    latest_parsed_date, latest_date_str, _ = dated_rows[-1]

    fii_row = None
    dii_row = None
    for parsed_date, original_date, row in dated_rows:
        if parsed_date != latest_parsed_date and parsed_date is not None:
            continue
        participant = _normalize_header(row.get(client_col, ""))
        if not fii_row and ("fii" in participant or "fpi" in participant):
            fii_row = row
            latest_date_str = original_date or latest_date_str
        if not dii_row and ("dii" in participant or "domestic" in participant):
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
        "Parsed NSE FII/DII data as_on=%s fii_found=%s dii_found=%s",
        data.as_on,
        data.fii is not None,
        data.dii is not None,
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
            warm_home = session.get(BASE_PAGE, timeout=10)
            logging.info(
                "NSE warm-up root status=%s bytes=%s",
                warm_home.status_code,
                len(warm_home.content or b""),
            )
            warm_resp = session.get(REPORT_PAGE, timeout=10)
            logging.info(
                "NSE warm-up report status=%s bytes=%s",
                warm_resp.status_code,
                len(warm_resp.content or b""),
            )
            api_headers = {
                **session.headers,
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

            try:
                return _parse_csv(response.text)
            except Exception as parse_exc:  # noqa: BLE001
                preview = _safe_preview(response.text)
                logging.warning(
                    "NSE FII/DII parsing failed attempt=%s status=%s content_type=%s bytes=%s preview=%s error=%s",
                    attempt + 1,
                    response.status_code,
                    response.headers.get("content-type"),
                    content_length,
                    preview,
                    parse_exc,
                )
                raise
        except Exception as exc:  # noqa: BLE001
            duration = time.monotonic() - start_time
            status = response.status_code if response is not None else "no-response"
            content_type = (
                response.headers.get("content-type") if response is not None else "unknown"
            )
            content_length = len(response.content or b"") if response is not None else 0
            preview = _safe_preview(response.text if response is not None else "")
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

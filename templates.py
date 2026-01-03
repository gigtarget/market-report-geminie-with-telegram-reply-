import logging
import random
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from db import ensure_template_table, fetch_templates, seed_templates_if_empty

TEMPLATE_NAME = "post_market_opening"


def initialize_templates_store(seed_path: Optional[Path] = None) -> None:
    seed_file = seed_path or Path(__file__).with_name("seed_templates.sql")
    try:
        ensure_template_table()
        seed_templates_if_empty(seed_file, TEMPLATE_NAME)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Template store initialization failed: %s", exc)


def classify_market(indices: Dict[str, float], market_closed: bool = False) -> Tuple[str, str, str]:
    nifty_pct = indices.get("Nifty 50", 0.0)
    sensex_pct = indices.get("Sensex", 0.0)
    banknifty_pct = indices.get("Nifty Bank", 0.0)

    if market_closed:
        direction = "closed"
    else:
        all_positive = all(pct > 0.10 for pct in (nifty_pct, sensex_pct, banknifty_pct))
        all_negative = all(pct < -0.10 for pct in (nifty_pct, sensex_pct, banknifty_pct))
        all_flat = all(abs(pct) < 0.10 for pct in (nifty_pct, sensex_pct, banknifty_pct))

        if all_positive:
            direction = "up"
        elif all_negative:
            direction = "down"
        elif all_flat:
            direction = "flat"
        else:
            direction = "mixed"

    avg_strength = (abs(nifty_pct) + abs(sensex_pct) + abs(banknifty_pct)) / 3
    if avg_strength < 0.30:
        strength = "mild"
    elif avg_strength < 0.80:
        strength = "solid"
    else:
        strength = "strong"

    leader_value = max(
        (
            ("nifty", abs(nifty_pct), nifty_pct),
            ("sensex", abs(sensex_pct), sensex_pct),
            ("banknifty", abs(banknifty_pct), banknifty_pct),
        ),
        key=lambda item: item[1],
    )
    leader = leader_value[0]

    return direction, strength, leader


class _SafeDict(dict):
    def __missing__(self, key):  # pragma: no cover - simple fallback
        return "{" + key + "}"


def _format_pct(value: float) -> str:
    return f"{value:+.2f}%"


def _build_placeholder_values(
    session_date: date,
    nifty_pct: float,
    sensex_pct: float,
    banknifty_pct: float,
    leader: str,
) -> Dict[str, str]:
    return {
        "leader_name": {
            "nifty": "Nifty",
            "sensex": "Sensex",
            "banknifty": "Bank Nifty",
        }.get(leader, "Markets"),
        "nifty_pct": _format_pct(nifty_pct),
        "sensex_pct": _format_pct(sensex_pct),
        "banknifty_pct": _format_pct(banknifty_pct),
        "session_date": session_date.strftime("%d-%b-%Y"),
    }


def _choose_template(templates: List[Tuple[int, str, str, str, int]], seed_value: str) -> Optional[Tuple[int, str, str, str, int]]:
    if not templates:
        return None
    rng = random.Random(seed_value)
    return rng.choice(templates)


def _filter_templates(
    templates: List[Tuple[int, str, str, str, int]], strength: str, leader: str
) -> List[Tuple[int, str, str, str, int]]:
    priority_order = [
        (strength, leader),
        (strength, "any"),
        ("any", leader),
        ("any", "any"),
    ]

    filtered: List[Tuple[int, str, str, str, int]] = []
    for strength_key, leader_key in priority_order:
        matches = [t for t in templates if t[1] == strength_key and t[2] == leader_key]
        if matches:
            filtered.extend(matches)
            break
    return filtered


def get_opening_line(
    session_date: date,
    market_closed: bool,
    nifty_pct: float,
    sensex_pct: float,
    banknifty_pct: float,
    leader: str,
    strength: str,
    direction: str,
) -> str:
    placeholders = _build_placeholder_values(
        session_date, nifty_pct, sensex_pct, banknifty_pct, leader
    )
    seed_value = f"{session_date.isoformat()}|{direction}|{leader}|{strength}"

    try:
        db_templates = fetch_templates(TEMPLATE_NAME, direction)
        candidate_templates = _filter_templates(db_templates, strength, leader)
        chosen = _choose_template(candidate_templates, seed_value)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Failed to load templates from database: %s", exc)
        chosen = None

    if chosen:
        template_id, _, _, template_text, _ = chosen
        logging.info(
            "Opening line selected: direction=%s strength=%s leader=%s template_id=%s",
            direction,
            strength,
            leader,
            template_id,
        )
        template = template_text
    else:
        fallback_templates = [
            "Bulls stayed in control; {leader_name} led the close.",
            "Choppy tape today with leadership from {leader_name}.",
            "Quiet finish as traders tracked {leader_name} moves.",
            "Markets held steady ahead of the next session.",
            "Mixed signals in play; eyes on {leader_name} into the close.",
        ]
        template = _choose_template([(0, "any", "any", text, 0) for text in fallback_templates], seed_value)[3]
        logging.info(
            "Opening line selected: direction=%s strength=%s leader=%s template_id=%s",
            direction,
            strength,
            leader,
            "fallback",
        )

    return template.format_map(_SafeDict(placeholders))

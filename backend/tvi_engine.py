"""TVI computation engine."""
from __future__ import annotations

from typing import Dict


def _safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _experience_score(class_year: str | None) -> float:
    if not class_year:
        return 0.5
    normalized = class_year.strip().lower()
    if normalized in {"sr", "senior"}:
        return 1.0
    if normalized in {"jr", "junior"}:
        return 0.8
    if normalized in {"so", "soph", "sophomore"}:
        return 0.6
    if normalized in {"fr", "freshman"}:
        return 0.4
    return 0.5


def compute_tvi(stats: Dict, player_meta: Dict) -> Dict[str, Dict[str, float] | float]:
    snaps = float(stats.get("snaps", 0) or 0)
    yards = float(stats.get("yards", 0) or 0)
    tds = float(stats.get("tds", 0) or 0)
    interceptions = float(stats.get("ints", 0) or 0)
    games = float(stats.get("games_played", 0) or 0)

    usage = min(_safe_divide(snaps, 800), 1.0)
    efficiency = _safe_divide(yards + 20 * tds + 5 * interceptions, snaps)
    durability = _safe_divide(games, 12)
    experience = _experience_score(player_meta.get("class_year"))

    components = {
        "usage": usage,
        "efficiency": efficiency,
        "durability": durability,
        "experience": experience,
    }
    tvi = sum(components.values()) / len(components)

    return {"tvi": tvi, "components": components}

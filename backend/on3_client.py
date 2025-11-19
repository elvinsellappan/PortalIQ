"""
on3_client.py

Lightweight client to pull transfer portal data from On3's
public College Football Transfer Portal "wire" page:

    https://www.on3.com/transfer-portal/wire/football/

This DOES NOT use any private API. It:
- Downloads the HTML for the wire page
- Parses the player cards
- Extracts: position, name, class, height, weight, high school, rating,
  status, entered date, and associated college (if present)
- Returns a list of normalized dicts for use in ingestion.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

WIRE_URL = "https://www.on3.com/transfer-portal/wire/football/"

# Simple browser UA so we don't look like a bot
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _fetch_wire_html() -> str:
    """Fetch the HTML for the On3 transfer portal wire page."""
    resp = requests.get(
        WIRE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"On3 wire request failed: {resp.status_code} {resp.text[:400]}"
        )
    return resp.text


def _parse_height_weight(line: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Line example: 'RS-JR / 6-3 / 275'
    Returns (class_year, height, weight)
    """
    parts = [p.strip() for p in line.split("/")]

    class_year = parts[0] if len(parts) > 0 else None
    height = parts[1] if len(parts) > 1 else None
    weight = parts[2] if len(parts) > 2 else None
    return class_year, height, weight


def _extract_rating(lines: List[str]) -> Optional[float]:
    """
    Find the first line that looks like a rating, e.g. '89.15'
    """
    for line in lines:
        m = re.match(r"^\d{2,3}\.\d{2}$", line.strip())
        if m:
            try:
                return float(m.group(0))
            except ValueError:
                continue
    return None


def _extract_status_and_date(lines: List[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Look for lines like:
      'Entered 11/16/2025'
      'Committed'
      'Expected'
    """
    status = None
    date = None

    for line in lines:
        line = line.strip()
        if line.startswith("Entered "):
            status = "Entered"
            date = line.replace("Entered ", "").strip()
            break
        if line in ("Committed", "Expected"):
            # We'll keep last one found if multiple
            status = line

    return status, date


def _extract_team_from_links(container) -> Optional[str]:
    """
    Try to get the college team name from the first '/college/' link in the card.
    """
    college_link = container.find("a", href=re.compile(r"/college/"))
    if college_link:
        text = college_link.get_text(strip=True)
        return text or None
    return None


def _normalize_player_card(container) -> Optional[Dict[str, Any]]:
    """
    Convert one player "card" container into a dict with normalized fields.
    We assume:
      - First line is position (DL, CB, WR, etc.)
      - Second line is player name
      - Somewhere below is 'RS-SO / 6-3 / 275'
      - Somewhere below is 'High School (City, ST)'
      - Somewhere below is rating like '89.15'
      - Somewhere below is 'Entered mm/dd/yyyy' or 'Expected' / 'Committed'
    """
    # Get all visible text in the container as separate lines
    text_lines = [
        t.strip()
        for t in container.get_text(separator="\n").split("\n")
        if t.strip()
    ]

    if len(text_lines) < 2:
        # too small to be a real player card
        return None

    # Heuristics based on the PDF:
    position = text_lines[0]
    name = text_lines[1]

    class_year = None
    height = None
    weight = None
    high_school = None
    rating = _extract_rating(text_lines)
    status, entered_date = _extract_status_and_date(text_lines)

    # Parse class/year/height/weight from the first line containing '/'
    for line in text_lines[2:]:
        if "/" in line and any(ch.isdigit() for ch in line):
            class_year, height, weight = _parse_height_weight(line)
            break

    # First line with parentheses is almost always "High School (City, ST)"
    for line in text_lines[2:]:
        if "(" in line and ")" in line:
            high_school = line
            break

    team = _extract_team_from_links(container)

    return {
        "player_name": name,
        "position": position,
        "class_year": class_year,
        "height": height,
        "weight": weight,
        "high_school": high_school,
        "rating": rating,
        "status": status,
        "entered_date": entered_date,
        "on3_team": team,
        # Keep raw lines in case we want to debug or extend later
        "raw_lines": text_lines,
    }


def get_on3_transfers(limit: int | None = None) -> List[Dict[str, Any]]:
    """
    Scrape the On3 transfer portal wire page and return a list of player dicts.

    For MVP:
      - We only scrape the first page (latest transfers).
      - If `limit` is provided, we truncate the list.

    Each dict has keys:
      - player_name
      - position
      - class_year
      - height
      - weight
      - high_school
      - rating
      - status
      - entered_date
      - on3_team
      - raw_lines
    """
    html = _fetch_wire_html()
    soup = BeautifulSoup(html, "html.parser")

    players: List[Dict[str, Any]] = []

    # Heuristic: each player name is linked to a /rivals/ profile, e.g.
    # https://www.on3.com/rivals/malachi-madison-81432/
    player_links = soup.find_all("a", href=re.compile(r"/rivals/"))

    seen_names = set()

    for link in player_links:
        # Climb to the card container. Parent is often enough, but we can
        # go up two levels just in case.
        container = link.parent
        if container is None:
            continue

        # If the direct parent is too small, go one more up
        # (basic safeguard; we rely on text_lines length).
        card = container

        player = _normalize_player_card(card)
        if not player:
            continue

        # Deduplicate by player_name + status + entered_date
        key = (player["player_name"], player["status"], player["entered_date"])
        if key in seen_names:
            continue
        seen_names.add(key)

        players.append(player)

        if limit is not None and len(players) >= limit:
            break

    return players

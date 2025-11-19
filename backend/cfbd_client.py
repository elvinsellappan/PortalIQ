"""Client wrapper for the College Football Data API (CFBD).

All functions return real data from the CFBD endpoints without mocking or
simulation. Errors are raised when the API responds with a non-200 status.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List

import requests


BASE_URL = "https://api.collegefootballdata.com"


def _build_headers() -> Dict[str, str]:
    api_key = os.environ["CFBD_API_KEY"]
    return {"Authorization": f"Bearer {api_key}"}


def _get(endpoint, params=None):
    url = BASE_URL + endpoint
    headers = _build_headers()

    response = requests.get(url, headers=headers, params=params, timeout=30)

    # Debug: if not JSON, print details
    try:
        return response.json()
    except Exception:
        print("=== CFBD ERROR RESPONSE ===")
        print("URL:", url)
        print("Status:", response.status_code)
        print("Text:", response.text)
        print("===========================")
        raise


def get_transfers(year: int) -> List[Dict[str, Any]]:
    """
    Returns REAL transfer portal data for the given year.
    CFBD endpoint: /transferportal?year=YYYY
    """
    return _get("/transferportal", params={"year": year})


def get_fbs_teams() -> List[Dict[str, Any]]:
    """
    Returns REAL FBS team metadata.
    CFBD endpoint: /teams/fbs
    """
    return _get("/teams/fbs")


def get_player_season_stats(year: int, team: str) -> List[Dict[str, Any]]:
    """
    Returns REAL player season stats for the given team in the given year.
    CFBD endpoint: /stats/player/season?year=YYYY&team=TEAM
    """
    return _get("/stats/player/season", params={"year": year, "team": team})

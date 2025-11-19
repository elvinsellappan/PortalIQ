"""
Client wrapper for the College Football Data API (CFBD).

All functions return real CFBD data. 
Handles non-list responses, errors, HTML responses, and correct portal endpoints.
"""
import os
from typing import Any, Dict, List, Union

import requests


BASE_URL = "https://api.collegefootballdata.com/api"


def _build_headers() -> Dict[str, str]:
    api_key = os.environ["CFBD_API_KEY"].strip()
    return {"Authorization": f"Bearer {api_key}"}


def _safe_json(response: requests.Response) -> Union[List, Dict]:
    """Safely parse JSON, raising with useful debug info if data is HTML or invalid."""
    try:
        return response.json()
    except Exception:
        raise RuntimeError(
            f"CFBD returned non-JSON response:\n"
            f"Status: {response.status_code}\n"
            f"URL: {response.url}\n"
            f"Text: {response.text[:500]}"
        )


def _get(path: str, params: Dict[str, Any] | None = None) -> Union[List, Dict]:
    url = f"{BASE_URL}{path}"
    response = requests.get(url, headers=_build_headers(), params=params, timeout=30)

    if response.status_code != 200:
        raise RuntimeError(
            f"CFBD request failed:\n"
            f"Status: {response.status_code}\n"
            f"URL: {response.url}\n"
            f"Text: {response.text[:500]}"
        )

    return _safe_json(response)


# --------------------------
# PUBLIC FUNCTIONS
# --------------------------

def get_transfers(year: int) -> List[Dict[str, Any]]:
    """
    Correct CFBD endpoint:
    /api/portal/players?classification=transfer&year=YYYY
    """
    data = _get(
        "/portal/players",
        params={"classification": "transfer", "year": year},
    )

    # CFBD sometimes returns a dict with key "players"
    if isinstance(data, dict) and "players" in data:
        return data["players"]

    # Otherwise, return as-is (must be list)
    if isinstance(data, list):
        return data

    raise RuntimeError(f"Unexpected transfer portal format: {type(data)}")


def get_fbs_teams() -> List[Dict[str, Any]]:
    """REAL FBS teams."""
    data = _get("/teams/fbs")

    if isinstance(data, list):
        return data
    raise RuntimeError("Unexpected FBS teams format")


def get_player_season_stats(year: int, team: str) -> List[Dict[str, Any]]:
    """
    Returns REAL player-season stats.
    CFBD endpoint:
    /stats/player/season?year=YYYY&team=TEAM
    """
    data = _get(
        "/stats/player/season",
        params={"year": year, "team": team},
    )

    if isinstance(data, list):
        return data

    # Sometimes CFBD returns a dict with "stats" key
    if isinstance(data, dict) and "stats" in data:
        return data["stats"]

    raise RuntimeError(f"Unexpected stats format for team {team}: {type(data)}")

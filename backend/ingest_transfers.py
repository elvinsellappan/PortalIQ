"""Ingestion pipeline for transfer portal players using On3 portal data."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from on3_client import get_on3_transfers
from cfbd_client import get_fbs_teams, get_player_season_stats  # still used for team metadata + stats
from supabase_client import get_supabase
from tvi_engine import compute_tvi


# ---------------------------------------------------------
# TEAM INGESTION (CFBD still works for team metadata)
# ---------------------------------------------------------

def _normalize_team_payload(team: Dict) -> Dict:
    logos = team.get("logos") or []
    logo_url = logos[0] if isinstance(logos, list) and logos else None
    return {
        "name": team.get("school") or team.get("team") or team.get("name"),
        "short_name": team.get("mascot") or team.get("short_name"),
        "conference": team.get("conference"),
        "school_id": team.get("id"),
        "logo_url": logo_url,
    }


def _build_team_index(team_rows: Iterable[Dict]) -> Dict[str, Dict]:
    """Create lookup by school_id and by name."""
    index: Dict[str, Dict] = {}
    for row in team_rows:
        if row.get("school_id"):
            index[str(row["school_id"]).lower()] = row
        if row.get("name"):
            index[row["name"].strip().lower()] = row
    return index


def _upsert_teams() -> Dict[str, Dict]:
    """Ensure FBS teams exist in Supabase and build lookup."""
    supabase = get_supabase()
    teams = get_fbs_teams()
    payload = [_normalize_team_payload(team) for team in teams if team]

    response = supabase.table("teams").upsert(
        payload, on_conflict="school_id"
    ).execute()

    data = response.data or []
    return _build_team_index(data)


# ---------------------------------------------------------
# SEASON INGESTION
# ---------------------------------------------------------

def _ensure_season(year: int) -> int:
    supabase = get_supabase()
    result = (
        supabase.table("seasons")
        .upsert({"year": year}, on_conflict="year")
        .execute()
    )
    if result.data and len(result.data) > 0:
        return int(result.data[0]["id"])

    season_row = (
        supabase.table("seasons")
        .select("id")
        .eq("year", year)
        .single()
        .execute()
    )
    return int(season_row.data["id"])


# ---------------------------------------------------------
# STATS NORMALIZATION (fallback-only — CFBD stats may not match On3 players)
# ---------------------------------------------------------

def _normalize_stats(raw: Optional[Dict]) -> Dict:
    """Fallback stat normalization if CFBD player-season match fails."""
    if not raw:
        return {
            "games_played": 0,
            "snaps": 0,
            "targets": 0,
            "receptions": 0,
            "yards": 0,
            "tds": 0,
            "tackles": 0,
            "pass_breakups": 0,
            "ints": 0,
        }

    def _first_present(keys: List[str]) -> int:
        for key in keys:
            if key in raw and raw.get(key) is not None:
                try:
                    return int(raw.get(key))
                except:
                    pass
        return 0

    yards_candidates = [
        "yards",
        "receivingYards",
        "rushingYards",
        "passingYards",
        "totalYards",
    ]
    td_candidates = [
        "tds",
        "touchdowns",
        "receivingTDs",
        "rushingTDs",
        "passingTDs",
        "totalTDs",
    ]

    return {
        "games_played": _first_present(["games", "gamesPlayed"]),
        "snaps": _first_present(["snaps", "plays", "offensePlays"]),
        "targets": _first_present(["targets", "receptions"]),
        "receptions": _first_present(["receptions", "catches"]),
        "yards": _first_present(yards_candidates),
        "tds": _first_present(td_candidates),
        "tackles": _first_present(["tackles", "soloTackles", "totalTackles"]),
        "pass_breakups": _first_present(["passBreakups", "passesDefended"]),
        "ints": _first_present(["ints", "interceptions"]),
    }


# ---------------------------------------------------------
# MAIN INGESTION USING ON3
# ---------------------------------------------------------

def ingest_transfers(year: int = 2024):
    supabase = get_supabase()

    # NEW: Pull directly from On3 portal wire
    transfers = get_on3_transfers()

    # CFBD still manages teams
    team_index = _upsert_teams()
    season_id = _ensure_season(year)

    processed = 0

    for t in transfers:
        full_name = t.get("player_name") or ""
        position = t.get("position")
        class_year = t.get("class_year")
        height = t.get("height")
        weight = t.get("weight")
        on3_team = t.get("on3_team")
        entered_date = t.get("entered_date")
        rating = t.get("rating")

        # Player payload for Supabase
        player_payload = {
            "full_name": full_name,
            "position": position,
            "height": height,
            "weight": weight,
            "class_year": class_year,
            "prev_school": None,   # On3 doesn't expose this directly on wire
            "cfbd_player_id": None,  # No CFBD ID for On3-only players
            "current_team_id": None,  # resolved below
            "rating": rating,
            "portal_status": t.get("status"),
            "entered_portal": entered_date,
        }

        # Attempt to resolve the team if name matches
        if on3_team:
            key = on3_team.strip().lower()
            if key in team_index:
                player_payload["current_team_id"] = team_index[key]["id"]

        # UPSERT PLAYER
        player_response = (
            supabase.table("players")
            .upsert(player_payload, on_conflict="full_name")
            .execute()
        )
        player_rows = player_response.data or []
        if not player_rows:
            player_rows = (
                supabase.table("players")
                .select("id")
                .eq("full_name", full_name)
                .limit(1)
                .execute()
                .data
                or []
            )
        if not player_rows:
            continue

        player_id = player_rows[0]["id"]

        # -------- STATS (OPTIONAL / FALLBACK ONLY) --------
        # We have no guaranteed team name for CFBD stats matching.
        stats_payload = _normalize_stats(None)

        season_stats_payload = {
            "player_id": player_id,
            "team_id": player_payload["current_team_id"],
            "season_id": season_id,
            "games_played": stats_payload["games_played"],
            "snaps": stats_payload["snaps"],
            "targets": stats_payload["targets"],
            "receptions": stats_payload["receptions"],
            "yards": stats_payload["yards"],
            "tds": stats_payload["tds"],
            "tackles": stats_payload["tackles"],
            "pass_breakups": stats_payload["pass_breakups"],
            "ints": stats_payload["ints"],
            "raw_source": {},
        }

        supabase.table("player_season_stats").upsert(
            season_stats_payload, on_conflict="player_id,season_id"
        ).execute()

        # -------- TVI COMPUTATION --------
        tvi_record = compute_tvi(stats_payload, player_payload)

        tvi_payload = {
            "player_id": player_id,
            "team_id": player_payload["current_team_id"],
            "season_id": season_id,
            "tvi": tvi_record["tvi"],
            "components": tvi_record["components"],
            "model_version": "v1",
        }

        supabase.table("tvi_scores").upsert(
            tvi_payload, on_conflict="player_id,season_id,model_version"
        ).execute()

        processed += 1

    return {"processed": processed}


if __name__ == "__main__":
    ingest_transfers()

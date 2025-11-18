"""Ingestion pipeline for transfer portal players using real CFBD data."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from cfbd_client import get_fbs_teams, get_player_season_stats, get_transfers
from supabase_client import get_supabase
from tvi_engine import compute_tvi


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
    index: Dict[str, Dict] = {}
    for row in team_rows:
        if row.get("school_id"):
            index[str(row["school_id"]).lower()] = row
        if row.get("name"):
            index[row["name"].strip().lower()] = row
    return index


def _upsert_teams() -> Dict[str, Dict]:
    supabase = get_supabase()
    teams = get_fbs_teams()
    payload = [_normalize_team_payload(team) for team in teams if team]
    response = supabase.table("teams").upsert(payload, on_conflict="school_id").execute()
    data = response.data or []
    return _build_team_index(data)


def _ensure_season(year: int) -> int:
    supabase = get_supabase()
    result = (
        supabase.table("seasons")
        .upsert({"year": year}, on_conflict="year")
        .execute()
    )
    if result.data and len(result.data) > 0:
        return int(result.data[0]["id"])
    season_row = supabase.table("seasons").select("id").eq("year", year).single().execute()
    return int(season_row.data["id"])


def _find_player_stats(
    stats: List[Dict], transfer: Dict, player_name: str
) -> Optional[Dict]:
    cfbd_player_id = transfer.get("id") or transfer.get("player_id")
    for stat in stats:
        if cfbd_player_id is not None and str(stat.get("id")) == str(cfbd_player_id):
            return stat
        if stat.get("player") and stat.get("player").strip().lower() == player_name:
            return stat
    return None


def _normalize_stats(raw: Optional[Dict]) -> Dict:
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
                except (TypeError, ValueError):
                    try:
                        return int(float(raw.get(key)))
                    except (TypeError, ValueError):
                        continue
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
        "snaps": _first_present(["snaps", "plays", "offensePlays", "offensivePlays"]),
        "targets": _first_present(["targets", "receptions"],),
        "receptions": _first_present(["receptions", "catches"]),
        "yards": _first_present(yards_candidates),
        "tds": _first_present(td_candidates),
        "tackles": _first_present(["tackles", "soloTackles", "totalTackles"]),
        "pass_breakups": _first_present(["passBreakups", "passesDefended"]),
        "ints": _first_present(["ints", "interceptions", "interceptionsThrown"]),
    }


def _resolve_team(team_index: Dict[str, Dict], team_name: Optional[str], school_id: Optional[int]) -> Optional[str]:
    if school_id is not None:
        key = str(school_id).lower()
        if key in team_index:
            return team_index[key].get("id")
    if team_name:
        key = team_name.strip().lower()
        if key in team_index:
            return team_index[key].get("id")
    return None


def ingest_transfers(year: int = 2024):
    supabase = get_supabase()

    transfers = get_transfers(year)
    team_index = _upsert_teams()
    season_id = _ensure_season(year)

    processed = 0
    for transfer in transfers:
        first = transfer.get("first_name") or transfer.get("firstName") or ""
        last = transfer.get("last_name") or transfer.get("lastName") or ""
        full_name = f"{first} {last}".strip()
        player_key_name = full_name.lower()

        destination_team = transfer.get("destination") or transfer.get("to_team") or transfer.get("toSchool")
        origin_team = transfer.get("origin") or transfer.get("from_team") or transfer.get("fromSchool")
        destination_school_id = transfer.get("to_id") or transfer.get("toSchoolId")
        origin_school_id = transfer.get("from_id") or transfer.get("fromSchoolId")

        dest_team_id = _resolve_team(team_index, destination_team, destination_school_id)
        origin_team_id = _resolve_team(team_index, origin_team, origin_school_id)

        player_payload = {
            "full_name": full_name,
            "position": transfer.get("position"),
            "height": transfer.get("height"),
            "weight": transfer.get("weight"),
            "class_year": transfer.get("classification") or transfer.get("class_year"),
            "hometown": transfer.get("hometown"),
            "prev_school": origin_team,
            "cfbd_player_id": transfer.get("id") or transfer.get("player_id"),
            "current_team_id": dest_team_id,
        }

        player_response = (
            supabase.table("players")
            .upsert(player_payload, on_conflict="cfbd_player_id")
            .execute()
        )
        player_rows = player_response.data or []
        if not player_rows:
            player_rows = (
                supabase.table("players")
                .select("id")
                .eq("cfbd_player_id", player_payload["cfbd_player_id"])
                .limit(1)
                .execute()
                .data
                or []
            )
        if not player_rows:
            continue
        player_id = player_rows[0]["id"]

        team_for_stats = origin_team or destination_team
        raw_stats_list = get_player_season_stats(year, team_for_stats) if team_for_stats else []
        raw_player_stats = _find_player_stats(raw_stats_list, transfer, player_key_name)
        stats_payload = _normalize_stats(raw_player_stats)

        season_stats_payload = {
            "player_id": player_id,
            "team_id": origin_team_id or dest_team_id,
            "season_id": season_id,
            "games_played": stats_payload.get("games_played"),
            "snaps": stats_payload.get("snaps"),
            "targets": stats_payload.get("targets"),
            "receptions": stats_payload.get("receptions"),
            "yards": stats_payload.get("yards"),
            "tds": stats_payload.get("tds"),
            "tackles": stats_payload.get("tackles"),
            "pass_breakups": stats_payload.get("pass_breakups"),
            "ints": stats_payload.get("ints"),
            "raw_source": raw_player_stats or {},
        }

        supabase.table("player_season_stats").upsert(
            season_stats_payload, on_conflict="player_id,season_id"
        ).execute()

        tvi_record = compute_tvi(stats_payload, player_payload)
        tvi_payload = {
            "player_id": player_id,
            "team_id": origin_team_id or dest_team_id,
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

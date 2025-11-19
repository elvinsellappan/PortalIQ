"""Ingestion pipeline for transfer portal players using On3 data and ESPN/other team metadata."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from on3_client import get_on3_transfers
from team_client import get_fbs_teams  # <- your ESPN-based team client
from supabase_client import get_supabase
from tvi_engine import compute_tvi


# ---------------------------------------------------------
# TEAM INGESTION (NO CFBD)
# ---------------------------------------------------------

def _normalize_team_payload(team: Dict) -> Dict:
    """
    Normalize whatever team_client returns into the schema expected
    by the Supabase `teams` table.

    Expected output keys:
      - name
      - short_name
      - conference
      - school_id   (can be ESPN/CFB id or any stable int)
      - logo_url    (optional)
    """
    return {
        "name": team.get("name"),
        "short_name": team.get("short_name"),
        "conference": team.get("conference"),
        "school_id": team.get("school_id"),
        "logo_url": team.get("logo_url"),
    }


def _build_team_index(team_rows: Iterable[Dict]) -> Dict[str, Dict]:
    """
    Build a lookup so we can go from team name/short_name to the row's `id`
    in the Supabase `teams` table.
    """
    index: Dict[str, Dict] = {}
    for row in team_rows:
        if row.get("name"):
            index[row["name"].strip().lower()] = row
        if row.get("short_name"):
            index[row["short_name"].strip().lower()] = row
    return index


def _upsert_teams() -> Dict[str, Dict]:
    """
    Pull teams from team_client (ESPN / static / whatever),
    upsert into Supabase, and return a lookup index.
    """
    supabase = get_supabase()
    teams = get_fbs_teams()  # <-- from team_client, NOT CFBD
    payload = [_normalize_team_payload(team) for team in teams if team]

    response = (
        supabase.table("teams")
        .upsert(payload, on_conflict="school_id")
        .execute()
    )

    data = response.data or []
    return _build_team_index(data)


# ---------------------------------------------------------
# SEASON HELPERS
# ---------------------------------------------------------

def _ensure_season(year: int) -> int:
    """
    Ensure a season row exists for `year` and return its id.
    """
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
# STATS NORMALIZATION (ZEROED FOR NOW)
# ---------------------------------------------------------

def _normalize_stats(raw: Optional[Dict]) -> Dict:
    """
    For now, we don't have a stats provider wired to On3 players,
    so we fall back to all zeros. This still lets TVI work if your
    model uses rating/metadata more than stats.

    Later: plug in an ESPN / NCAA stats provider and feed real `raw`.
    """
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


# ---------------------------------------------------------
# MAIN INGESTION USING ON3 + TEAM CLIENT
# ---------------------------------------------------------

def ingest_transfers(year: int = 2024) -> Dict[str, int]:
    """
    Main entrypoint.

    - Scrapes transfer players from On3 (via on3_client)
    - Upserts FBS teams (via team_client)
    - Ensures season exists
    - Upserts players
    - Inserts zeroed season stats row
    - Computes and upserts TVI scores
    """
    supabase = get_supabase()

    # 1) Get portal players from On3 wire
    transfers = get_on3_transfers()

    # 2) Ensure teams + season are present
    team_index = _upsert_teams()
    season_id = _ensure_season(year)

    processed = 0

    for t in transfers:
        # From on3_client.get_on3_transfers:
        # {
        #   "player_name": ...,
        #   "position": ...,
        #   "class_year": ...,
        #   "height": ...,
        #   "weight": ...,
        #   "high_school": ...,
        #   "rating": ...,
        #   "status": ...,
        #   "entered_date": ...,
        #   "on3_team": ...,
        #   "raw_lines": [...]
        # }
        full_name = (t.get("player_name") or "").strip()
        position = t.get("position")
        class_year = t.get("class_year")
        height = t.get("height")
        weight = t.get("weight")
        high_school = t.get("high_school")
        rating = t.get("rating")
        portal_status = t.get("status")
        entered_date = t.get("entered_date")
        on3_team = t.get("on3_team")

        # Resolve the team_id in Supabase if we can match the name
        current_team_id = None
        if on3_team:
            key = on3_team.strip().lower()
            if key in team_index:
                current_team_id = team_index[key].get("id")

        # ---------------- PLAYER UPSERT (DB PAYLOAD) ----------------
        # IMPORTANT: keep these keys consistent with your existing Supabase schema.
        # Original schema (from your previous code) used:
        #   full_name, position, height, weight, class_year,
        #   hometown, prev_school, cfbd_player_id, current_team_id
        db_player_payload = {
            "full_name": full_name,
            "position": position,
            "height": height,
            "weight": weight,
            "class_year": class_year,
            "hometown": high_school,          # best we can do from On3 wire
            "prev_school": None,              # On3 wire page doesn't expose clearly
            "cfbd_player_id": None,           # no CFBD IDs when using On3 only
            "current_team_id": current_team_id,
        }

        # This dict can have extra fields for the TVI model without touching the DB
        tvi_player_features = {
            **db_player_payload,
            "rating": rating,
            "portal_status": portal_status,
            "entered_portal": entered_date,
        }

        # Upsert player; keep on_conflict as cfbd_player_id to match your schema.
        # Because cfbd_player_id is always NULL now, upsert will behave like insert
        # each run. That's fine for MVP; later you can add a unique index on full_name.
        player_response = (
            supabase.table("players")
            .upsert(db_player_payload, on_conflict="cfbd_player_id")
            .execute()
        )

        player_rows = player_response.data or []
        if not player_rows:
            # Fallback: fetch by full_name
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
            # Could not create or find player row; skip
            continue

        player_id = player_rows[0]["id"]

        # ---------------- SEASON STATS (ZEROED, FOR NOW) ----------------
        stats_payload = _normalize_stats(None)

        season_stats_payload = {
            "player_id": player_id,
            "team_id": current_team_id,
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
            "raw_source": {},  # no real stats yet
        }

        supabase.table("player_season_stats").upsert(
            season_stats_payload,
            on_conflict="player_id,season_id",
        ).execute()

        # ---------------- TVI COMPUTATION ----------------
        tvi_record = compute_tvi(stats_payload, tvi_player_features)

        tvi_payload = {
            "player_id": player_id,
            "team_id": current_team_id,
            "season_id": season_id,
            "tvi": tvi_record["tvi"],
            "components": tvi_record["components"],
            "model_version": "v1",
        }

        supabase.table("tvi_scores").upsert(
            tvi_payload,
            on_conflict="player_id,season_id,model_version",
        ).execute()

        processed += 1

    return {"processed": processed}


if __name__ == "__main__":
    ingest_transfers()

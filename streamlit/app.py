import pandas as pd
import streamlit as st

from supabase_client import get_supabase


st.set_page_config(page_title="PortalIQ — Transfer Portal Analytics", layout="wide")


def _supabase_client():
    try:
        return get_supabase()
    except Exception as exc:  # pragma: no cover - Streamlit runtime guard
        st.error(f"Supabase configuration error: {exc}")
        st.stop()


def fetch_seasons(client):
    response = client.table("seasons").select("id, year").order("year", desc=True).execute()
    seasons = response.data or []
    return seasons


def fetch_conferences(client):
    response = client.table("teams").select("conference").execute()
    conferences = sorted({row.get("conference") for row in (response.data or []) if row.get("conference")})
    return conferences


def fetch_positions(client):
    response = client.table("players").select("position").execute()
    positions = sorted({row.get("position") for row in (response.data or []) if row.get("position")})
    return positions


def fetch_tvi_scores(client, season_id):
    response = (
        client.table("tvi_scores")
        .select("id, player_id, team_id, season_id, tvi, components")
        .eq("season_id", season_id)
        .execute()
    )
    return response.data or []


def fetch_players(client, player_ids):
    if not player_ids:
        return {}
    response = client.table("players").select("id, name, position").in_("id", list(player_ids)).execute()
    return {row["id"]: row for row in (response.data or [])}


def fetch_teams(client, team_ids):
    if not team_ids:
        return {}
    response = client.table("teams").select("id, name, conference").in_("id", list(team_ids)).execute()
    return {row["id"]: row for row in (response.data or [])}


def build_tvi_dataframe(client, season_id):
    tvi_rows = fetch_tvi_scores(client, season_id)
    player_ids = {row["player_id"] for row in tvi_rows if row.get("player_id")}
    team_ids = {row["team_id"] for row in tvi_rows if row.get("team_id")}

    players = fetch_players(client, player_ids)
    teams = fetch_teams(client, team_ids)

    records = []
    for row in tvi_rows:
        player = players.get(row.get("player_id"), {})
        team = teams.get(row.get("team_id"), {})
        records.append(
            {
                "player_id": row.get("player_id"),
                "team_id": row.get("team_id"),
                "Player": player.get("name", "Unknown"),
                "Position": player.get("position", "-"),
                "Team": team.get("name", "-"),
                "Conference": team.get("conference", "-"),
                "TVI": row.get("tvi"),
                "components": row.get("components"),
            }
        )

    df = pd.DataFrame(records)
    return df


def filter_dataframe(df, conferences, positions, minimum_tvi):
    filtered = df.copy()

    if conferences:
        filtered = filtered[filtered["Conference"].isin(conferences)]

    if positions:
        filtered = filtered[filtered["Position"].isin(positions)]

    if minimum_tvi is not None:
        filtered = filtered[filtered["TVI"] >= minimum_tvi]

    return filtered.sort_values(by="TVI", ascending=False)


def fetch_player_stats(client, player_id, season_id):
    response = (
        client.table("player_season_stats")
        .select("*")
        .eq("player_id", player_id)
        .eq("season_id", season_id)
        .limit(1)
        .execute()
    )
    data = response.data or []
    return data[0] if data else None


def display_player_details(client, player_row, season_id):
    if not player_row:
        return

    stats = fetch_player_stats(client, player_row["player_id"], season_id)

    st.subheader("Player Details")
    st.write(
        f"**{player_row['Player']}** — {player_row['Team']} | {player_row['Conference']} | {player_row['Position']}"
    )

    components = player_row.get("components") or {}
    tvi_value = player_row.get("TVI")
    st.metric("Transfer Value Index", f"{tvi_value:.2f}" if tvi_value is not None else "N/A")
    if components:
        st.write("**TVI Components**")
        if isinstance(components, dict):
            comp_df = pd.DataFrame([
                {"Component": key, "Value": value} for key, value in components.items()
            ])
            st.dataframe(comp_df)
        else:
            st.write(components)

    if not stats:
        st.info("No stats available for this player and season.")
        return

    key_metrics = [
        ("Games", stats.get("games")),
        ("Snaps", stats.get("snaps")),
        ("Yards", stats.get("yards")),
        ("Touchdowns", stats.get("touchdowns") or stats.get("tds")),
        ("Tackles", stats.get("tackles")),
        ("Interceptions", stats.get("interceptions") or stats.get("ints")),
    ]

    metric_columns = st.columns(3)
    for idx, (label, value) in enumerate(key_metrics):
        if value is not None:
            metric_columns[idx % 3].metric(label, value)

    remaining_fields = {
        k: v
        for k, v in stats.items()
        if k not in {"id", "player_id", "season_id"} and v not in {None, ""}
    }
    if remaining_fields:
        st.write("**Additional Stats**")
        st.write(remaining_fields)


def main():
    client = _supabase_client()

    st.title("PortalIQ — Transfer Portal Analytics")

    seasons = fetch_seasons(client)
    season_options = {str(s["year"]): s["id"] for s in seasons}
    if not season_options:
        st.error("No seasons available.")
        st.stop()

    with st.sidebar:
        st.header("Filters")
        selected_year = st.selectbox("Season", list(season_options.keys()))
        conferences = fetch_conferences(client)
        selected_conferences = st.multiselect("Conference", conferences, default=conferences)
        positions = fetch_positions(client)
        selected_positions = st.multiselect("Position", positions, default=positions)
        minimum_tvi = st.slider("TVI minimum", min_value=0, max_value=100, value=0)

    selected_season_id = season_options[selected_year]
    tvi_df = build_tvi_dataframe(client, selected_season_id)

    if tvi_df.empty:
        st.warning("No TVI scores available for the selected season.")
        return

    filtered_df = filter_dataframe(tvi_df, selected_conferences, selected_positions, minimum_tvi)

    st.subheader("Transfer Portal TVI")
    st.write(f"Players after filtering: **{len(filtered_df)}**")
    st.dataframe(filtered_df[["Player", "Position", "Team", "Conference", "TVI"]])

    player_options = filtered_df.to_dict(orient="records")
    option_labels = {idx: f"{row['Player']} — {row['Team']}" for idx, row in enumerate(player_options)}
    selection = st.selectbox(
        "Select player for detailed view",
        options=list(option_labels.keys()),
        format_func=lambda idx: option_labels[idx],
    ) if player_options else None

    if selection is not None and player_options:
        player_row = player_options[selection]
        display_player_details(client, player_row, selected_season_id)


if __name__ == "__main__":
    main()

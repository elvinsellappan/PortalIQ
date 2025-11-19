"""
team_client.py

Scrapes ESPN College Football teams:
    https://www.espn.com/college-football/teams

Returns a normalized list of FBS teams with:
- name
- short_name
- conference
- school_id  (ESPN numeric id)
- logo_url
"""

from __future__ import annotations

import re
from typing import Dict, List

import requests
from bs4 import BeautifulSoup


ESPN_TEAMS_URL = "https://www.espn.com/college-football/teams"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def _extract_espn_id(url: str) -> int | None:
    """
    ESPN team URLs look like:
      https://www.espn.com/college-football/team/_/id/333/alabama-crimson-tide

    We extract the numeric ID after '/id/'.
    """
    m = re.search(r"/id/(\d+)", url)
    if m:
        return int(m.group(1))
    return None


def get_fbs_teams() -> List[Dict]:
    """
    Scrape the ESPN teams page and extract FBS teams grouped by conference.

    Returns list of dicts:
      {
        "name": "Alabama",
        "short_name": "Crimson Tide",
        "conference": "SEC",
        "school_id": 333,
        "logo_url": "https://a.espncdn.com/i/teamlogos/ncaa/500/333.png"
      }
    """
    resp = requests.get(ESPN_TEAMS_URL, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to fetch ESPN teams: {resp.status_code}")

    soup = BeautifulSoup(resp.text, "html.parser")

    teams: List[Dict] = []

    # The ESPN page is structured with DIVs titled by conference,
    # followed by team cards inside UL lists.
    conference_blocks = soup.find_all("div", class_="ContentList__Item")

    for block in conference_blocks:
        # The conference name appears in an h2 or h3
        h2 = block.find("h2")
        h3 = block.find("h3")
        conference = None

        if h2:
            conference = h2.get_text(strip=True)
        elif h3:
            conference = h3.get_text(strip=True)
        else:
            continue

        # Now extract team cards inside this block
        team_links = block.find_all("a", href=re.compile(r"/college-football/team/"))

        for link in team_links:
            url = link.get("href", "")
            espn_id = _extract_espn_id(url)
            if not espn_id:
                continue

            # Team name and short name are split inside divs
            name_tag = link.find("span", class_="AnchorLink")
            name = name_tag.get_text(strip=True) if name_tag else None

            # ESPN renders the mascot "Crimson Tide" inside a second span
            spans = link.find_all("span")
            short_name = spans[-1].get_text(strip=True) if len(spans) > 1 else name

            # Logo
            logo_img = link.find("img")
            logo_url = logo_img.get("src") if logo_img else None

            if name:
                teams.append(
                    {
                        "name": name,
                        "short_name": short_name,
                        "conference": conference,
                        "school_id": espn_id,
                        "logo_url": logo_url,
                    }
                )

    return teams

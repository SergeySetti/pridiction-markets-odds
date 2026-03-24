"""Scraper configuration: league tiers, intervals, and budget controls."""

from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# League registry  (name -> API-Football league ID)
# ---------------------------------------------------------------------------

LEAGUE_IDS: dict[str, int] = {
    "EPL": 39,
    "LaLiga": 140,
    "Bundesliga": 78,
    "Serie A": 135,
    "Ligue 1": 61,
    "UCL": 2,
    "UEL": 3,
    "UECL": 848,
    "WCQ-UEFA": 32,
    "WCQ-CONMEBOL": 34,
    "Nations League": 5,
    "Euro": 4,
    "World Cup": 1,
    "Copa America": 9,
}

# ---------------------------------------------------------------------------
# Austerity tiers  (Strategy 5 knob)
# ---------------------------------------------------------------------------

TIERS: dict[int, list[str]] = {
    1: ["EPL", "UCL"],                              # always on
    2: ["LaLiga", "Bundesliga", "Serie A"],          # default on
    3: ["Ligue 1", "UEL", "UECL"],                  # optional
}

# Seasonal leagues activate automatically when they have fixtures
SEASONAL: list[str] = ["WCQ-UEFA", "WCQ-CONMEBOL", "Nations League", "Euro", "World Cup", "Copa America"]

# ---------------------------------------------------------------------------
# Schedule intervals (seconds)
# ---------------------------------------------------------------------------

INTERVAL_FULL_SWEEP = int(os.getenv("INTERVAL_FULL_SWEEP", 3 * 3600))      # 3 h
INTERVAL_CLOSING_24H = int(os.getenv("INTERVAL_CLOSING_24H", 3600))         # 1 h
INTERVAL_CLOSING_6H = int(os.getenv("INTERVAL_CLOSING_6H", 900))            # 15 min
INTERVAL_POLYMARKET_REFRESH = int(os.getenv("INTERVAL_POLYMARKET", 1800))    # 30 min
INTERVAL_FIXTURE_REFRESH = int(os.getenv("INTERVAL_FIXTURE_REFRESH", 1800))  # 30 min

# ---------------------------------------------------------------------------
# Budget
# ---------------------------------------------------------------------------

DAILY_REQUEST_LIMIT = int(os.getenv("DAILY_REQUEST_LIMIT", 7500))
BUDGET_ALERT_PCT = float(os.getenv("BUDGET_ALERT_PCT", 0.80))  # warn at 80%

# ---------------------------------------------------------------------------
# Active tier level  (1 = lean, 2 = default, 3 = full)
# ---------------------------------------------------------------------------

ACTIVE_TIER = int(os.getenv("ACTIVE_TIER", 3))

SEASON = int(os.getenv("SEASON", 2025))

MIN_REQUEST_INTERVAL = 2.0  # seconds between API calls (rate limit)


def active_leagues() -> list[str]:
    """Return league names enabled at the current tier level."""
    leagues: list[str] = []
    for tier, names in sorted(TIERS.items()):
        if tier <= ACTIVE_TIER:
            leagues.extend(names)
    return leagues

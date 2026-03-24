"""Self-contained API-Football client for the odds scraper.

Handles rate limiting, pagination, and fixture lookups.
Returns raw dicts (no dataclass overhead) — MongoDB-ready.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests

from .config import LEAGUE_IDS, MIN_REQUEST_INTERVAL, SEASON

log = logging.getLogger(__name__)

BASE_URL = "https://v3.football.api-sports.io"


class FootballAPI:
    """Thin client for api-football.com v3."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._last_call = 0.0
        self._daily_count = 0
        self._day_start: str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._session = requests.Session()
        self._session.headers.update({
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": "v3.football.api-sports.io",
            "Accept": "application/json",
        })

    # -- request accounting ---------------------------------------------------

    @property
    def requests_today(self) -> int:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self._day_start:
            self._day_start = today
            self._daily_count = 0
        return self._daily_count

    # -- low-level ------------------------------------------------------------

    def _request(self, endpoint: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        elapsed = time.time() - self._last_call
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)

        url = f"{BASE_URL}/{endpoint}"
        self._last_call = time.time()
        resp = self._session.get(url, params=params or {}, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        self._daily_count += 1

        errors = data.get("errors")
        if errors:
            if isinstance(errors, dict) and errors:
                raise RuntimeError(f"API-Football error: {errors}")
            elif isinstance(errors, list) and errors:
                raise RuntimeError(f"API-Football error: {errors}")

        return data

    # -- public API -----------------------------------------------------------

    def status(self) -> dict[str, Any]:
        """Account status and remaining requests (does NOT count toward quota)."""
        return self._request("status")

    def fixtures(
        self,
        league_id: int,
        season: int = SEASON,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {"league": str(league_id), "season": str(season)}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        data = self._request("fixtures", params)
        return data.get("response", [])

    def fixture_by_id(self, fixture_id: int) -> dict[str, Any] | None:
        data = self._request("fixtures", {"id": str(fixture_id)})
        resp = data.get("response", [])
        return resp[0] if resp else None

    def odds_page(
        self,
        league_id: int,
        season: int = SEASON,
        page: int = 1,
        fixture: int | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """Fetch one page of odds. Returns (response_items, total_pages)."""
        params: dict[str, str] = {
            "league": str(league_id),
            "season": str(season),
            "page": str(page),
        }
        if fixture is not None:
            params["fixture"] = str(fixture)
        data = self._request("odds", params)
        total_pages = data.get("paging", {}).get("total", 1)
        return data.get("response", []), total_pages

    def odds_all_pages(
        self,
        league_id: int,
        season: int = SEASON,
    ) -> list[dict[str, Any]]:
        """Paginate through all odds for a league+season."""
        all_items: list[dict[str, Any]] = []
        page = 1
        while True:
            items, total_pages = self.odds_page(league_id, season, page)
            all_items.extend(items)
            log.debug("League %d page %d/%d: %d fixtures", league_id, page, total_pages, len(items))
            if page >= total_pages:
                break
            page += 1
        return all_items

    def odds_for_fixture(self, fixture_id: int, league_id: int, season: int = SEASON) -> list[dict[str, Any]]:
        """Fetch odds for a single fixture."""
        items, _ = self.odds_page(league_id, season, fixture=fixture_id)
        return items

    # -- fixture parsing helpers ----------------------------------------------

    @staticmethod
    def parse_fixture(raw: dict[str, Any]) -> dict[str, Any]:
        """Extract flat fixture dict from raw API response item."""
        fix = raw.get("fixture", {})
        teams = raw.get("teams", {})
        league = raw.get("league", {})
        return {
            "fixture_id": fix.get("id"),
            "date": fix.get("date"),
            "timestamp": fix.get("timestamp"),
            "status_short": fix.get("status", {}).get("short"),
            "status_long": fix.get("status", {}).get("long"),
            "league": league.get("name"),
            "league_id": league.get("id"),
            "season": league.get("season"),
            "round": league.get("round"),
            "home_team": teams.get("home", {}).get("name"),
            "home_id": teams.get("home", {}).get("id"),
            "away_team": teams.get("away", {}).get("name"),
            "away_id": teams.get("away", {}).get("id"),
        }

    @staticmethod
    def parse_odds_item(raw: dict[str, Any]) -> dict[str, Any]:
        """Parse a single odds response item into a MongoDB-ready document."""
        fixture = raw.get("fixture", {})
        league = raw.get("league", {})
        bookmakers = []
        for bm in raw.get("bookmakers", []):
            bets = []
            for bet in bm.get("bets", []):
                bets.append({
                    "name": bet.get("name"),
                    "id": bet.get("id"),
                    "values": [
                        {"value": v.get("value"), "odd": v.get("odd")}
                        for v in bet.get("values", [])
                    ],
                })
            bookmakers.append({
                "id": bm.get("id"),
                "name": bm.get("name"),
                "bets": bets,
            })

        return {
            "fixture_id": fixture.get("id"),
            "league": league.get("name", ""),
            "league_id": league.get("id"),
            "season": league.get("season"),
            "update": raw.get("update"),
            "bookmakers": bookmakers,
        }

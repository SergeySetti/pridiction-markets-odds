"""Polymarket Gamma API client for discovering active football markets.

Strategy 3: only scrape fixtures that Polymarket actually lists.
The Gamma API is free and has no quota.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import requests

log = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"

# Tags / categories that indicate football markets on Polymarket
FOOTBALL_TAGS = {"Soccer", "Football", "FIFA", "UEFA", "Premier League", "La Liga",
                 "Champions League", "Europa League", "World Cup", "Serie A",
                 "Bundesliga", "Ligue 1", "Copa America", "EURO"}


class PolymarketDiscovery:
    """Discover active football markets on Polymarket via the Gamma API."""

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "prediction-markets-scraper/0.1"})

    def _get(self, endpoint: str, params: dict[str, str] | None = None) -> Any:
        url = f"{GAMMA_API}/{endpoint}"
        resp = self._session.get(url, params=params or {}, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def search_football_events(self, limit: int = 100) -> list[dict[str, Any]]:
        """Search for active football/soccer events."""
        events: list[dict[str, Any]] = []

        # Search with multiple keywords to maximize coverage
        for keyword in ["soccer", "football", "UEFA", "Premier League",
                        "Champions League", "La Liga", "Serie A", "Bundesliga"]:
            try:
                results = self._get("events", {
                    "tag": keyword,
                    "active": "true",
                    "closed": "false",
                    "limit": str(limit),
                })
                if isinstance(results, list):
                    events.extend(results)
            except Exception:
                pass

        # Deduplicate by event ID
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        for ev in events:
            eid = ev.get("id", "")
            if eid and eid not in seen:
                seen.add(eid)
                unique.append(ev)

        log.info("Polymarket: found %d unique football events", len(unique))
        return unique

    def extract_market_info(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract relevant info from a Polymarket event for fixture matching."""
        markets = event.get("markets", [])
        result: list[dict[str, Any]] = []
        for m in markets:
            result.append({
                "event_id": event.get("id"),
                "event_slug": event.get("slug", ""),
                "event_title": event.get("title", ""),
                "market_id": m.get("id", ""),
                "market_slug": m.get("slug", ""),
                "question": m.get("question", ""),
                "group_title": m.get("groupItemTitle", ""),
                "game_start_time": m.get("gameStartTime"),
                "end_date": m.get("endDate"),
                "active": m.get("active", False),
                "closed": m.get("closed", False),
                "volume": float(m.get("volumeNum", 0) or 0),
            })
        return result

    @staticmethod
    def extract_team_names(title: str) -> tuple[str, str] | None:
        """Try to extract home/away team names from event title.

        Handles patterns like:
          "Turkey vs Romania"
          "Manchester City vs. Arsenal"
          "Real Madrid - Barcelona"
        """
        for pattern in [
            r"(.+?)\s+(?:vs\.?|v\.?|-)\s+(.+)",
        ]:
            match = re.match(pattern, title.strip(), re.IGNORECASE)
            if match:
                return match.group(1).strip(), match.group(2).strip()
        return None

    def get_active_football_markets(self) -> list[dict[str, Any]]:
        """Main entry point: return flat list of active football market info."""
        events = self.search_football_events()
        all_markets: list[dict[str, Any]] = []
        for ev in events:
            all_markets.extend(self.extract_market_info(ev))
        log.info("Polymarket: %d total football markets across %d events", len(all_markets), len(events))
        return all_markets

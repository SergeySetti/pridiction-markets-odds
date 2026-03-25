"""MongoDB persistence layer for odds snapshots.

Collections:
  - odds_snapshots : one doc per fixture per scrape, full bookmaker+bet data
  - fixtures       : fixture metadata cache (upserted)
  - polymarket     : active Polymarket football markets (refreshed)
  - scrape_log     : per-run metadata (strategy, duration, request count)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

log = logging.getLogger(__name__)

DEFAULT_DB = "odds_scraper"


class Store:
    """MongoDB persistence for the odds scraper."""

    def __init__(self, mongo_uri: str, db_name: str = DEFAULT_DB) -> None:
        self._client: MongoClient = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db: Database = self._client[db_name]
        self._indexes_created = False

    def _maybe_ensure_indexes(self) -> None:
        """Create indexes on first successful DB operation."""
        if self._indexes_created:
            return
        try:
            self._ensure_indexes()
            self._indexes_created = True
        except Exception:
            log.warning("Could not create indexes (MongoDB may be starting up)")

    def _ensure_indexes(self) -> None:
        # odds_snapshots: unique per fixture+bookmaker+update (provider timestamp)
        self.db.odds_snapshots.create_index(
            [("fixture_id", 1), ("bookmaker_id", 1), ("update", 1)],
            unique=True,
            name="fixture_bookmaker_update",
        )
        self.db.odds_snapshots.create_index([("fixture_id", 1), ("snapshot_ts", -1)])
        self.db.odds_snapshots.create_index([("snapshot_ts", -1)])
        self.db.odds_snapshots.create_index([("league_id", 1), ("snapshot_ts", -1)])

        # fixtures: lookup by league, kickoff time
        self.db.fixtures.create_index([("league_id", 1), ("date", 1)])
        self.db.fixtures.create_index("fixture_id", unique=True)

        # polymarket: lookup by event slug
        self.db.polymarket.create_index("event_slug")
        self.db.polymarket.create_index("fixture_id")

    # -- odds snapshots -------------------------------------------------------

    def save_odds_batch(
        self,
        parsed_items: list[dict[str, Any]],
        strategy: str,
        snapshot_ts: datetime | None = None,
    ) -> int:
        """Save a batch of per-bookmaker odds documents. Returns upsert count.

        Each document is keyed by (fixture_id, bookmaker_id, update).
        Duplicates are skipped naturally by the unique index.
        """
        self._maybe_ensure_indexes()
        if not parsed_items:
            return 0

        ts = snapshot_ts or datetime.now(timezone.utc)
        ops = []
        for item in parsed_items:
            key = {
                "fixture_id": item["fixture_id"],
                "bookmaker_id": item["bookmaker_id"],
                "update": item["update"],
            }
            ops.append(UpdateOne(
                key,
                {
                    "$set": {**item, "snapshot_ts": ts, "strategy": strategy},
                    "$setOnInsert": {"created_at": ts},
                },
                upsert=True,
            ))

        result = self.db.odds_snapshots.bulk_write(ops, ordered=False)
        total = result.upserted_count
        skipped = result.matched_count
        if skipped:
            log.info("Skipped %d/%d rows (unchanged), inserted %d new",
                     skipped, len(ops), total)
        return total

    def last_snapshot_ts(self, fixture_id: int) -> datetime | None:
        """When was this fixture last scraped?"""
        doc = self.db.odds_snapshots.find_one(
            {"fixture_id": fixture_id},
            sort=[("snapshot_ts", -1)],
            projection={"snapshot_ts": 1},
        )
        return doc["snapshot_ts"] if doc else None

    # -- fixtures cache -------------------------------------------------------

    def upsert_fixtures(self, fixtures: list[dict[str, Any]]) -> int:
        """Bulk upsert fixture metadata. Returns modified count."""
        if not fixtures:
            return 0
        ops = [
            UpdateOne(
                {"fixture_id": f["fixture_id"]},
                {"$set": {**f, "updated_at": datetime.now(timezone.utc)}},
                upsert=True,
            )
            for f in fixtures
        ]
        result = self.db.fixtures.bulk_write(ops)
        return result.upserted_count + result.modified_count

    def get_upcoming_fixtures(
        self,
        league_ids: list[int] | None = None,
        hours_ahead: float = 48,
    ) -> list[dict[str, Any]]:
        """Return fixtures with kickoff within `hours_ahead` from now."""
        now = datetime.now(timezone.utc)
        cutoff = datetime.fromtimestamp(
            now.timestamp() + hours_ahead * 3600, tz=timezone.utc
        )
        query: dict[str, Any] = {
            "date": {"$gte": now.isoformat(), "$lte": cutoff.isoformat()},
            "status_short": {"$in": ["NS", "TBD", None]},  # not started
        }
        if league_ids:
            query["league_id"] = {"$in": league_ids}
        return list(self.db.fixtures.find(query))

    # -- polymarket -----------------------------------------------------------

    def save_polymarket_markets(self, markets: list[dict[str, Any]]) -> int:
        if not markets:
            return 0
        # Replace all — full refresh
        self.db.polymarket.delete_many({})
        for m in markets:
            m["updated_at"] = datetime.now(timezone.utc)
        result = self.db.polymarket.insert_many(markets)
        return len(result.inserted_ids)

    def get_polymarket_fixture_ids(self) -> set[int]:
        """Return fixture_ids that have been matched to Polymarket markets."""
        docs = self.db.polymarket.find(
            {"fixture_id": {"$exists": True, "$ne": None}},
            projection={"fixture_id": 1},
        )
        return {d["fixture_id"] for d in docs}

    def link_polymarket_to_fixture(self, event_slug: str, fixture_id: int) -> None:
        """Associate a Polymarket event with an API-Football fixture_id."""
        self.db.polymarket.update_many(
            {"event_slug": event_slug},
            {"$set": {"fixture_id": fixture_id}},
        )

    # -- scrape log -----------------------------------------------------------

    def log_scrape(
        self,
        strategy: str,
        leagues: list[str],
        fixtures_scraped: int,
        requests_used: int,
        duration_s: float,
        api_status: dict[str, Any] | None = None,
    ) -> None:
        doc: dict[str, Any] = {
            "ts": datetime.now(timezone.utc),
            "strategy": strategy,
            "leagues": leagues,
            "fixtures_scraped": fixtures_scraped,
            "requests_used": requests_used,
            "duration_s": round(duration_s, 2),
        }
        if api_status:
            doc["api_status"] = api_status
        self.db.scrape_log.insert_one(doc)

    def log_api_status(self, status_response: dict[str, Any]) -> None:
        """Log a standalone API status snapshot to scrape_log.

        Useful for tracking quota usage over time, even outside scrape jobs.
        """
        resp = status_response.get("response", {})
        req = resp.get("requests", {})
        sub = resp.get("subscription", {})
        self.db.scrape_log.insert_one({
            "ts": datetime.now(timezone.utc),
            "strategy": "api_status",
            "api_status": {
                "requests_current": req.get("current"),
                "requests_limit": req.get("limit_day"),
                "plan": sub.get("plan"),
                "active": sub.get("active"),
                "expires": sub.get("end"),
            },
            "leagues": [],
            "fixtures_scraped": 0,
            "requests_used": 0,
            "duration_s": 0,
        })

    # -- stats ----------------------------------------------------------------

    def snapshot_count(self) -> int:
        return self.db.odds_snapshots.estimated_document_count()

    def fixture_count(self) -> int:
        return self.db.fixtures.estimated_document_count()

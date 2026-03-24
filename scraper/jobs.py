"""Scrape jobs implementing Strategies 1 + 2 + 3.

Each job is a standalone function that receives the shared API client and store.
Jobs are idempotent — safe to re-run or overlap.

Strategy 1: full_sweep        — all active leagues, all pages, every 3h
Strategy 2: closing_line      — fixtures approaching kickoff, escalating frequency
Strategy 3: polymarket_target — only fixtures listed on Polymarket
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from .config import LEAGUE_IDS, SEASON, DAILY_REQUEST_LIMIT, BUDGET_ALERT_PCT, active_leagues
from .football import FootballAPI
from .polymarket import PolymarketDiscovery
from .store import Store

log = logging.getLogger(__name__)


def _check_budget(api: FootballAPI) -> bool:
    """Return True if we're within budget. Log warning if close."""
    used = api.requests_today
    limit = DAILY_REQUEST_LIMIT
    pct = used / limit if limit > 0 else 1.0
    if pct >= 1.0:
        log.error("BUDGET EXHAUSTED: %d/%d requests used. Skipping.", used, limit)
        return False
    if pct >= BUDGET_ALERT_PCT:
        log.warning("Budget alert: %d/%d (%.0f%%) requests used", used, limit, pct * 100)
    return True


def _fetch_api_status(api: FootballAPI) -> dict[str, Any] | None:
    """Fetch live API status and return a flat dict for embedding in log docs."""
    try:
        raw = api.status()
        resp = raw.get("response", {})
        req = resp.get("requests", {})
        sub = resp.get("subscription", {})
        return {
            "requests_current": req.get("current"),
            "requests_limit": req.get("limit_day"),
            "plan": sub.get("plan"),
            "active": sub.get("active"),
            "expires": sub.get("end"),
        }
    except Exception:
        log.warning("Could not fetch API status")
        return None


def _scrape_and_store(
    api: FootballAPI,
    store: Store,
    items: list[dict[str, Any]],
    strategy: str,
    snapshot_ts: datetime,
) -> int:
    """Parse raw odds items and save to MongoDB. Returns count saved."""
    parsed = [FootballAPI.parse_odds_item(item) for item in items]
    return store.save_odds_batch(parsed, strategy, snapshot_ts)


# ---------------------------------------------------------------------------
# Strategy 1: Full Sweep
# ---------------------------------------------------------------------------

def job_full_sweep(api: FootballAPI, store: Store) -> None:
    """Scrape all odds pages for all active leagues."""
    if not _check_budget(api):
        return

    leagues = active_leagues()
    snapshot_ts = datetime.now(timezone.utc)
    t0 = time.time()
    req_before = api.requests_today
    total_fixtures = 0

    log.info("FULL SWEEP: %d leagues %s", len(leagues), leagues)

    for league_name in leagues:
        league_id = LEAGUE_IDS.get(league_name)
        if league_id is None:
            continue

        if not _check_budget(api):
            break

        try:
            items = api.odds_all_pages(league_id, SEASON)
            saved = _scrape_and_store(api, store, items, "full_sweep", snapshot_ts)
            total_fixtures += saved
            log.info("  %s (id=%d): %d fixtures saved", league_name, league_id, saved)
        except Exception:
            log.exception("  %s: failed to scrape odds", league_name)

    duration = time.time() - t0
    req_used = api.requests_today - req_before
    status = _fetch_api_status(api)
    store.log_scrape("full_sweep", leagues, total_fixtures, req_used, duration, api_status=status)
    log.info("FULL SWEEP done: %d fixtures, %d requests, %.1fs", total_fixtures, req_used, duration)


# ---------------------------------------------------------------------------
# Strategy 2: Closing Line Hunter
# ---------------------------------------------------------------------------

def job_refresh_fixtures(api: FootballAPI, store: Store) -> None:
    """Refresh fixture cache for all active leagues (upcoming matches)."""
    if not _check_budget(api):
        return

    leagues = active_leagues()
    t0 = time.time()
    req_before = api.requests_today
    total = 0

    log.info("FIXTURE REFRESH: %d leagues", len(leagues))

    for league_name in leagues:
        league_id = LEAGUE_IDS.get(league_name)
        if league_id is None:
            continue

        if not _check_budget(api):
            break

        try:
            raw_fixtures = api.fixtures(league_id, SEASON)
            parsed = [FootballAPI.parse_fixture(f) for f in raw_fixtures]
            count = store.upsert_fixtures(parsed)
            total += count
            log.info("  %s: %d fixtures cached", league_name, count)
        except Exception:
            log.exception("  %s: failed to refresh fixtures", league_name)

    duration = time.time() - t0
    req_used = api.requests_today - req_before
    status = _fetch_api_status(api)
    store.log_scrape("fixture_refresh", leagues, total, req_used, duration, api_status=status)
    log.info("FIXTURE REFRESH done: %d fixtures, %d requests, %.1fs", total, req_used, duration)


def job_closing_line(api: FootballAPI, store: Store, hours_ahead: float = 24) -> None:
    """Scrape odds for fixtures approaching kickoff (Strategy 2).

    Called with hours_ahead=24 every hour, and hours_ahead=6 every 15 min.
    """
    if not _check_budget(api):
        return

    league_names = active_leagues()
    league_ids = [LEAGUE_IDS[n] for n in league_names if n in LEAGUE_IDS]
    upcoming = store.get_upcoming_fixtures(league_ids, hours_ahead)

    if not upcoming:
        log.info("CLOSING LINE (<%dh): no upcoming fixtures", int(hours_ahead))
        return

    snapshot_ts = datetime.now(timezone.utc)
    t0 = time.time()
    req_before = api.requests_today
    total_fixtures = 0
    strategy = f"closing_{int(hours_ahead)}h"

    log.info("CLOSING LINE (<%dh): %d fixtures to scrape", int(hours_ahead), len(upcoming))

    for fix in upcoming:
        if not _check_budget(api):
            break

        fixture_id = fix.get("fixture_id")
        league_id = fix.get("league_id")
        if not fixture_id or not league_id:
            continue

        try:
            items = api.odds_for_fixture(fixture_id, league_id, SEASON)
            saved = _scrape_and_store(api, store, items, strategy, snapshot_ts)
            total_fixtures += saved
            log.debug("  fixture %d: %d snapshots", fixture_id, saved)
        except Exception:
            log.exception("  fixture %d: failed", fixture_id)

    duration = time.time() - t0
    req_used = api.requests_today - req_before
    status = _fetch_api_status(api)
    store.log_scrape(strategy, league_names, total_fixtures, req_used, duration, api_status=status)
    log.info("CLOSING LINE (<%dh) done: %d fixtures, %d requests, %.1fs",
             int(hours_ahead), total_fixtures, req_used, duration)


# ---------------------------------------------------------------------------
# Strategy 3: Polymarket-Targeted
# ---------------------------------------------------------------------------

def job_polymarket_refresh(store: Store) -> None:
    """Refresh the list of active Polymarket football markets."""
    t0 = time.time()
    try:
        discovery = PolymarketDiscovery()
        markets = discovery.get_active_football_markets()
        count = store.save_polymarket_markets(markets)
        log.info("POLYMARKET REFRESH: %d markets saved (%.1fs)", count, time.time() - t0)
    except Exception:
        log.exception("POLYMARKET REFRESH failed")


def job_polymarket_target(api: FootballAPI, store: Store) -> None:
    """Scrape odds specifically for Polymarket-listed fixtures (Strategy 3).

    Only fires for fixtures that have been linked to a Polymarket event
    via fixture_id in the polymarket collection.
    """
    if not _check_budget(api):
        return

    fixture_ids = store.get_polymarket_fixture_ids()
    if not fixture_ids:
        log.info("POLYMARKET TARGET: no linked fixtures, skipping")
        return

    snapshot_ts = datetime.now(timezone.utc)
    t0 = time.time()
    req_before = api.requests_today
    total_fixtures = 0

    log.info("POLYMARKET TARGET: %d fixtures to scrape", len(fixture_ids))

    for fixture_id in fixture_ids:
        if not _check_budget(api):
            break

        # Look up league_id from fixtures cache
        fix_doc = store.db.fixtures.find_one({"fixture_id": fixture_id})
        if not fix_doc:
            log.warning("  fixture %d not in cache, skipping", fixture_id)
            continue

        league_id = fix_doc.get("league_id")
        if not league_id:
            continue

        try:
            items = api.odds_for_fixture(fixture_id, league_id, SEASON)
            saved = _scrape_and_store(api, store, items, "polymarket", snapshot_ts)
            total_fixtures += saved
        except Exception:
            log.exception("  fixture %d: failed", fixture_id)

    duration = time.time() - t0
    req_used = api.requests_today - req_before
    status = _fetch_api_status(api)
    store.log_scrape("polymarket", [], total_fixtures, req_used, duration, api_status=status)
    log.info("POLYMARKET TARGET done: %d fixtures, %d requests, %.1fs",
             total_fixtures, req_used, duration)

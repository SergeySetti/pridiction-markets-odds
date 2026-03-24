"""Odds scraper entry point.

Runs Strategies 1 + 2 + 3 on independent schedules using dbader/schedule.

Schedule:
  - Every 3h:   full_sweep        (all leagues, all pages)
  - Every 30m:  fixture_refresh   (update kickoff times cache)
  - Every 30m:  polymarket_refresh (discover Polymarket football markets)
  - Every 1h:   closing_line 24h  (fixtures within 24h of kickoff)
  - Every 15m:  closing_line 6h   (fixtures within 6h of kickoff)
  - Every 15m:  polymarket_target (Polymarket-listed fixtures only)

Usage:
  python -m scraper                          # run scheduler
  python -m scraper --once full_sweep        # run one job and exit
  python -m scraper --once closing_24h
  python -m scraper --status                 # print account + DB stats
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time

import schedule
from dotenv import load_dotenv

from .config import (
    INTERVAL_CLOSING_24H,
    INTERVAL_CLOSING_6H,
    INTERVAL_FIXTURE_REFRESH,
    INTERVAL_FULL_SWEEP,
    INTERVAL_POLYMARKET_REFRESH,
    active_leagues,
    ACTIVE_TIER,
)
from .football import FootballAPI
from .jobs import (
    job_closing_line,
    job_full_sweep,
    job_polymarket_refresh,
    job_polymarket_target,
    job_refresh_fixtures,
)
from .store import Store

log = logging.getLogger("scraper")

_shutdown_flag = False


def _setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _load_env() -> tuple[str, str]:
    """Load env vars and return (FOOTBALL_API_KEY, MONGODB_URI)."""
    here = os.path.dirname(os.path.abspath(__file__))
    for env_path in [
        os.path.join(here, "..", ".env"),
        os.path.join(here, "..", "..", ".env"),
    ]:
        if os.path.exists(env_path):
            load_dotenv(env_path)
            break

    api_key = os.getenv("FOOTBALL_API_KEY", "")
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")

    if not api_key:
        log.error("FOOTBALL_API_KEY not set")
        sys.exit(1)

    return api_key, mongo_uri


def cmd_status(api: FootballAPI, store: Store) -> None:
    """Print current account and database status."""
    try:
        status = api.status()
        req = status["response"]["requests"]
        sub = status["response"]["subscription"]
        print(f"Plan: {sub['plan']}  |  Requests: {req['current']}/{req['limit_day']}  |  Expires: {sub['end']}")
    except Exception as e:
        print(f"API status error: {e}")

    print(f"Active tier: {ACTIVE_TIER}  |  Leagues: {', '.join(active_leagues())}")
    try:
        print(f"MongoDB snapshots: {store.snapshot_count()}  |  Fixtures cached: {store.fixture_count()}")
    except Exception as e:
        print(f"MongoDB: {e}")


def cmd_once(api: FootballAPI, store: Store, job_name: str) -> None:
    """Run a single job and exit."""
    jobs = {
        "full_sweep": lambda: job_full_sweep(api, store),
        "fixture_refresh": lambda: job_refresh_fixtures(api, store),
        "closing_24h": lambda: job_closing_line(api, store, hours_ahead=24),
        "closing_6h": lambda: job_closing_line(api, store, hours_ahead=6),
        "polymarket_refresh": lambda: job_polymarket_refresh(store),
        "polymarket_target": lambda: job_polymarket_target(api, store),
    }
    if job_name not in jobs:
        print(f"Unknown job: {job_name}. Choose from: {', '.join(jobs)}")
        sys.exit(1)

    log.info("Running one-shot: %s", job_name)
    jobs[job_name]()
    log.info("Done.")


def _safe(fn, *args):
    """Wrap a job so exceptions don't kill the scheduler."""
    def wrapper():
        try:
            fn(*args)
        except Exception:
            log.exception("Job %s failed", fn.__name__)
    wrapper.__name__ = fn.__name__
    return wrapper


def cmd_scheduler(api: FootballAPI, store: Store) -> None:
    """Run the full scheduler loop."""
    global _shutdown_flag

    def _shutdown(signum, frame):
        global _shutdown_flag
        log.info("Shutting down...")
        _shutdown_flag = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Strategy 1: Full sweep every 3h
    schedule.every(INTERVAL_FULL_SWEEP).seconds.do(_safe(job_full_sweep, api, store))

    # Fixture cache refresh every 30m
    schedule.every(INTERVAL_FIXTURE_REFRESH).seconds.do(_safe(job_refresh_fixtures, api, store))

    # Strategy 2: Closing line — 24h window, every 1h
    schedule.every(INTERVAL_CLOSING_24H).seconds.do(_safe(job_closing_line, api, store, 24))

    # Strategy 2: Closing line — 6h window, every 15m
    schedule.every(INTERVAL_CLOSING_6H).seconds.do(_safe(job_closing_line, api, store, 6))

    # Strategy 3: Polymarket refresh every 30m
    schedule.every(INTERVAL_POLYMARKET_REFRESH).seconds.do(_safe(job_polymarket_refresh, store))

    # Strategy 3: Polymarket-targeted scrape every 15m
    schedule.every(INTERVAL_CLOSING_6H).seconds.do(_safe(job_polymarket_target, api, store))

    log.info("=" * 60)
    log.info("ODDS SCRAPER STARTED")
    log.info("  Tier: %d  |  Leagues: %s", ACTIVE_TIER, ", ".join(active_leagues()))
    log.info("  Full sweep:      every %ds", INTERVAL_FULL_SWEEP)
    log.info("  Closing <24h:    every %ds", INTERVAL_CLOSING_24H)
    log.info("  Closing <6h:     every %ds", INTERVAL_CLOSING_6H)
    log.info("  Fixture refresh: every %ds", INTERVAL_FIXTURE_REFRESH)
    log.info("  Polymarket:      every %ds", INTERVAL_POLYMARKET_REFRESH)
    log.info("=" * 60)

    # Run initial jobs immediately
    log.info("Running initial jobs...")
    _safe(job_refresh_fixtures, api, store)()
    _safe(job_polymarket_refresh, store)()
    _safe(job_full_sweep, api, store)()

    while not _shutdown_flag:
        schedule.run_pending()
        time.sleep(1)

    log.info("Scheduler stopped.")


def main() -> None:
    _setup_logging()

    parser = argparse.ArgumentParser(description="Football odds scraper")
    parser.add_argument("--once", type=str, help="Run a single job and exit")
    parser.add_argument("--status", action="store_true", help="Print status and exit")
    args = parser.parse_args()

    api_key, mongo_uri = _load_env()
    api = FootballAPI(api_key)
    store = Store(mongo_uri)

    if args.status:
        cmd_status(api, store)
    elif args.once:
        cmd_once(api, store, args.once)
    else:
        cmd_scheduler(api, store)


if __name__ == "__main__":
    main()

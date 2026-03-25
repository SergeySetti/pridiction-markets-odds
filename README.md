# Football Odds Scraper

Scheduled scraper that collects bookmaker odds from [API-Football](https://www.api-football.com/) and stores them in MongoDB. Designed to detect prediction market inefficiencies by comparing bookmaker consensus against Polymarket prices.

## Strategies

The scraper combines three strategies to maximize data value within the API quota (7,500 req/day on Pro):

| # | Strategy              | Interval | What it does                              | Cost/day     |
|---|-----------------------|----------|-------------------------------------------|--------------|
| 1 | **Full Sweep**        | 3h       | All active leagues, all pages             | ~208 req     |
| 2 | **Closing Line**      | 1h / 15m | Fixtures approaching kickoff (<24h / <6h) | ~200–600 req |
| 3 | **Polymarket Target** | 15m      | Only fixtures listed on Polymarket        | ~160 req     |

**Total: ~600–1,000 req/day (8–13% of budget)**

### Austerity Tiers

Control league coverage via `ACTIVE_TIER` env var:

| Tier        | Leagues                       | Pages/sweep |
|-------------|-------------------------------|-------------|
| 1 (lean)    | EPL, UCL                      | 4           |
| 2 (default) | + LaLiga, Bundesliga, Serie A | 13          |
| 3 (full)    | + Ligue 1, UEL, UECL          | 17          |

## Quick Start

### Docker (recommended)

```bash
cd scraper
cp .env.example .env
# Edit .env with your FOOTBALL_API_KEY

docker compose up -d
```

### Local

```bash
cd scraper
pip install -r requirements.txt
cp .env.example .env
# Edit .env

# Run the scheduler
python -m scraper

# One-shot jobs
python -m scraper --once full_sweep
python -m scraper --once fixture_refresh
python -m scraper --once closing_24h
python -m scraper --once polymarket_refresh

# Check status
python -m scraper --status
```

## MongoDB Schema

### `odds_snapshots`

One document per fixture per bookmaker per provider update. Unique on `(fixture_id, bookmaker_id, update)` — duplicate scrapes are skipped automatically. Stores only Match Winner odds as flat fields.

```json
{
  "fixture_id": 1379270,
  "league": "Premier League",
  "league_id": 39,
  "season": 2025,
  "update": "2026-03-20T18:02:20+00:00",
  "bookmaker_id": 1,
  "bookmaker": "Bet365",
  "home_odd": "3.30",
  "draw_odd": "3.80",
  "away_odd": "2.05",
  "snapshot_ts": "2026-03-24T12:00:00Z",
  "strategy": "full_sweep",
  "created_at": "2026-03-24T12:00:00Z"
}
```

### `fixtures`

Cached fixture metadata (kickoff times, teams, status). Refreshed every 30 min.

### `polymarket`

Active Polymarket football markets. Linked to `fixture_id` for Strategy 3 targeting.

### `scrape_log`

Per-run metadata: strategy, duration, request count, leagues scraped.

## Configuration

All config via environment variables (see `.env.example`). Key knobs:

| Var                   | Default                     | Description                                |
|-----------------------|-----------------------------|--------------------------------------------|
| `FOOTBALL_API_KEY`    | required                    | API-Football key                           |
| `MONGODB_URI`         | `mongodb://localhost:27017` | MongoDB connection string                  |
| `ACTIVE_TIER`         | `3`                         | League coverage: 1=lean, 2=mid, 3=full     |
| `SEASON`              | `2025`                      | Football season to scrape                  |
| `INTERVAL_FULL_SWEEP` | `10800`                     | Seconds between full sweeps (3h)           |
| `INTERVAL_CLOSING_6H` | `900`                       | Seconds between closing-line scrapes (15m) |
| `DAILY_REQUEST_LIMIT` | `7500`                      | Budget cap — stops scraping when hit       |
| `BUDGET_ALERT_PCT`    | `0.80`                      | Log warning at this % of daily budget      |
| `LOG_LEVEL`           | `INFO`                      | Python log level                           |

## Linking Polymarket Events

Strategy 3 requires linking Polymarket events to API-Football fixture IDs. This is done via the `polymarket` MongoDB collection. To link manually:

```python
from scraper.store import Store
store = Store("mongodb://localhost:27017")
store.link_polymarket_to_fixture("uef-tur-rom-2026-03-26", fixture_id=1234567)
```

The `polymarket_refresh` job discovers events automatically. Fixture matching is a manual step for now (automated fuzzy matching is planned).

## Architecture

```
scraper/
├── __main__.py      # APScheduler entry point, CLI
├── config.py        # Tiers, intervals, league IDs
├── football.py      # API-Football client (rate-limited, paginated)
├── polymarket.py    # Gamma API client (free, no quota)
├── store.py         # MongoDB collections + indexes
└── jobs.py          # Strategy 1/2/3 job functions
```

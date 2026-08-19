# Lucky Lincoln Contact Center Analytics

FastAPI + PostgreSQL dashboard for Webex Contact Center reporting.

## Features
- Pulls Webex `taskDetails`
- Pulls `agentSession` telephony activity state history
- Stores calls and agent activities in PostgreSQL
- Classifies answered, in-progress, unserved/no-agent, and unserved/agent-available calls
- Browser dashboard at `/`
- Filters for date range, queue, agent, and outcome
- Manual **Sync Webex** button
- Historical import/backfill from a selected date range
- Outer Webex pagination for `taskDetails` and `agentSession`
- Adaptive historical windows when nested CAR/AAR activity results exceed 100 records
- Duplicate-safe database upserts, so historical ranges can be rerun
- Swagger docs at `/docs`

## Environment variables
```text
WEBEX_SEARCH_URL=<your working Webex Contact Center /search URL>
WEBEX_ACCESS_TOKEN=<service app access token>
DATABASE_URL=postgresql+psycopg://...
SYNC_LOOKBACK_MINUTES=1440
PYTHON_VERSION=3.13.7
```

The app automatically converts a Render-provided `postgresql://...` URL to `postgresql+psycopg://...` so SQLAlchemy uses psycopg v3.

## Build command
```bash
pip install -r requirements.txt
```

## Start command
```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

## Historical import
Open the dashboard and click **Historical Import**. Select a start date (for example `2024-01-01`) and an end date, then click **Import History**.

The browser sends one safe import window at a time so a multi-year import does not depend on one long-running Render request. The server initially accepts windows up to 30 days. If Webex indicates that a nested activity list exceeded its 100-record inner page, the browser automatically splits that window and retries the smaller ranges.

Keep the dashboard tab open while the import runs. Imported calls and agent activities are upserted by their unique IDs, so rerunning the same range is safe.

## Endpoints
- `GET /` dashboard
- `GET /health`
- `POST /api/sync`
- `POST /api/backfill/window?from_ms=...&to_ms=...`
- `GET /api/dashboard/summary`
- `GET /api/dashboard/calls`
- `GET /api/dashboard/filters`
- `GET /docs`

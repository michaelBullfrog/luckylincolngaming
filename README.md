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
- Swagger docs at `/docs`

## Environment variables
```text
WEBEX_SEARCH_URL=<your working Webex Contact Center /search URL>
WEBEX_ACCESS_TOKEN=<service app access token>
DATABASE_URL=postgresql+psycopg://...
SYNC_LOOKBACK_MINUTES=1440
PYTHON_VERSION=3.13.7
```

If Render supplies a `postgresql://...` database URL, change the scheme to `postgresql+psycopg://...` so SQLAlchemy uses psycopg v3.

## Build command
```bash
pip install -r requirements.txt
```

## Start command
```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

## Endpoints
- `GET /` dashboard
- `GET /health`
- `POST /api/sync`
- `GET /api/dashboard/summary`
- `GET /api/dashboard/calls`
- `GET /api/dashboard/filters`
- `GET /docs`

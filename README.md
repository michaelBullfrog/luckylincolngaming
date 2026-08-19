# Lucky Lincoln Contact Center Analytics

FastAPI + PostgreSQL starter for a Lucky Lincoln Gaming Webex Contact Center analytics dashboard.

## What v0.1 does

- Pulls Webex Contact Center `taskDetails` data.
- Pulls `agentSession` telephony activity history.
- Stores call and agent-state data.
- Classifies calls as:
  - `ANSWERED`
  - `IN_PROGRESS`
  - `UNSERVED_AGENTS_AVAILABLE`
  - `UNSERVED_NO_AGENTS_AVAILABLE`
- Exposes dashboard JSON endpoints.

## Important v0.1 limitation

The exact task queue-entry timestamp has not yet been added to the tested task query. Availability correlation currently compares agent `available` intervals against the whole task interval. The next refinement is to add task activity timestamps and compare against the precise queue interval.

## Local setup

```bash
python -m venv .venv
# Windows
.venv\\Scripts\\activate
# macOS/Linux
# source .venv/bin/activate

pip install -r requirements.txt
copy .env.example .env
# Edit .env with your working Webex /search URL and access token.
uvicorn app.main:app --reload
```

Open:

- `http://127.0.0.1:8000/docs`
- `http://127.0.0.1:8000/api/dashboard/summary`
- `http://127.0.0.1:8000/api/dashboard/calls`

## First sync

In Swagger (`/docs`) run:

`POST /api/sync`

With no query parameters it pulls the previous 24 hours.

Or with curl:

```bash
curl -X POST "http://127.0.0.1:8000/api/sync"
```

## Render deployment

This repository includes `render.yaml` for a web service plus PostgreSQL.

Set these secrets in Render:

- `WEBEX_SEARCH_URL` = the exact `/search` URL that already works in Postman.
- `WEBEX_ACCESS_TOKEN` = current Service App access token.

Do not commit the access token.

## Next build steps

1. Add exact queue interval timestamps from task activity records.
2. Add scheduled synchronization.
3. Add Service App token refresh.
4. Add SMS-provider integration/status.
5. Build the Lucky Lincoln browser dashboard UI.

# Lucky Lincoln Gaming — Contact Center Analytics v0.5

FastAPI + PostgreSQL dashboard for Webex Contact Center with Clerk Chat SMS matching.

## What v0.5 adds

- Automatic Webex sync in the backend (default every 120 seconds)
- Recent Webex lookback on every auto-sync (default 180 minutes)
- Clerk Chat `message.sent` webhook fallback: if the SMS cannot match locally, the webhook immediately syncs the previous 90 minutes from Webex and retries
- Persistent retry of unmatched SMS events after every Webex sync for 24 hours
- SMS match window defaults to 60 minutes after a call ends
- Dashboard data auto-refreshes every 30 seconds (browser refresh only; backend sync is independent)
- `/api/sync/status` for basic auto-sync/matching health

## Render commands

Build:

```bash
pip install -r requirements.txt
```

Start:

```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

## Environment variables

Required:

```text
DATABASE_URL=...
WEBEX_SEARCH_URL=...
WEBEX_ACCESS_TOKEN=...
```

Recommended:

```text
AUTO_SYNC_ENABLED=true
AUTO_SYNC_SECONDS=120
AUTO_SYNC_LOOKBACK_MINUTES=180
SYNC_LOOKBACK_MINUTES=1440
CLERKCHAT_FALLBACK_SYNC_MINUTES=90
SMS_MATCH_WINDOW_MINUTES=60
SMS_RETRY_MAX_AGE_HOURS=24
CLERKCHAT_WEBHOOK_TOKEN=<random secret>
```

Render may provide `postgresql://...`; the app automatically switches it to SQLAlchemy's `postgresql+psycopg://...` form.

## Clerk Chat webhook

Subscribe to `message.sent` and use:

```text
https://YOUR-RENDER-SERVICE.onrender.com/api/webhooks/clerkchat?token=YOUR_SECRET
```

A webhook is stored first. If no Webex call is in PostgreSQL yet, the app immediately performs a recent Webex sync and retries. Any still-unmatched SMS remains in `clerk_sms_events` and is retried on subsequent automatic syncs.

Debug recent events:

```text
GET /api/webhooks/clerkchat/recent
```

Auto-sync status:

```text
GET /api/sync/status
```

Manual sync remains available:

```text
POST /api/sync
```

Historical import remains available through the dashboard and `POST /api/backfill/window`.

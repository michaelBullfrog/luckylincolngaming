import os
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from dotenv import load_dotenv

load_dotenv()

from .db import Base, engine, get_db
from .models import Call, ClerkSmsEvent
from .services import record_clerkchat_outbound, upsert_agent_activities, upsert_calls
from .webex import WebexError, activity_truncation_counts, fetch_agent_sessions, fetch_tasks

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Lucky Lincoln Contact Center Analytics", version="0.4.0")
STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _iso(ms: int) -> str | None:
    if ms is None or ms < 0:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _parse_date_ms(value: str | None, *, end_of_day: bool = False) -> int | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if end_of_day and len(value) <= 10:
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
        return int(dt.timestamp() * 1000)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date value: {value}") from exc


def _filtered_calls_stmt(
    *,
    outcome: str | None = None,
    queue: str | None = None,
    agent: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
):
    stmt = select(Call)
    if outcome:
        stmt = stmt.where(Call.outcome == outcome)
    if queue:
        stmt = stmt.where(Call.queue_name == queue)
    if agent:
        stmt = stmt.where(Call.agent_name == agent)
    from_ms = _parse_date_ms(from_date)
    to_ms = _parse_date_ms(to_date, end_of_day=True)
    if from_ms is not None:
        stmt = stmt.where(Call.created_ms >= from_ms)
    if to_ms is not None:
        stmt = stmt.where(Call.created_ms <= to_ms)
    return stmt


@app.get("/", include_in_schema=False)
def root():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health():
    return {"name": "Lucky Lincoln Contact Center Analytics", "status": "ok", "docs": "/docs"}


@app.post("/api/sync")
async def sync(
    from_ms: int | None = Query(default=None),
    to_ms: int | None = Query(default=None),
    db: Session = Depends(get_db),
):
    to_ms = to_ms or _now_ms()
    lookback_minutes = int(os.getenv("SYNC_LOOKBACK_MINUTES", "1440"))
    from_ms = from_ms or (to_ms - lookback_minutes * 60_000)

    try:
        sessions = await fetch_agent_sessions(from_ms, to_ms)
        tasks = await fetch_tasks(from_ms, to_ms)
        truncation = activity_truncation_counts(tasks, sessions)
        activity_rows = upsert_agent_activities(db, sessions)
        reliable = truncation["agent_activity_channels_truncated"] == 0
        call_rows = upsert_calls(db, tasks, availability_reliable=reliable)
    except WebexError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {
        "from_ms": from_ms,
        "to_ms": to_ms,
        "agent_sessions": len(sessions),
        "agent_activity_rows_processed": activity_rows,
        "tasks": len(tasks),
        "call_rows_processed": call_rows,
        **truncation,
    }


@app.post("/api/backfill/window")
async def backfill_window(
    from_ms: int = Query(...),
    to_ms: int = Query(...),
    db: Session = Depends(get_db),
):
    if to_ms <= from_ms:
        raise HTTPException(status_code=400, detail="to_ms must be greater than from_ms")
    max_span_ms = 30 * 24 * 60 * 60 * 1000
    if to_ms - from_ms > max_span_ms:
        raise HTTPException(status_code=400, detail="Backfill windows cannot exceed 30 days")
    if from_ms < _now_ms() - (36 * 31 * 24 * 60 * 60 * 1000):
        raise HTTPException(status_code=400, detail="Requested history may be outside Webex's 36-month Search API limit")

    try:
        sessions = await fetch_agent_sessions(from_ms, to_ms)
        tasks = await fetch_tasks(from_ms, to_ms)
    except WebexError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    truncation = activity_truncation_counts(tasks, sessions)
    span_ms = to_ms - from_ms
    truncated = any(truncation.values())

    # If Webex's inner 100-record activity page was not enough, ask the browser
    # to split this time window and retry. This keeps the historical import
    # accurate without one giant long-running Render request.
    if truncated and span_ms > 60 * 60 * 1000:
        midpoint = from_ms + span_ms // 2
        return {
            "status": "split_required",
            "from_ms": from_ms,
            "to_ms": to_ms,
            "split_at_ms": midpoint,
            "tasks_seen": len(tasks),
            "agent_sessions_seen": len(sessions),
            **truncation,
        }

    activity_rows = upsert_agent_activities(db, sessions)
    availability_reliable = truncation["agent_activity_channels_truncated"] == 0
    call_rows = upsert_calls(db, tasks, availability_reliable=availability_reliable)

    return {
        "status": "processed",
        "from_ms": from_ms,
        "to_ms": to_ms,
        "tasks": len(tasks),
        "call_rows_processed": call_rows,
        "agent_sessions": len(sessions),
        "agent_activity_rows_processed": activity_rows,
        "availability_reliable": availability_reliable,
        **truncation,
    }


@app.post("/api/webhooks/clerkchat")
async def clerkchat_webhook(
    request: Request,
    token: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    expected = os.getenv("CLERKCHAT_WEBHOOK_TOKEN")
    if expected and token != expected:
        raise HTTPException(status_code=401, detail="Invalid Clerk Chat webhook token")
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Webhook body must be JSON") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Webhook body must be a JSON object")
    result = record_clerkchat_outbound(db, payload, _now_ms())
    return {"ok": True, **result}


@app.get("/api/webhooks/clerkchat/recent")
def clerkchat_recent(limit: int = Query(default=20, ge=1, le=100), db: Session = Depends(get_db)):
    rows = db.scalars(select(ClerkSmsEvent).order_by(ClerkSmsEvent.id.desc()).limit(limit)).all()
    return [
        {
            "message_id": r.message_id,
            "to_number": r.to_number,
            "from_number": r.from_number,
            "direction": r.direction,
            "sent_at": _iso(r.sent_ms) if r.sent_ms else None,
            "matched_task_id": r.matched_task_id,
        }
        for r in rows
    ]


@app.get("/api/dashboard/summary")
def dashboard_summary(
    queue: str | None = Query(default=None),
    agent: str | None = Query(default=None),
    from_date: str | None = Query(default=None),
    to_date: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    base = _filtered_calls_stmt(queue=queue, agent=agent, from_date=from_date, to_date=to_date).subquery()

    total = db.scalar(select(func.count()).select_from(base)) or 0
    answered = db.scalar(select(func.count()).select_from(base).where(base.c.outcome == "ANSWERED")) or 0
    unserved_available = db.scalar(
        select(func.count()).select_from(base).where(base.c.outcome == "UNSERVED_AGENTS_AVAILABLE")
    ) or 0
    unserved_no_agents = db.scalar(
        select(func.count()).select_from(base).where(base.c.outcome == "UNSERVED_NO_AGENTS_AVAILABLE")
    ) or 0
    availability_unknown = db.scalar(
        select(func.count()).select_from(base).where(base.c.outcome == "UNSERVED_AVAILABILITY_UNKNOWN")
    ) or 0
    in_progress = db.scalar(select(func.count()).select_from(base).where(base.c.outcome == "IN_PROGRESS")) or 0
    avg_wait = db.scalar(select(func.avg(base.c.wait_ms)).where(base.c.wait_ms > 0)) or 0
    longest_wait = db.scalar(select(func.max(base.c.wait_ms))) or 0

    return {
        "total_calls": total,
        "answered": answered,
        "unserved": unserved_available + unserved_no_agents + availability_unknown,
        "unserved_agents_available": unserved_available,
        "unserved_no_agents_available": unserved_no_agents,
        "unserved_availability_unknown": availability_unknown,
        "in_progress": in_progress,
        "avg_wait_seconds": round(float(avg_wait) / 1000, 1),
        "longest_wait_seconds": round(float(longest_wait) / 1000, 1),
    }


@app.get("/api/dashboard/calls")
def dashboard_calls(
    limit: int = Query(default=100, ge=1, le=1000),
    outcome: str | None = Query(default=None),
    queue: str | None = Query(default=None),
    agent: str | None = Query(default=None),
    from_date: str | None = Query(default=None),
    to_date: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    stmt = _filtered_calls_stmt(
        outcome=outcome,
        queue=queue,
        agent=agent,
        from_date=from_date,
        to_date=to_date,
    ).order_by(Call.created_ms.desc()).limit(limit)
    calls = db.scalars(stmt).all()
    return [
        {
            "task_id": c.task_id,
            "caller_number": c.caller_number,
            "destination": c.destination,
            "queue": c.queue_name,
            "created_at": _iso(c.created_ms),
            "ended_at": _iso(c.ended_ms),
            "wait_seconds": round(c.wait_ms / 1000, 1),
            "answered": c.answered,
            "answered_by": c.agent_name,
            "outcome": c.outcome,
            "available_agent_count": c.available_agent_count,
            "available_agent_names": c.available_agent_names,
            "sms_sent": c.sms_sent,
            "sms_sent_at": _iso(c.sms_sent_ms) if c.sms_sent_ms else None,
        }
        for c in calls
    ]


@app.get("/api/dashboard/filters")
def dashboard_filters(db: Session = Depends(get_db)):
    queues = [x for x in db.scalars(select(Call.queue_name).where(Call.queue_name.is_not(None)).distinct().order_by(Call.queue_name)).all() if x]
    agents = [x for x in db.scalars(select(Call.agent_name).where(Call.agent_name.is_not(None)).distinct().order_by(Call.agent_name)).all() if x]
    outcomes = [x for x in db.scalars(select(Call.outcome).distinct().order_by(Call.outcome)).all() if x]
    return {"queues": queues, "agents": agents, "outcomes": outcomes}

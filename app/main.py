import os
import time
from datetime import datetime, timezone
from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from dotenv import load_dotenv

load_dotenv()

from .db import Base, engine, get_db
from .models import Call
from .services import upsert_agent_activities, upsert_calls
from .webex import WebexError, fetch_agent_sessions, fetch_tasks

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Lucky Lincoln Contact Center Analytics", version="0.1.0")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _iso(ms: int) -> str | None:
    if ms is None or ms < 0:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


@app.get("/")
def root():
    return {
        "name": "Lucky Lincoln Contact Center Analytics",
        "status": "ok",
        "docs": "/docs",
    }


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
        # Store states first, then classify calls against those states.
        sessions = await fetch_agent_sessions(from_ms, to_ms)
        activity_rows = upsert_agent_activities(db, sessions)
        tasks = await fetch_tasks(from_ms, to_ms)
        call_rows = upsert_calls(db, tasks)
    except WebexError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {
        "from_ms": from_ms,
        "to_ms": to_ms,
        "agent_sessions": len(sessions),
        "agent_activity_rows_processed": activity_rows,
        "tasks": len(tasks),
        "call_rows_processed": call_rows,
    }


@app.get("/api/dashboard/summary")
def dashboard_summary(db: Session = Depends(get_db)):
    total = db.scalar(select(func.count()).select_from(Call)) or 0
    answered = db.scalar(select(func.count()).select_from(Call).where(Call.outcome == "ANSWERED")) or 0
    unserved_available = db.scalar(
        select(func.count()).select_from(Call).where(Call.outcome == "UNSERVED_AGENTS_AVAILABLE")
    ) or 0
    unserved_no_agents = db.scalar(
        select(func.count()).select_from(Call).where(Call.outcome == "UNSERVED_NO_AGENTS_AVAILABLE")
    ) or 0
    in_progress = db.scalar(select(func.count()).select_from(Call).where(Call.outcome == "IN_PROGRESS")) or 0
    avg_wait = db.scalar(select(func.avg(Call.wait_ms)).where(Call.wait_ms > 0)) or 0

    return {
        "total_calls": total,
        "answered": answered,
        "unserved": unserved_available + unserved_no_agents,
        "unserved_agents_available": unserved_available,
        "unserved_no_agents_available": unserved_no_agents,
        "in_progress": in_progress,
        "avg_wait_seconds": round(float(avg_wait) / 1000, 1),
    }


@app.get("/api/dashboard/calls")
def dashboard_calls(
    limit: int = Query(default=100, ge=1, le=1000),
    outcome: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    stmt = select(Call).order_by(Call.created_ms.desc()).limit(limit)
    if outcome:
        stmt = stmt.where(Call.outcome == outcome)
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

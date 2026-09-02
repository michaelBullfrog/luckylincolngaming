import os
import time
import asyncio
import logging
import json
import hashlib
import uuid
import re
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from dotenv import load_dotenv

load_dotenv()

from .db import Base, engine, get_db, SessionLocal
from .models import Call, ClerkSmsEvent, CallJourney, CallJourneyEvent
from .services import record_clerkchat_outbound, retry_unmatched_sms, upsert_agent_activities, upsert_calls, reclassify_calls
from .webex import WebexError, activity_truncation_counts, fetch_agent_sessions, fetch_tasks

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Lucky Lincoln Contact Center Analytics", version="1.0.5")
INDEX_FILE = Path(__file__).resolve().parent / "index.html"
LOGO_FILE = Path(__file__).resolve().parent / "llg-logo.svg"

logger = logging.getLogger("lucky_lincoln.sync")
_sync_lock = asyncio.Lock()
_auto_sync_task: asyncio.Task | None = None



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
    caller_query: str | None = None,
    sms: str | None = None,
):
    stmt = select(Call)
    if outcome:
        stmt = stmt.where(Call.outcome == outcome)
    if queue:
        stmt = stmt.where(Call.queue_name == queue)
    if agent:
        stmt = stmt.where(Call.agent_name == agent)
    if caller_query:
        stmt = stmt.where(Call.caller_number.ilike(f"%{caller_query.strip()}%"))
    if sms == "yes":
        stmt = stmt.where(Call.sms_sent.is_(True))
    elif sms == "no":
        stmt = stmt.where((Call.sms_sent.is_(False)) | (Call.sms_sent.is_(None)))
    from_ms = _parse_date_ms(from_date)
    to_ms = _parse_date_ms(to_date, end_of_day=True)
    if from_ms is not None:
        stmt = stmt.where(Call.created_ms >= from_ms)
    if to_ms is not None:
        stmt = stmt.where(Call.created_ms <= to_ms)
    return stmt


async def _sync_window(db: Session, from_ms: int, to_ms: int) -> dict:
    sessions = await fetch_agent_sessions(from_ms, to_ms)
    tasks = await fetch_tasks(from_ms, to_ms)
    truncation = activity_truncation_counts(tasks, sessions)
    activity_rows = upsert_agent_activities(db, sessions)
    reliable = truncation["agent_activity_channels_truncated"] == 0
    call_rows = upsert_calls(db, tasks, availability_reliable=reliable, raw_agent_sessions=sessions)
    retry = retry_unmatched_sms(
        db,
        max_age_hours=int(os.getenv("SMS_RETRY_MAX_AGE_HOURS", "24")),
        now_ms=to_ms,
    )
    return {
        "from_ms": from_ms,
        "to_ms": to_ms,
        "agent_sessions": len(sessions),
        "agent_activity_rows_processed": activity_rows,
        "tasks": len(tasks),
        "call_rows_processed": call_rows,
        **retry,
        **truncation,
    }


async def _auto_sync_loop():
    interval = max(60, int(os.getenv("AUTO_SYNC_SECONDS", "120")))
    lookback_minutes = int(os.getenv("AUTO_SYNC_LOOKBACK_MINUTES", "180"))
    # Let the web process finish startup before the first API call.
    await asyncio.sleep(5)
    while True:
        try:
            to_ms = _now_ms()
            from_ms = to_ms - lookback_minutes * 60_000
            async with _sync_lock:
                with SessionLocal() as db:
                    result = await _sync_window(db, from_ms, to_ms)
            logger.info("Automatic Webex sync complete: %s", result)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Automatic Webex sync failed")
        await asyncio.sleep(interval)


@app.on_event("startup")
async def start_auto_sync():
    global _auto_sync_task
    if os.getenv("AUTO_SYNC_ENABLED", "true").lower() not in {"0", "false", "no", "off"}:
        _auto_sync_task = asyncio.create_task(_auto_sync_loop())


@app.on_event("shutdown")
async def stop_auto_sync():
    global _auto_sync_task
    if _auto_sync_task:
        _auto_sync_task.cancel()
        try:
            await _auto_sync_task
        except asyncio.CancelledError:
            pass
        _auto_sync_task = None


@app.get("/", include_in_schema=False)
def root():
    return FileResponse(INDEX_FILE)


@app.get("/logo.svg", include_in_schema=False)
def logo():
    return FileResponse(LOGO_FILE, media_type="image/svg+xml")


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
        async with _sync_lock:
            return await _sync_window(db, from_ms, to_ms)
    except WebexError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


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
    call_rows = upsert_calls(db, tasks, availability_reliable=availability_reliable, raw_agent_sessions=sessions)

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


@app.post("/api/reclassify")
def reclassify_history(
    from_date: str | None = Query(default=None),
    to_date: str | None = Query(default=None),
    limit: int = Query(default=10000, ge=1, le=50000),
    db: Session = Depends(get_db),
):
    """Reclassify stored unserved calls using Contact Center queue history and agentSession states."""
    from_ms = _parse_date_ms(from_date)
    to_ms = _parse_date_ms(to_date, end_of_day=True)
    return {
        "ok": True,
        "from_date": from_date,
        "to_date": to_date,
        **reclassify_calls(db, from_ms=from_ms, to_ms=to_ms, limit=limit),
    }



def _normalize_phone(value: str | None) -> str | None:
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    if len(digits) == 10:
        digits = "1" + digits
    return "+" + digits


def _payload_ms(payload: dict, *keys: str) -> int:
    value = next((payload.get(k) for k in keys if payload.get(k) not in (None, "")), None)
    if value is None:
        return _now_ms()
    try:
        if isinstance(value, (int, float)):
            n = int(value)
            return n if n > 10_000_000_000 else n * 1000
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return _now_ms()


def _check_journey_token(request: Request, query_token: str | None):
    expected = os.getenv("CALL_JOURNEY_WEBHOOK_TOKEN")
    header_token = request.headers.get("X-Webhook-Token")
    if expected and query_token != expected and header_token != expected:
        raise HTTPException(status_code=401, detail="Invalid call journey webhook token")


def _store_journey_event(db: Session, journey_id: str, interaction_id: str | None, event_type: str, event_ms: int, ani: str | None, dnis: str | None, payload: dict):
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    event_key = str(payload.get("event_id") or payload.get("eventId") or hashlib.sha256(f"{journey_id}|{interaction_id}|{event_type}|{event_ms}|{raw}".encode()).hexdigest())
    existing = db.scalar(select(CallJourneyEvent).where(CallJourneyEvent.event_key == event_key))
    if existing:
        return existing, False
    event = CallJourneyEvent(
        event_key=event_key,
        journey_id=journey_id,
        interaction_id=interaction_id,
        event_type=event_type,
        event_ms=event_ms,
        ani=ani,
        dnis=dnis,
        raw_json=raw,
    )
    db.add(event)
    return event, True


@app.post("/api/webhooks/call-overflow")
async def call_overflow_webhook(request: Request, token: str | None = Query(default=None), db: Session = Depends(get_db)):
    _check_journey_token(request, token)
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Webhook body must be JSON") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Webhook body must be a JSON object")

    interaction_id = payload.get("interaction_id") or payload.get("interactionId") or payload.get("task_id") or payload.get("taskId")
    if not interaction_id:
        raise HTTPException(status_code=400, detail="interaction_id is required")
    interaction_id = str(interaction_id).strip()
    ani = _normalize_phone(payload.get("ani") or payload.get("ANI") or payload.get("caller_number"))
    dnis = payload.get("dnis") or payload.get("DNIS") or payload.get("destination")
    event_ms = _payload_ms(payload, "overflow_at", "overflowAt", "timestamp")

    existing = db.scalar(select(CallJourney).where(CallJourney.root_interaction_id == interaction_id))
    created = existing is None
    if existing is None:
        journey_id = str(payload.get("journey_id") or payload.get("journeyId") or uuid.uuid4())
        existing = CallJourney(
            journey_id=journey_id,
            caller_number=ani,
            root_interaction_id=interaction_id,
            overflow_ms=event_ms,
            status="OVERFLOWED",
            raw_json=json.dumps(payload, sort_keys=True, separators=(",", ":")),
        )
        db.add(existing)
        db.flush()
    else:
        journey_id = existing.journey_id
        if ani:
            existing.caller_number = ani
        existing.overflow_ms = min(existing.overflow_ms or event_ms, event_ms)
        existing.status = existing.status or "OVERFLOWED"

    _store_journey_event(db, journey_id, interaction_id, "OVERFLOW_TO_WEBEX_CALLING", event_ms, ani, str(dnis) if dnis else None, payload)
    db.commit()
    return {"ok": True, "created": created, "journey_id": journey_id, "interaction_id": interaction_id, "status": existing.status, "overflow_at": _iso(event_ms)}


@app.post("/api/webhooks/contact-center-return")
async def contact_center_return_webhook(request: Request, token: str | None = Query(default=None), db: Session = Depends(get_db)):
    _check_journey_token(request, token)
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Webhook body must be JSON") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Webhook body must be a JSON object")

    interaction_id = payload.get("interaction_id") or payload.get("interactionId") or payload.get("task_id") or payload.get("taskId")
    if not interaction_id:
        raise HTTPException(status_code=400, detail="interaction_id is required")
    interaction_id = str(interaction_id).strip()
    ani = _normalize_phone(payload.get("ani") or payload.get("ANI") or payload.get("caller_number"))
    dnis = payload.get("dnis") or payload.get("DNIS") or payload.get("destination")
    event_ms = _payload_ms(payload, "returned_at", "returnedAt", "timestamp")
    window_minutes = max(1, int(os.getenv("CALL_JOURNEY_MATCH_WINDOW_MINUTES", "5")))
    window_ms = window_minutes * 60_000

    journey = db.scalar(select(CallJourney).where(CallJourney.returned_interaction_id == interaction_id))
    if journey is None:
        explicit_journey = payload.get("journey_id") or payload.get("journeyId")
        if explicit_journey:
            journey = db.scalar(select(CallJourney).where(CallJourney.journey_id == str(explicit_journey)))
    if journey is None and ani:
        candidates = db.scalars(
            select(CallJourney).where(
                CallJourney.caller_number == ani,
                CallJourney.returned_interaction_id.is_(None),
                CallJourney.overflow_ms <= event_ms,
                CallJourney.overflow_ms >= event_ms - window_ms,
            ).order_by(CallJourney.overflow_ms.desc()).limit(10)
        ).all()
        journey = candidates[0] if candidates else None

    if journey is None:
        raise HTTPException(status_code=409, detail=f"No unmatched overflow journey found for this ANI within {window_minutes} minutes")

    journey.returned_interaction_id = interaction_id
    journey.returned_ms = event_ms
    journey.status = "RETURNED_TO_CONTACT_CENTER"
    _store_journey_event(db, journey.journey_id, interaction_id, "RETURNED_TO_CONTACT_CENTER", event_ms, ani, str(dnis) if dnis else None, payload)
    db.commit()
    return {"ok": True, "matched": True, "journey_id": journey.journey_id, "root_interaction_id": journey.root_interaction_id, "returned_interaction_id": interaction_id, "status": journey.status, "returned_at": _iso(event_ms)}


@app.post("/api/webhooks/call-journey-event")
async def call_journey_event_webhook(request: Request, token: str | None = Query(default=None), db: Session = Depends(get_db)):
    _check_journey_token(request, token)
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Webhook body must be JSON") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Webhook body must be a JSON object")

    interaction_id = payload.get("interaction_id") or payload.get("interactionId") or payload.get("task_id") or payload.get("taskId")
    event_type = str(payload.get("event") or payload.get("event_type") or payload.get("eventType") or "").strip().upper()
    if not interaction_id or not event_type:
        raise HTTPException(status_code=400, detail="interaction_id and event are required")
    interaction_id = str(interaction_id).strip()
    event_ms = _payload_ms(payload, "event_at", "eventAt", "timestamp")
    ani = _normalize_phone(payload.get("ani") or payload.get("ANI") or payload.get("caller_number"))
    dnis = payload.get("dnis") or payload.get("DNIS") or payload.get("destination")

    journey = db.scalar(select(CallJourney).where(
        (CallJourney.root_interaction_id == interaction_id) | (CallJourney.returned_interaction_id == interaction_id)
    ))
    if journey is None:
        raise HTTPException(status_code=409, detail="No call journey is linked to this interaction_id")

    _, created = _store_journey_event(db, journey.journey_id, interaction_id, event_type, event_ms, ani, str(dnis) if dnis else None, payload)
    status_map = {
        "PRIORITY_QUEUE_ENTERED": "PRIORITY_QUEUE",
        "VOICEMAIL": "VOICEMAIL",
        "ANSWERED_AFTER_OVERFLOW": "ANSWERED_AFTER_OVERFLOW",
    }
    if event_type in status_map:
        journey.status = status_map[event_type]
    db.commit()
    return {"ok": True, "created": created, "journey_id": journey.journey_id, "interaction_id": interaction_id, "event": event_type, "status": journey.status, "event_at": _iso(event_ms)}


@app.get("/api/webhooks/call-journey/recent")
def call_journey_recent(limit: int = Query(default=20, ge=1, le=100), db: Session = Depends(get_db)):
    rows = db.scalars(select(CallJourney).order_by(CallJourney.overflow_ms.desc()).limit(limit)).all()
    return [{
        "journey_id": j.journey_id,
        "caller_number": j.caller_number,
        "root_interaction_id": j.root_interaction_id,
        "overflow_at": _iso(j.overflow_ms),
        "returned_interaction_id": j.returned_interaction_id,
        "returned_at": _iso(j.returned_ms) if j.returned_ms else None,
        "status": j.status,
    } for j in rows]



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

    # If Clerk Chat arrives before Webex Search data is in PostgreSQL, immediately
    # pull a recent Webex window, then retry every unmatched SMS event.
    if not result.get("matched") and result.get("to_number") and result.get("sent_ms"):
        fallback_minutes = int(os.getenv("CLERKCHAT_FALLBACK_SYNC_MINUTES", "90"))
        sent_ms = int(result["sent_ms"])
        to_ms = max(_now_ms(), sent_ms)
        from_ms = sent_ms - fallback_minutes * 60_000
        try:
            async with _sync_lock:
                sync_result = await _sync_window(db, from_ms, to_ms)
            event = db.scalar(select(ClerkSmsEvent).where(ClerkSmsEvent.event_key == result.get("event_key")))
            if event:
                result["matched"] = bool(event.matched_task_id)
                result["matched_task_id"] = event.matched_task_id
            result["fallback_sync"] = sync_result
        except WebexError as exc:
            # Preserve the SMS event. The normal auto-sync will retry it later.
            result["fallback_sync_error"] = str(exc)

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


@app.get("/api/sync/status")
def sync_status(db: Session = Depends(get_db)):
    latest_call = db.scalar(select(func.max(Call.created_ms)))
    unmatched_sms = db.scalar(
        select(func.count()).select_from(ClerkSmsEvent).where(ClerkSmsEvent.matched_task_id.is_(None))
    ) or 0
    return {
        "auto_sync_enabled": os.getenv("AUTO_SYNC_ENABLED", "true").lower() not in {"0", "false", "no", "off"},
        "auto_sync_seconds": max(60, int(os.getenv("AUTO_SYNC_SECONDS", "120"))),
        "auto_sync_lookback_minutes": int(os.getenv("AUTO_SYNC_LOOKBACK_MINUTES", "180")),
        "latest_call_at": _iso(latest_call) if latest_call else None,
        "unmatched_sms_events": unmatched_sms,
    }


@app.get("/api/dashboard/summary")
def dashboard_summary(
    queue: str | None = Query(default=None),
    agent: str | None = Query(default=None),
    from_date: str | None = Query(default=None),
    to_date: str | None = Query(default=None),
    caller_query: str | None = Query(default=None),
    sms: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    base = _filtered_calls_stmt(
        queue=queue,
        agent=agent,
        from_date=from_date,
        to_date=to_date,
        caller_query=caller_query,
        sms=sms,
    ).subquery()

    total = db.scalar(select(func.count()).select_from(base)) or 0
    answered = db.scalar(select(func.count()).select_from(base).where(base.c.outcome == "ANSWERED")) or 0
    unserved_available = db.scalar(
        select(func.count()).select_from(base).where(base.c.outcome == "UNSERVED_AGENTS_AVAILABLE")
    ) or 0
    unserved_on_call = db.scalar(
        select(func.count()).select_from(base).where(base.c.outcome == "UNSERVED_AGENTS_ON_CALL")
    ) or 0
    unserved_idle_code = db.scalar(
        select(func.count()).select_from(base).where(base.c.outcome == "UNSERVED_AGENTS_IDLE_CODE")
    ) or 0
    unserved_no_logged_in = db.scalar(
        select(func.count()).select_from(base).where(base.c.outcome == "UNSERVED_NO_AGENTS_LOGGED_IN")
    ) or 0
    # Legacy bucket remains counted until old history is reclassified.
    legacy_no_agents = db.scalar(
        select(func.count()).select_from(base).where(base.c.outcome == "UNSERVED_NO_AGENTS_AVAILABLE")
    ) or 0
    availability_unknown = db.scalar(
        select(func.count()).select_from(base).where(base.c.outcome == "UNSERVED_AVAILABILITY_UNKNOWN")
    ) or 0
    not_queued = db.scalar(
        select(func.count()).select_from(base).where(base.c.outcome == "NOT_QUEUED_IVR_EXIT")
    ) or 0
    in_progress = db.scalar(select(func.count()).select_from(base).where(base.c.outcome == "IN_PROGRESS")) or 0
    sms_sent = db.scalar(select(func.count()).select_from(base).where(base.c.sms_sent.is_(True))) or 0
    avg_wait = db.scalar(select(func.avg(base.c.wait_ms)).where(base.c.wait_ms > 0)) or 0
    longest_wait = db.scalar(select(func.max(base.c.wait_ms))) or 0
    base_task_ids = select(base.c.task_id)
    overflowed_journeys = db.scalar(
        select(func.count()).select_from(CallJourney).where(CallJourney.root_interaction_id.in_(base_task_ids))
    ) or 0
    returned_journeys = db.scalar(
        select(func.count()).select_from(CallJourney).where(
            CallJourney.root_interaction_id.in_(base_task_ids), CallJourney.returned_interaction_id.is_not(None)
        )
    ) or 0
    voicemail_after_overflow = db.scalar(
        select(func.count()).select_from(CallJourney).where(
            CallJourney.root_interaction_id.in_(base_task_ids), CallJourney.status == "VOICEMAIL"
        )
    ) or 0

    return {
        "total_calls": total,
        "answered": answered,
        "unserved": unserved_available + unserved_on_call + unserved_idle_code + unserved_no_logged_in + legacy_no_agents + availability_unknown,
        "unserved_agents_available": unserved_available,
        "unserved_agents_on_call": unserved_on_call,
        "unserved_agents_idle_code": unserved_idle_code,
        "unserved_no_agents_logged_in": unserved_no_logged_in,
        "unserved_no_agents_available": legacy_no_agents,
        "unserved_availability_unknown": availability_unknown,
        "not_queued_ivr_exit": not_queued,
        "in_progress": in_progress,
        "sms_sent": sms_sent,
        "overflowed_to_calling": overflowed_journeys,
        "returned_to_contact_center": returned_journeys,
        "voicemail_after_overflow": voicemail_after_overflow,
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
    caller_query: str | None = Query(default=None),
    sms: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    stmt = _filtered_calls_stmt(
        outcome=outcome,
        queue=queue,
        agent=agent,
        from_date=from_date,
        to_date=to_date,
        caller_query=caller_query,
        sms=sms,
    ).order_by(Call.created_ms.desc()).limit(limit)
    calls = db.scalars(stmt).all()
    task_ids = [c.task_id for c in calls]
    journeys_by_interaction = {}
    if task_ids:
        journey_rows = db.scalars(select(CallJourney).where(
            (CallJourney.root_interaction_id.in_(task_ids)) | (CallJourney.returned_interaction_id.in_(task_ids))
        )).all()
        for j in journey_rows:
            journeys_by_interaction[j.root_interaction_id] = j
            if j.returned_interaction_id:
                journeys_by_interaction[j.returned_interaction_id] = j
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
            "agent_state_count": c.available_agent_count,
            "agent_state_names": c.available_agent_names,
            "agent_state_reason": c.outcome,
            "sms_sent": c.sms_sent,
            "sms_sent_at": _iso(c.sms_sent_ms) if c.sms_sent_ms else None,
            "journey_id": journeys_by_interaction[c.task_id].journey_id if c.task_id in journeys_by_interaction else None,
            "journey_status": journeys_by_interaction[c.task_id].status if c.task_id in journeys_by_interaction else None,
            "journey_root_interaction_id": journeys_by_interaction[c.task_id].root_interaction_id if c.task_id in journeys_by_interaction else None,
            "journey_returned_interaction_id": journeys_by_interaction[c.task_id].returned_interaction_id if c.task_id in journeys_by_interaction else None,
        }
        for c in calls
    ]


@app.get("/api/dashboard/filters")
def dashboard_filters(db: Session = Depends(get_db)):
    queues = [x for x in db.scalars(select(Call.queue_name).where(Call.queue_name.is_not(None)).distinct().order_by(Call.queue_name)).all() if x]
    agents = [x for x in db.scalars(select(Call.agent_name).where(Call.agent_name.is_not(None)).distinct().order_by(Call.agent_name)).all() if x]
    outcomes = [x for x in db.scalars(select(Call.outcome).distinct().order_by(Call.outcome)).all() if x]
    return {"queues": queues, "agents": agents, "outcomes": outcomes}

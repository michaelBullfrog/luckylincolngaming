import json
import hashlib
import re
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.orm import Session
from .models import Call, AgentActivity, ClerkSmsEvent

AVAILABLE_STATES = {"available"}
ON_CALL_STATES = {"connected", "ringing", "inbound-reserved", "call-ended"}
IDLE_CODE_STATES = {"idle", "wrapup", "wrap-up"}


def _event_map(task: dict) -> dict[str, list[int]]:
    events: dict[str, list[int]] = {}
    for node in (task.get("activities") or {}).get("nodes", []) or []:
        name = node.get("eventName")
        if not name:
            continue
        events.setdefault(name, []).append(node.get("duration") or 0)
    return events


def _fallback_wait_ms(task: dict) -> int:
    queue_duration = task.get("queueDuration") or 0
    if queue_duration > 0:
        return int(queue_duration)
    events = _event_map(task)
    queued = events.get("queued", [])
    return int(max(queued) if queued else 0)


def _answered(task: dict) -> bool:
    events = _event_map(task)
    last_agent = task.get("lastAgent") or {}
    return bool(last_agent.get("id") and "connected" in events)


def _queue_window(task: dict) -> tuple[int, int] | None:
    """Best available queue-wait interval for an unserved task.

    Webex taskDetails gives queue duration but not absolute timestamps for each task
    activity in this query. For an unserved call, the queue wait is normally the
    final portion of the task, so we anchor the known wait duration to endedTime.
    This is much tighter than comparing agent state against the entire IVR/task.
    """
    created_ms = int(task.get("createdTime") or 0)
    ended_ms = int(task.get("endedTime") or -1)
    wait_ms = _fallback_wait_ms(task)
    if ended_ms <= 0 or wait_ms <= 0:
        return None
    start_ms = max(created_ms, ended_ms - wait_ms)
    if start_ms >= ended_ms:
        return None
    return start_ms, ended_ms


def upsert_agent_activities(db: Session, sessions: list[dict]) -> int:
    written = 0
    for session in sessions:
        for channel in session.get("channelInfo") or []:
            activities = ((channel.get("activities") or {}).get("nodes") or [])
            for node in activities:
                activity_id = node.get("id")
                if not activity_id:
                    continue
                existing = db.scalar(select(AgentActivity).where(AgentActivity.activity_id == activity_id))
                values = {
                    "agent_id": session.get("agentId"),
                    "agent_name": session.get("agentName") or session.get("agentId") or "Unknown",
                    "team_name": session.get("teamName"),
                    "channel_type": channel.get("channelType"),
                    "state": (node.get("state") or "unknown").lower(),
                    "start_ms": int(node.get("startTime") or 0),
                    "end_ms": int(node.get("endTime") or -1),
                }
                if existing:
                    for key, value in values.items():
                        setattr(existing, key, value)
                else:
                    db.add(AgentActivity(activity_id=activity_id, **values))
                written += 1
    db.commit()
    return written


def agent_states_during(db: Session, start_ms: int, end_ms: int) -> dict[str, list[str]]:
    """Return unique telephony agents by state overlapping the queue window."""
    from sqlalchemy import or_

    rows = db.scalars(
        select(AgentActivity).where(
            AgentActivity.channel_type == "telephony",
            AgentActivity.start_ms < end_ms,
            or_(AgentActivity.end_ms == -1, AgentActivity.end_ms > start_ms),
        )
    ).all()

    buckets: dict[str, set[str]] = {
        "available": set(),
        "on_call": set(),
        "idle_code": set(),
        "logged_in": set(),
    }
    for row in rows:
        name = row.agent_name or row.agent_id or "Unknown"
        state = (row.state or "unknown").lower()
        buckets["logged_in"].add(name)
        if state in AVAILABLE_STATES:
            buckets["available"].add(name)
        elif state in ON_CALL_STATES:
            buckets["on_call"].add(name)
        else:
            # Any other overlapping telephony state means the agent was logged in
            # but not available/on-call (idle, AUX/code, wrap-up, etc.).
            buckets["idle_code"].add(name)

    return {key: sorted(value) for key, value in buckets.items()}


def classify_task(db: Session, task: dict, *, availability_reliable: bool = True) -> tuple[str, list[str]]:
    if _answered(task):
        return "ANSWERED", []

    ended_ms = int(task.get("endedTime") or -1)
    if ended_ms == -1:
        return "IN_PROGRESS", []

    # Calls that never reached a queue should not be counted as an
    # "unserved queue call."  Webex commonly shows these with no lastQueue
    # and no measurable queue wait (for example an IVR exit / early hang-up).
    last_queue = task.get("lastQueue") or {}
    wait_ms = _fallback_wait_ms(task)
    if not last_queue.get("id") and not last_queue.get("name") and wait_ms <= 0:
        return "NOT_QUEUED_IVR_EXIT", []

    if not availability_reliable:
        return "UNSERVED_AVAILABILITY_UNKNOWN", []

    queue_window = _queue_window(task)
    if not queue_window:
        return "UNSERVED_AVAILABILITY_UNKNOWN", []

    states = agent_states_during(db, *queue_window)

    # Precedence answers the operational question: was anyone truly available?
    # If not, were agents tied up on calls? If not, were they logged in but in
    # idle/AUX/wrap-up? Finally, no overlapping telephony state means no agents
    # were logged in during the caller's queue wait.
    if states["available"]:
        return "UNSERVED_AGENTS_AVAILABLE", states["available"]
    if states["on_call"]:
        return "UNSERVED_AGENTS_ON_CALL", states["on_call"]
    if states["idle_code"]:
        return "UNSERVED_AGENTS_IDLE_CODE", states["idle_code"]
    return "UNSERVED_NO_AGENTS_LOGGED_IN", []


def upsert_calls(db: Session, tasks: list[dict], *, availability_reliable: bool = True) -> int:
    written = 0
    for task in tasks:
        task_id = task.get("id")
        if not task_id:
            continue
        existing = db.scalar(select(Call).where(Call.task_id == task_id))
        last_agent = task.get("lastAgent") or {}
        last_queue = task.get("lastQueue") or {}
        answered = _answered(task)
        outcome, state_names = classify_task(db, task, availability_reliable=availability_reliable)

        values = {
            "caller_number": task.get("origin"),
            "destination": task.get("destination"),
            "queue_id": last_queue.get("id"),
            "queue_name": last_queue.get("name"),
            "created_ms": int(task.get("createdTime") or 0),
            "ended_ms": int(task.get("endedTime") or -1),
            "wait_ms": _fallback_wait_ms(task),
            "total_duration_ms": int(task.get("totalDuration") or 0),
            "answered": answered,
            "agent_id": last_agent.get("id"),
            "agent_name": last_agent.get("name"),
            "outcome": outcome,
            # Kept in the existing columns to avoid a PostgreSQL schema migration.
            # For unserved calls these now hold the agents in the winning state bucket.
            "available_agent_count": len(state_names),
            "available_agent_names": json.dumps(state_names),
            "raw_json": json.dumps(task),
        }
        if existing:
            for key, value in values.items():
                setattr(existing, key, value)
        else:
            db.add(Call(task_id=task_id, **values))
        written += 1
    db.commit()
    return written


def reclassify_calls(db: Session, from_ms: int | None = None, to_ms: int | None = None, limit: int = 10000) -> dict:
    """Re-run availability classification using already-stored Webex history."""
    stmt = select(Call).where(Call.answered.is_(False), Call.ended_ms >= 0)
    if from_ms is not None:
        stmt = stmt.where(Call.created_ms >= from_ms)
    if to_ms is not None:
        stmt = stmt.where(Call.created_ms <= to_ms)
    stmt = stmt.order_by(Call.created_ms.asc()).limit(limit)
    calls = db.scalars(stmt).all()

    changed = 0
    counts: dict[str, int] = {}
    for call in calls:
        try:
            task = json.loads(call.raw_json or "{}")
        except json.JSONDecodeError:
            task = {}
        if not task:
            continue
        outcome, state_names = classify_task(db, task, availability_reliable=True)
        if call.outcome != outcome or call.available_agent_names != json.dumps(state_names):
            changed += 1
        call.outcome = outcome
        call.available_agent_count = len(state_names)
        call.available_agent_names = json.dumps(state_names)
        counts[outcome] = counts.get(outcome, 0) + 1

    db.commit()
    return {"processed": len(calls), "changed": changed, "outcomes": counts}


SMS_MATCH_WINDOW_MINUTES = int(__import__("os").getenv("SMS_MATCH_WINDOW_MINUTES", "60"))
SMS_MATCH_WINDOW_MS = SMS_MATCH_WINDOW_MINUTES * 60 * 1000


def normalize_phone(value: str | None) -> str | None:
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    if len(digits) == 10:
        digits = "1" + digits
    return "+" + digits


def _walk_values(obj, keys: set[str]):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() in keys and v not in (None, ""):
                yield v
            yield from _walk_values(v, keys)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_values(item, keys)


def _first_value(payload: dict, *keys: str):
    return next(_walk_values(payload, {k.lower() for k in keys}), None)


def _timestamp_ms(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        n = int(value)
        return n if n > 10_000_000_000 else n * 1000
    text = str(value).strip()
    if text.isdigit():
        n = int(text)
        return n if n > 10_000_000_000 else n * 1000
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


def find_matching_call(db: Session, to_number: str | None, sent_ms: int | None) -> Call | None:
    """Find the most recent ended Webex call to this recipient inside the SMS window."""
    if not to_number or sent_ms is None:
        return None
    candidates = db.scalars(
        select(Call)
        .where(
            Call.ended_ms >= 0,
            Call.ended_ms <= sent_ms,
            Call.ended_ms >= sent_ms - SMS_MATCH_WINDOW_MS,
        )
        .order_by(Call.ended_ms.desc())
        .limit(100)
    ).all()
    for call in candidates:
        if normalize_phone(call.caller_number) == to_number:
            return call
    return None


def match_sms_event(db: Session, event: ClerkSmsEvent) -> Call | None:
    """Attach one stored outbound SMS event to a matching call if possible."""
    if event.matched_task_id or not event.to_number or event.sent_ms is None:
        return None
    if event.direction and "inbound" in event.direction.lower():
        return None
    call = find_matching_call(db, event.to_number, event.sent_ms)
    if not call:
        return None
    call.sms_sent = True
    call.sms_sent_ms = event.sent_ms
    event.matched_task_id = call.task_id
    return call


def retry_unmatched_sms(db: Session, *, max_age_hours: int = 24, now_ms: int | None = None) -> dict:
    """Retry recent Clerk Chat events that arrived before their Webex call was synced."""
    now_ms = now_ms or int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    cutoff = now_ms - max_age_hours * 60 * 60 * 1000
    events = db.scalars(
        select(ClerkSmsEvent)
        .where(
            ClerkSmsEvent.matched_task_id.is_(None),
            ClerkSmsEvent.sent_ms.is_not(None),
            ClerkSmsEvent.sent_ms >= cutoff,
        )
        .order_by(ClerkSmsEvent.sent_ms.asc())
        .limit(1000)
    ).all()
    matched = 0
    checked = 0
    for event in events:
        checked += 1
        if match_sms_event(db, event):
            matched += 1
    if matched:
        db.commit()
    return {"sms_retry_checked": checked, "sms_retry_matched": matched}


def record_clerkchat_outbound(db: Session, payload: dict, received_ms: int) -> dict:
    """Store a Clerk Chat webhook and immediately try to match it to a recent call."""
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    message_id = _first_value(payload, "messageId", "message_id", "id")
    direction_raw = _first_value(payload, "direction", "messageDirection", "message_direction")
    direction = str(direction_raw).lower() if direction_raw is not None else "outbound"

    to_raw = _first_value(payload, "to", "toNumber", "to_number", "recipient", "recipientNumber", "destination")
    from_raw = _first_value(payload, "from", "fromNumber", "from_number", "sender", "senderNumber", "source")
    to_number = normalize_phone(str(to_raw)) if to_raw is not None else None
    from_number = normalize_phone(str(from_raw)) if from_raw is not None else None

    ts_raw = _first_value(payload, "sentAt", "sent_at", "timestamp", "createdAt", "created_at", "time")
    sent_ms = _timestamp_ms(ts_raw) or received_ms

    event_key = str(message_id) if message_id else hashlib.sha256(raw.encode()).hexdigest()
    existing_event = db.scalar(select(ClerkSmsEvent).where(ClerkSmsEvent.event_key == event_key))
    if existing_event:
        if not existing_event.matched_task_id:
            match_sms_event(db, existing_event)
            db.commit()
        return {
            "stored": False,
            "duplicate": True,
            "message_id": existing_event.message_id,
            "to_number": existing_event.to_number,
            "sent_ms": existing_event.sent_ms,
            "matched": bool(existing_event.matched_task_id),
            "matched_task_id": existing_event.matched_task_id,
            "event_key": existing_event.event_key,
        }

    event = ClerkSmsEvent(
        event_key=event_key,
        message_id=str(message_id) if message_id is not None else None,
        to_number=to_number,
        from_number=from_number,
        direction=direction,
        sent_ms=sent_ms,
        matched_task_id=None,
        raw_json=raw,
    )
    db.add(event)
    db.flush()
    matched_call = match_sms_event(db, event)
    db.commit()
    return {
        "stored": True,
        "duplicate": False,
        "message_id": event.message_id,
        "direction": event.direction,
        "to_number": event.to_number,
        "sent_ms": event.sent_ms,
        "matched": matched_call is not None,
        "matched_task_id": event.matched_task_id,
        "event_key": event.event_key,
    }

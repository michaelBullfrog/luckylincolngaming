import json
import hashlib
import re
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.orm import Session
from .models import Call, AgentActivity, ClerkSmsEvent

AVAILABLE_STATES = {"available"}


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
                    "state": node.get("state") or "unknown",
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


def available_agents_during(db: Session, start_ms: int, end_ms: int) -> list[str]:
    # V1 uses the task interval as the comparison window. Once Webex task activity
    # timestamps are added, replace start_ms/end_ms with the exact queue interval.
    rows = db.scalars(
        select(AgentActivity).where(
            AgentActivity.state.in_(AVAILABLE_STATES),
            AgentActivity.start_ms < end_ms,
        )
    ).all()

    names: set[str] = set()
    for row in rows:
        row_end = end_ms if row.end_ms == -1 else row.end_ms
        if row_end > start_ms:
            names.add(row.agent_name)
    return sorted(names)


def classify_task(db: Session, task: dict, *, availability_reliable: bool = True) -> tuple[str, list[str]]:
    if _answered(task):
        return "ANSWERED", []

    created_ms = int(task.get("createdTime") or 0)
    ended_ms = int(task.get("endedTime") or -1)
    if ended_ms == -1:
        return "IN_PROGRESS", []

    if not availability_reliable:
        return "UNSERVED_AVAILABILITY_UNKNOWN", []

    available = available_agents_during(db, created_ms, ended_ms)
    events = _event_map(task)

    if available:
        return "UNSERVED_AGENTS_AVAILABLE", available
    if "optOutOfQueue" in events or "dequeued" in events:
        return "UNSERVED_NO_AGENTS_AVAILABLE", []
    return "UNSERVED_NO_AGENTS_AVAILABLE", []


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
        outcome, available_names = classify_task(db, task, availability_reliable=availability_reliable)

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
            "available_agent_count": len(available_names),
            "available_agent_names": json.dumps(available_names),
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


SMS_MATCH_WINDOW_MS = 30 * 60 * 1000


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


def record_clerkchat_outbound(db: Session, payload: dict, received_ms: int) -> dict:
    """Store a Clerk Chat webhook and match an outbound SMS to the most recent call.

    Clerk Chat's public support docs expose webhook subscriptions but do not publish a
    stable payload schema. This parser intentionally recognizes several common field
    names. Once the first real payload arrives, the exact mappings can be tightened.
    """
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    message_id = _first_value(payload, "messageId", "message_id", "id")
    direction_raw = _first_value(payload, "direction", "messageDirection", "message_direction")
    direction = str(direction_raw).lower() if direction_raw is not None else "outbound"

    # Prefer explicit recipient/destination keys and then generic `to`.
    to_raw = _first_value(payload, "to", "toNumber", "to_number", "recipient", "recipientNumber", "destination")
    from_raw = _first_value(payload, "from", "fromNumber", "from_number", "sender", "senderNumber", "source")
    to_number = normalize_phone(str(to_raw)) if to_raw is not None else None
    from_number = normalize_phone(str(from_raw)) if from_raw is not None else None

    ts_raw = _first_value(payload, "sentAt", "sent_at", "timestamp", "createdAt", "created_at", "time")
    sent_ms = _timestamp_ms(ts_raw) or received_ms

    event_key = str(message_id) if message_id else hashlib.sha256(raw.encode()).hexdigest()
    existing_event = db.scalar(select(ClerkSmsEvent).where(ClerkSmsEvent.event_key == event_key))
    if existing_event:
        return {
            "stored": False,
            "duplicate": True,
            "message_id": existing_event.message_id,
            "to_number": existing_event.to_number,
            "sent_ms": existing_event.sent_ms,
            "matched_task_id": existing_event.matched_task_id,
        }

    matched_call = None
    # The webhook endpoint is intended to subscribe only to outgoing messages. If a
    # direction field exists and explicitly says inbound, store it but don't match it.
    is_outbound = "inbound" not in direction
    if is_outbound and to_number:
        candidates = db.scalars(
            select(Call)
            .where(
                Call.ended_ms >= 0,
                Call.ended_ms <= sent_ms,
                Call.ended_ms >= sent_ms - SMS_MATCH_WINDOW_MS,
            )
            .order_by(Call.ended_ms.desc())
            .limit(50)
        ).all()
        for call in candidates:
            if normalize_phone(call.caller_number) == to_number:
                matched_call = call
                break

    if matched_call:
        matched_call.sms_sent = True
        matched_call.sms_sent_ms = sent_ms

    event = ClerkSmsEvent(
        event_key=event_key,
        message_id=str(message_id) if message_id is not None else None,
        to_number=to_number,
        from_number=from_number,
        direction=direction,
        sent_ms=sent_ms,
        matched_task_id=matched_call.task_id if matched_call else None,
        raw_json=raw,
    )
    db.add(event)
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
    }

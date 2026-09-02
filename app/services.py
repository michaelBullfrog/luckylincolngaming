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
    """Return the best queue-wait interval for an unserved task.

    Prefer the actual Customer Activity Record for the latest `queued` event.
    Webex exposes a CAR `createdTime` plus `duration`, which lets us calculate
    the queue exit timestamp directly instead of assuming the whole task ended
    at the same moment the caller left the queue.

    Older rows imported before v1.0.4 do not contain CAR createdTime. Those rows
    retain the previous endedTime-based fallback until they are refreshed from
    Webex by the dashboard's Reclassify button.
    """
    activities = ((task.get("activities") or {}).get("nodes") or [])
    queued = []
    for node in activities:
        if (node.get("eventName") or "").lower() != "queued":
            continue
        start_ms = int(node.get("createdTime") or 0)
        duration_ms = int(node.get("duration") or 0)
        if start_ms > 0 and duration_ms > 0:
            queued.append((start_ms, start_ms + duration_ms))

    if queued:
        # A task can touch more than one queue. lastQueue corresponds most closely
        # to the latest queued CAR, so use the chronologically latest queue leg.
        start_ms, end_ms = max(queued, key=lambda item: item[0])
        task_end_ms = int(task.get("endedTime") or -1)
        if task_end_ms > 0:
            end_ms = min(end_ms, task_end_ms)
        if end_ms > start_ms:
            return start_ms, end_ms

    # Backward-compatible fallback for previously stored task JSON.
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
                # AAR activity rows can occasionally report endTime=-1/null even
                # after the parent agent session has ended. Never allow a child
                # telephony state to outlive its session, otherwise a stale
                # `available` row can make a logged-out agent appear available
                # hours later. Prefer the activity endTime when present; if it is
                # still open, cap it at the parent agentSession endTime. Only keep
                # -1 when BOTH the activity and the agent session are truly open.
                activity_end_ms = int(node.get("endTime") or -1)
                session_end_ms = int(session.get("endTime") or -1)
                session_is_active = session.get("isActive")

                # Some historical agentSession records can remain with endTime=-1
                # even after Webex reports the parent session as inactive. In that
                # case an open `available` child activity must NOT be allowed to
                # remain active forever. When Webex explicitly says the session is
                # inactive but gives no session endTime, cap the open activity at
                # the latest timestamp actually observed inside that session.
                if session_end_ms <= 0 and session_is_active is False:
                    observed_times = [int(session.get("startTime") or 0)]
                    for session_channel in session.get("channelInfo") or []:
                        for session_node in ((session_channel.get("activities") or {}).get("nodes") or []):
                            observed_times.append(int(session_node.get("startTime") or 0))
                            node_end = int(session_node.get("endTime") or -1)
                            if node_end > 0:
                                observed_times.append(node_end)
                    inferred_session_end_ms = max(observed_times or [0])
                    if inferred_session_end_ms > 0:
                        session_end_ms = inferred_session_end_ms

                if activity_end_ms <= 0 and session_end_ms > 0:
                    effective_end_ms = session_end_ms
                elif activity_end_ms > 0 and session_end_ms > 0:
                    effective_end_ms = min(activity_end_ms, session_end_ms)
                else:
                    effective_end_ms = activity_end_ms

                values = {
                    "agent_id": session.get("agentId"),
                    "agent_name": session.get("agentName") or session.get("agentId") or "Unknown",
                    "team_name": session.get("teamName"),
                    "channel_type": channel.get("channelType"),
                    "state": (node.get("state") or "unknown").lower(),
                    "start_ms": int(node.get("startTime") or 0),
                    "end_ms": effective_end_ms,
                }
                if existing:
                    for key, value in values.items():
                        setattr(existing, key, value)
                else:
                    db.add(AgentActivity(activity_id=activity_id, **values))
                written += 1
    db.commit()
    return written


def observed_queue_agent_ids(db: Session, queue_id: str | None, queue_name: str | None) -> list[str]:
    """Infer queue eligibility from Contact Center history only.

    We use agents who have actually answered calls for this queue in the stored
    taskDetails history. This avoids treating an unrelated Contact Center agent
    as available just because they were in an `available` state elsewhere.
    """
    if not queue_id and not queue_name:
        return []

    stmt = select(Call.agent_id).where(
        Call.answered.is_(True),
        Call.agent_id.is_not(None),
    )
    if queue_id:
        stmt = stmt.where(Call.queue_id == queue_id)
    elif queue_name:
        stmt = stmt.where(Call.queue_name == queue_name)

    rows = db.scalars(stmt.distinct()).all()
    return sorted({str(v) for v in rows if v})


def agent_states_at_queue_exit(
    db: Session,
    queue_end_ms: int,
    *,
    eligible_agent_ids: list[str],
) -> dict[str, list[str]]:
    """Return each eligible agent's Contact Center telephony state at queue exit.

    We sample one millisecond before the caller leaves the queue so an agent is
    counted as available only if their `available` activity is active at that
    exact point. An agent who was available earlier in the wait but became busy
    before the caller abandoned is therefore not reported as available.
    """
    from sqlalchemy import or_

    if not eligible_agent_ids or queue_end_ms <= 0:
        return {"available": [], "on_call": [], "idle_code": [], "logged_in": []}

    sample_ms = max(0, queue_end_ms - 1)
    rows = db.scalars(
        select(AgentActivity).where(
            AgentActivity.channel_type == "telephony",
            AgentActivity.agent_id.in_(eligible_agent_ids),
            AgentActivity.start_ms <= sample_ms,
            or_(AgentActivity.end_ms == -1, AgentActivity.end_ms > sample_ms),
        )
    ).all()

    # Webex history can occasionally contain overlapping telephony activity
    # rows for the same agent. Do NOT count every overlapping row. At the
    # queue-exit timestamp, select exactly one effective state per agent: the
    # active activity with the latest start time. This prevents an older
    # `available` interval from making an agent appear available when a newer
    # `connected`, `ringing`, `idle`, etc. state is also active.
    latest_by_agent: dict[str, AgentActivity] = {}
    for row in rows:
        agent_key = str(row.agent_id or "")
        if not agent_key:
            continue
        current = latest_by_agent.get(agent_key)
        if current is None or int(row.start_ms or -1) > int(current.start_ms or -1):
            latest_by_agent[agent_key] = row

    buckets: dict[str, set[str]] = {
        "available": set(),
        "on_call": set(),
        "idle_code": set(),
        "logged_in": set(),
    }
    for row in latest_by_agent.values():
        name = row.agent_name or row.agent_id or "Unknown"
        state = (row.state or "unknown").lower()
        buckets["logged_in"].add(name)
        if state in AVAILABLE_STATES:
            buckets["available"].add(name)
        elif state in ON_CALL_STATES:
            buckets["on_call"].add(name)
        else:
            buckets["idle_code"].add(name)

    return {key: sorted(value) for key, value in buckets.items()}



def agent_states_from_raw_sessions_at(
    queue_end_ms: int,
    sessions: list[dict],
    *,
    eligible_agent_ids: list[str],
) -> dict[str, list[str]]:
    """Resolve one exact telephony state per agent directly from raw Webex sessions.

    This intentionally bypasses the accumulated AgentActivity table. For the
    caller's queue-exit timestamp we only consider an agentSession that itself
    covers that timestamp, then choose the latest telephony activity active at
    that instant. Only a literal `available` state is reported as available.
    """
    if not eligible_agent_ids or queue_end_ms <= 0:
        return {"available": [], "on_call": [], "idle_code": [], "logged_in": []}

    sample_ms = max(0, queue_end_ms - 1)
    eligible = {str(v) for v in eligible_agent_ids if v}
    latest_by_agent: dict[str, tuple[int, str, str]] = {}

    for session in sessions or []:
        agent_id = str(session.get("agentId") or "")
        if not agent_id or agent_id not in eligible:
            continue

        session_start = int(session.get("startTime") or 0)
        session_end = int(session.get("endTime") or -1)
        is_active = session.get("isActive")

        if session_start > sample_ms:
            continue
        if session_end > 0:
            if session_end <= sample_ms:
                continue
        elif is_active is not True:
            # An open-ended session is only allowed to cover the sample when
            # Webex explicitly says it is still active. This prevents old,
            # inactive sessions from behaving as if they lasted forever.
            continue

        agent_name = session.get("agentName") or agent_id
        for channel in session.get("channelInfo") or []:
            if (channel.get("channelType") or "").lower() != "telephony":
                continue
            for node in ((channel.get("activities") or {}).get("nodes") or []):
                start_ms = int(node.get("startTime") or 0)
                end_ms = int(node.get("endTime") or -1)
                if start_ms > sample_ms:
                    continue
                if end_ms > 0 and end_ms <= sample_ms:
                    continue
                # If the activity is open-ended, the parent session coverage
                # check above is the guardrail.
                state = (node.get("state") or "unknown").lower()
                current = latest_by_agent.get(agent_id)
                if current is None or start_ms > current[0]:
                    latest_by_agent[agent_id] = (start_ms, state, agent_name)

    buckets: dict[str, set[str]] = {
        "available": set(),
        "on_call": set(),
        "idle_code": set(),
        "logged_in": set(),
    }
    for _agent_id, (_start, state, name) in latest_by_agent.items():
        buckets["logged_in"].add(name)
        if state in AVAILABLE_STATES:
            buckets["available"].add(name)
        elif state in ON_CALL_STATES:
            buckets["on_call"].add(name)
        else:
            buckets["idle_code"].add(name)

    return {key: sorted(value) for key, value in buckets.items()}

def classify_task(
    db: Session,
    task: dict,
    *,
    availability_reliable: bool = True,
    queue_roster_cache: dict[str, list[str]] | None = None,
    raw_agent_sessions: list[dict] | None = None,
) -> tuple[str, list[str]]:
    if _answered(task):
        return "ANSWERED", []

    ended_ms = int(task.get("endedTime") or -1)
    if ended_ms == -1:
        return "IN_PROGRESS", []

    last_queue = task.get("lastQueue") or {}
    wait_ms = _fallback_wait_ms(task)
    if not last_queue.get("id") and not last_queue.get("name") and wait_ms <= 0:
        return "NOT_QUEUED_IVR_EXIT", []

    if not availability_reliable:
        return "UNSERVED_AVAILABILITY_UNKNOWN", []

    queue_window = _queue_window(task)
    if not queue_window:
        return "UNSERVED_AVAILABILITY_UNKNOWN", []

    queue_id = last_queue.get("id")
    queue_name = last_queue.get("name")
    cache_key = str(queue_id or queue_name or "")
    if queue_roster_cache is not None and cache_key in queue_roster_cache:
        eligible_ids = queue_roster_cache[cache_key]
    else:
        eligible_ids = observed_queue_agent_ids(db, queue_id, queue_name)
        if queue_roster_cache is not None and eligible_ids:
            queue_roster_cache[cache_key] = eligible_ids

    # If Contact Center history has never shown an agent answering this queue,
    # we do not guess. This is intentionally conservative.
    if not eligible_ids:
        return "UNSERVED_AVAILABILITY_UNKNOWN", []

    # Evaluate state at the caller's queue exit, not anywhere during the wait.
    # This prevents an agent who was briefly available earlier from being shown
    # as available when the caller actually abandoned or left the queue.
    _, queue_end_ms = queue_window
    if raw_agent_sessions is not None:
        states = agent_states_from_raw_sessions_at(
            queue_end_ms, raw_agent_sessions, eligible_agent_ids=eligible_ids
        )
    else:
        states = agent_states_at_queue_exit(db, queue_end_ms, eligible_agent_ids=eligible_ids)

    # Exclusive classification at queue exit using Contact Center state only.
    # The names returned for Agent Available are therefore only agents whose
    # telephony state was actually `available` when the caller left the queue.
    if states["available"]:
        return "UNSERVED_AGENTS_AVAILABLE", states["available"]
    if states["on_call"]:
        return "UNSERVED_AGENTS_ON_CALL", states["on_call"]
    if states["idle_code"]:
        return "UNSERVED_AGENTS_IDLE_CODE", states["idle_code"]
    return "UNSERVED_NO_AGENTS_LOGGED_IN", []


def upsert_calls(
    db: Session,
    tasks: list[dict],
    *,
    availability_reliable: bool = True,
    raw_agent_sessions: list[dict] | None = None,
) -> int:
    written = 0
    queue_roster_cache: dict[str, list[str]] = {}
    for task in tasks:
        task_id = task.get("id")
        if not task_id:
            continue
        existing = db.scalar(select(Call).where(Call.task_id == task_id))
        last_agent = task.get("lastAgent") or {}
        last_queue = task.get("lastQueue") or {}
        answered = _answered(task)
        outcome, state_names = classify_task(
            db,
            task,
            availability_reliable=availability_reliable,
            queue_roster_cache=queue_roster_cache,
            raw_agent_sessions=raw_agent_sessions,
        )

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
    queue_roster_cache: dict[str, list[str]] = {}
    for call in calls:
        try:
            task = json.loads(call.raw_json or "{}")
        except json.JSONDecodeError:
            task = {}
        if not task:
            continue
        outcome, state_names = classify_task(
            db, task, availability_reliable=True, queue_roster_cache=queue_roster_cache
        )
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

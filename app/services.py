import json
from sqlalchemy import select
from sqlalchemy.orm import Session
from .models import Call, AgentActivity

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

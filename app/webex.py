import asyncio
import os
import time
from typing import Any

import httpx

from .db import SessionLocal
from .models import WebexOAuthToken

SEARCH_URL = os.getenv("WEBEX_SEARCH_URL", "")
ACCESS_TOKEN = os.getenv("WEBEX_ACCESS_TOKEN", "")
CLIENT_ID = os.getenv("WEBEX_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("WEBEX_CLIENT_SECRET", "")
REFRESH_TOKEN = os.getenv("WEBEX_REFRESH_TOKEN", "")
TOKEN_URL = os.getenv("WEBEX_TOKEN_URL", "https://webexapis.com/v1/access_token")

_token_lock = asyncio.Lock()

TASK_DETAILS_QUERY = """
query($from: Long!, $to: Long!, $cursor: String!) {
  taskDetails(from: $from, to: $to, pagination: { cursor: $cursor }) {
    tasks {
      id
      origin
      destination
      createdTime
      endedTime
      totalDuration
      queueCount
      queueDuration
      lastAgent { id name }
      lastQueue { id name }
      activities(first: 100) {
        totalCount
        pageInfo { endCursor hasNextPage }
        nodes { id createdTime eventName duration }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

TASK_DETAILS_ENDED_QUERY = """
query($from: Long!, $to: Long!, $cursor: String!) {
  taskDetails(
    from: $from
    to: $to
    timeComparator: endedTime
    pagination: { cursor: $cursor }
  ) {
    tasks {
      id
      origin
      destination
      createdTime
      endedTime
      totalDuration
      queueCount
      queueDuration
      lastAgent { id name }
      lastQueue { id name }
      activities(first: 100) {
        totalCount
        pageInfo { endCursor hasNextPage }
        nodes { id createdTime eventName duration }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

AGENT_SESSION_QUERY = """
query($from: Long!, $to: Long!, $cursor: String!) {
  agentSession(from: $from, to: $to, pagination: { cursor: $cursor }) {
    agentSessions {
      agentSessionId
      agentId
      agentName
      teamName
      startTime
      endTime
      isActive
      state
      channelInfo {
        channelId
        channelType
        activities(first: 100) {
          totalCount
          pageInfo { endCursor hasNextPage }
          nodes { id startTime endTime state }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


class WebexError(RuntimeError):
    pass


def _now_ms() -> int:
    return int(time.time() * 1000)


def _oauth_configured() -> bool:
    return bool(CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN)


def _load_saved_token() -> WebexOAuthToken | None:
    try:
        with SessionLocal() as db:
            row = db.get(WebexOAuthToken, 1)
            if row is None:
                return None
            return WebexOAuthToken(
                id=row.id,
                access_token=row.access_token,
                refresh_token=row.refresh_token,
                access_token_expires_at_ms=row.access_token_expires_at_ms,
                refresh_token_expires_at_ms=row.refresh_token_expires_at_ms,
                updated_ms=row.updated_ms,
            )
    except Exception:
        return None


def _save_token(payload: dict[str, Any], fallback_refresh_token: str) -> None:
    now = _now_ms()
    expires_in = int(payload.get("expires_in") or 0)
    refresh_expires_in = int(payload.get("refresh_token_expires_in") or 0)
    new_refresh = payload.get("refresh_token") or fallback_refresh_token

    with SessionLocal() as db:
        row = db.get(WebexOAuthToken, 1)
        if row is None:
            row = WebexOAuthToken(id=1, updated_ms=now)
            db.add(row)
        row.access_token = payload.get("access_token")
        row.refresh_token = new_refresh
        row.access_token_expires_at_ms = now + expires_in * 1000 if expires_in else None
        row.refresh_token_expires_at_ms = (
            now + refresh_expires_in * 1000 if refresh_expires_in else None
        )
        row.updated_ms = now
        db.commit()


async def _refresh_access_token() -> str:
    if not _oauth_configured():
        raise WebexError(
            "Webex access token is invalid/expired and OAuth refresh is not configured. "
            "Set WEBEX_CLIENT_ID, WEBEX_CLIENT_SECRET, and WEBEX_REFRESH_TOKEN."
        )

    async with _token_lock:
        saved = _load_saved_token()
        now = _now_ms()
        if (
            saved
            and saved.access_token
            and saved.access_token_expires_at_ms
            and saved.access_token_expires_at_ms > now + 60_000
        ):
            return saved.access_token

        refresh_token = (saved.refresh_token if saved and saved.refresh_token else None) or REFRESH_TOKEN
        data = {
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": refresh_token,
        }
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                TOKEN_URL,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data,
            )
        if response.status_code >= 400:
            raise WebexError(
                f"Webex OAuth refresh HTTP {response.status_code}: {response.text[:3000]}"
            )
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise WebexError(f"Webex OAuth refresh did not return access_token: {payload}")
        _save_token(payload, refresh_token)
        return token


async def _get_access_token(*, force_refresh: bool = False) -> str:
    saved = _load_saved_token()
    now = _now_ms()
    if not force_refresh and saved and saved.access_token:
        if not saved.access_token_expires_at_ms or saved.access_token_expires_at_ms > now + 60_000:
            return saved.access_token

    if force_refresh or (_oauth_configured() and not ACCESS_TOKEN):
        return await _refresh_access_token()

    if ACCESS_TOKEN:
        return ACCESS_TOKEN

    if _oauth_configured():
        return await _refresh_access_token()

    raise WebexError(
        "Webex authentication is not configured. Set WEBEX_ACCESS_TOKEN, or configure "
        "WEBEX_CLIENT_ID, WEBEX_CLIENT_SECRET, and WEBEX_REFRESH_TOKEN."
    )


async def _headers(*, force_refresh: bool = False) -> dict[str, str]:
    if not SEARCH_URL:
        raise WebexError("WEBEX_SEARCH_URL must be configured")
    token = await _get_access_token(force_refresh=force_refresh)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
    }


async def _graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=120) as client:
        response = await client.post(
            SEARCH_URL,
            headers=await _headers(),
            json={"query": query, "variables": variables},
        )

        if response.status_code == 401 and _oauth_configured():
            response = await client.post(
                SEARCH_URL,
                headers=await _headers(force_refresh=True),
                json={"query": query, "variables": variables},
            )

    if response.status_code >= 400:
        raise WebexError(f"Webex HTTP {response.status_code}: {response.text[:3000]}")
    payload = response.json()
    if payload.get("error") or payload.get("errors"):
        raise WebexError(f"Webex GraphQL error: {payload}")
    return payload


def _activity_truncation_count(records: list[dict[str, Any]], *, agent: bool) -> int:
    truncated = 0
    if agent:
        for session in records:
            for channel in session.get("channelInfo") or []:
                activities = channel.get("activities") or {}
                nodes = activities.get("nodes") or []
                if int(activities.get("totalCount") or 0) > len(nodes):
                    truncated += 1
    else:
        for task in records:
            activities = task.get("activities") or {}
            nodes = activities.get("nodes") or []
            if int(activities.get("totalCount") or 0) > len(nodes):
                truncated += 1
    return truncated


async def fetch_tasks(from_ms: int, to_ms: int) -> list[dict[str, Any]]:
    """Fetch all taskDetails pages for a time window."""
    tasks: list[dict[str, Any]] = []
    cursor = "NA"
    pages = 0
    while True:
        payload = await _graphql(
            TASK_DETAILS_QUERY,
            {"from": from_ms, "to": to_ms, "cursor": cursor},
        )
        container = payload.get("data", {}).get("taskDetails", {}) or {}
        tasks.extend(container.get("tasks", []) or [])
        pages += 1
        page_info = container.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            raise WebexError("taskDetails reported hasNextPage=true without an endCursor")
        if pages > 500:
            raise WebexError("taskDetails pagination exceeded safety limit")
    return tasks


async def fetch_tasks_by_ended_time(from_ms: int, to_ms: int) -> list[dict[str, Any]]:
    """Fetch taskDetails whose endedTime falls inside the requested window."""
    tasks: list[dict[str, Any]] = []
    cursor = "NA"
    pages = 0
    while True:
        payload = await _graphql(
            TASK_DETAILS_ENDED_QUERY,
            {"from": from_ms, "to": to_ms, "cursor": cursor},
        )
        container = payload.get("data", {}).get("taskDetails", {}) or {}
        tasks.extend(container.get("tasks", []) or [])
        pages += 1
        page_info = container.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            raise WebexError("taskDetails endedTime query reported hasNextPage=true without an endCursor")
        if pages > 500:
            raise WebexError("taskDetails endedTime pagination exceeded safety limit")
    return tasks


async def fetch_agent_sessions(from_ms: int, to_ms: int) -> list[dict[str, Any]]:
    """Fetch all outer agentSession pages for a time window."""
    sessions: list[dict[str, Any]] = []
    cursor = "NA"
    pages = 0
    while True:
        payload = await _graphql(
            AGENT_SESSION_QUERY,
            {"from": from_ms, "to": to_ms, "cursor": cursor},
        )
        container = payload.get("data", {}).get("agentSession", {}) or {}
        sessions.extend(container.get("agentSessions", []) or [])
        pages += 1
        page_info = container.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            raise WebexError("agentSession reported hasNextPage=true without an endCursor")
        if pages > 500:
            raise WebexError("agentSession pagination exceeded safety limit")
    return sessions


def activity_truncation_counts(
    tasks: list[dict[str, Any]], sessions: list[dict[str, Any]]
) -> dict[str, int]:
    return {
        "task_activity_records_truncated": _activity_truncation_count(tasks, agent=False),
        "agent_activity_channels_truncated": _activity_truncation_count(sessions, agent=True),
    }

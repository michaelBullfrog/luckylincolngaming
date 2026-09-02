import os
from typing import Any

import httpx

SEARCH_URL = os.getenv("WEBEX_SEARCH_URL", "")
ACCESS_TOKEN = os.getenv("WEBEX_ACCESS_TOKEN", "")

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
        nodes { eventName duration }
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


def _headers() -> dict[str, str]:
    if not SEARCH_URL or not ACCESS_TOKEN:
        raise WebexError("WEBEX_SEARCH_URL and WEBEX_ACCESS_TOKEN must be configured")
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
    }


async def _graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=120) as client:
        response = await client.post(
            SEARCH_URL,
            headers=_headers(),
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


async def fetch_agent_sessions(from_ms: int, to_ms: int) -> list[dict[str, Any]]:
    """Fetch all outer agentSession pages for a time window.

    AAR activity lists are requested at Webex's documented maximum of 100 per
    channel. Callers can use activity_truncation_counts() to detect channels
    whose inner activity history requires a narrower import window.
    """
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

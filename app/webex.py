import os
from typing import Any
import httpx

SEARCH_URL = os.getenv("WEBEX_SEARCH_URL", "")
ACCESS_TOKEN = os.getenv("WEBEX_ACCESS_TOKEN", "")

TASK_DETAILS_QUERY = """
query($from: Long!, $to: Long!) {
  taskDetails(from: $from, to: $to) {
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
      activities {
        totalCount
        nodes { eventName duration }
      }
    }
  }
}
"""

AGENT_SESSION_QUERY = """
query($from: Long!, $to: Long!) {
  agentSession(from: $from, to: $to) {
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
    }


async def _graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            SEARCH_URL,
            headers=_headers(),
            json={"query": query, "variables": variables},
        )
    if response.status_code >= 400:
        raise WebexError(f"Webex HTTP {response.status_code}: {response.text[:2000]}")
    payload = response.json()
    if payload.get("error") or payload.get("errors"):
        raise WebexError(f"Webex GraphQL error: {payload}")
    return payload


async def fetch_tasks(from_ms: int, to_ms: int) -> list[dict[str, Any]]:
    payload = await _graphql(TASK_DETAILS_QUERY, {"from": from_ms, "to": to_ms})
    return payload.get("data", {}).get("taskDetails", {}).get("tasks", [])


async def fetch_agent_sessions(from_ms: int, to_ms: int) -> list[dict[str, Any]]:
    payload = await _graphql(AGENT_SESSION_QUERY, {"from": from_ms, "to": to_ms})
    return payload.get("data", {}).get("agentSession", {}).get("agentSessions", [])

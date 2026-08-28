from sqlalchemy import BigInteger, Boolean, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base


class Call(Base):
    __tablename__ = "calls"

    id: Mapped[int] = mapped_column(primary_key=True)
    task_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    caller_number: Mapped[str | None] = mapped_column(String(64), index=True)
    destination: Mapped[str | None] = mapped_column(String(64))
    queue_id: Mapped[str | None] = mapped_column(String(64), index=True)
    queue_name: Mapped[str | None] = mapped_column(String(255), index=True)
    created_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    ended_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    wait_ms: Mapped[int] = mapped_column(BigInteger, default=0)
    total_duration_ms: Mapped[int] = mapped_column(BigInteger, default=0)
    answered: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    agent_id: Mapped[str | None] = mapped_column(String(64), index=True)
    agent_name: Mapped[str | None] = mapped_column(String(255), index=True)
    outcome: Mapped[str] = mapped_column(String(64), default="REVIEW", index=True)
    available_agent_count: Mapped[int] = mapped_column(Integer, default=0)
    available_agent_names: Mapped[str | None] = mapped_column(Text)
    sms_sent: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    sms_sent_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    raw_json: Mapped[str | None] = mapped_column(Text)


class AgentActivity(Base):
    __tablename__ = "agent_activities"
    __table_args__ = (UniqueConstraint("activity_id", name="uq_agent_activity_id"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    activity_id: Mapped[str] = mapped_column(String(255), index=True)
    agent_id: Mapped[str] = mapped_column(String(64), index=True)
    agent_name: Mapped[str] = mapped_column(String(255), index=True)
    team_name: Mapped[str | None] = mapped_column(String(255), index=True)
    channel_type: Mapped[str | None] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(64), index=True)
    start_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    end_ms: Mapped[int] = mapped_column(BigInteger, index=True)


class SupervisorRerouteEvent(Base):
    __tablename__ = "supervisor_reroute_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    task_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    event_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    rerouted_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    supervisor_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    raw_json: Mapped[str] = mapped_column(Text)


class ClerkSmsEvent(Base):
    __tablename__ = "clerk_sms_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    event_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    message_id: Mapped[str | None] = mapped_column(String(255), index=True)
    to_number: Mapped[str | None] = mapped_column(String(64), index=True)
    from_number: Mapped[str | None] = mapped_column(String(64), index=True)
    direction: Mapped[str | None] = mapped_column(String(32), index=True)
    sent_ms: Mapped[int | None] = mapped_column(BigInteger, index=True)
    matched_task_id: Mapped[str | None] = mapped_column(String(64), index=True)
    raw_json: Mapped[str] = mapped_column(Text)

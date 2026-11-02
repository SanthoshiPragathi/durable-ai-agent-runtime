"""SQLAlchemy models. PostgreSQL is the durable source of truth for execution history.

Phase 1 tables: workflows, workflow_runs, workflow_events.
worker_leases, workflow_timers, workflow_signals and idempotency_records arrive in the
phases that need them, each with its own migration.
"""

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RunStatus(enum.StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class EventType(enum.StrEnum):
    WORKFLOW_STARTED = "WORKFLOW_STARTED"
    STEP_STARTED = "STEP_STARTED"
    STEP_COMPLETED = "STEP_COMPLETED"
    STEP_FAILED = "STEP_FAILED"
    WORKFLOW_COMPLETED = "WORKFLOW_COMPLETED"
    WORKFLOW_FAILED = "WORKFLOW_FAILED"


def _sql_list(enum_cls: type[enum.StrEnum]) -> str:
    return ", ".join(f"'{member.value}'" for member in enum_cls)


# JSONB columns store Python None as SQL NULL (not the JSON value null). A STEP_COMPLETED
# event with NULL result_data therefore means "the step returned None".
def _json_column(nullable: bool = True):
    return mapped_column(JSONB(none_as_null=True), nullable=nullable)


class Workflow(Base):
    """A registered workflow definition. (name, version) identifies the code a run used."""

    __tablename__ = "workflows"

    name: Mapped[str] = mapped_column(String(200), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workflow_name", "workflow_version"], ["workflows.name", "workflows.version"]
        ),
        CheckConstraint(f"status IN ({_sql_list(RunStatus)})", name="ck_workflow_runs_status"),
        Index("ix_workflow_runs_status", "status"),
        Index("ix_workflow_runs_created_at", "created_at"),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workflow_name: Mapped[str] = mapped_column(String(200), nullable=False)
    workflow_version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=RunStatus.PENDING.value)
    input_data: Mapped[dict[str, Any]] = _json_column(nullable=False)
    result_data: Mapped[Any] = _json_column()
    current_step: Mapped[str | None] = mapped_column(String(200))
    # Set when this run is a fork of another run (Phase 6).
    parent_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workflow_runs.run_id")
    )
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WorkflowEvent(Base):
    """Append-only execution history. A database trigger rejects UPDATE and DELETE."""

    __tablename__ = "workflow_events"
    __table_args__ = (
        UniqueConstraint("run_id", "sequence_number", name="uq_workflow_events_run_seq"),
        CheckConstraint(f"event_type IN ({_sql_list(EventType)})", name="ck_workflow_events_type"),
        # Backstop against duplicate completion events: a step completes at most once per run.
        Index(
            "uq_workflow_events_step_completed",
            "run_id",
            "step_name",
            unique=True,
            postgresql_where=text("event_type = 'STEP_COMPLETED'"),
        ),
    )

    event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workflow_runs.run_id"), nullable=False
    )
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(30), nullable=False)
    step_name: Mapped[str | None] = mapped_column(String(200))
    input_hash: Mapped[str | None] = mapped_column(String(64))
    result_data: Mapped[Any] = _json_column()
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

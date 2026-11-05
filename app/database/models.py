"""SQLAlchemy models. PostgreSQL is the durable source of truth for execution history.

Tables: workflows, workflow_runs, workflow_events (Phase 1), idempotency_records (Phase 3),
workers plus the lease columns on workflow_runs (Phase 4), workflow_timers and workflow_signals
plus the WAITING status (Phase 5).
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
    # A step with an external effect has an unknown outcome; a person must resolve it.
    NEEDS_REVIEW = "NEEDS_REVIEW"
    # Parked on ctx.sleep or ctx.wait_for_signal. A waiting run holds no lease and uses no worker;
    # a timer firing or a matching signal makes it claimable again.
    WAITING = "WAITING"


class TimerStatus(enum.StrEnum):
    PENDING = "PENDING"
    FIRED = "FIRED"
    CANCELLED = "CANCELLED"


class IdempotencyStatus(enum.StrEnum):
    IN_FLIGHT = "IN_FLIGHT"  # the call was started; no outcome recorded yet
    SUCCEEDED = "SUCCEEDED"
    RECONCILED = "RECONCILED"  # completed after checking with the external service or a person
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class EventType(enum.StrEnum):
    WORKFLOW_STARTED = "WORKFLOW_STARTED"
    WORKFLOW_RESUMED = "WORKFLOW_RESUMED"
    WORKFLOW_WAITING = "WORKFLOW_WAITING"
    STEP_STARTED = "STEP_STARTED"
    STEP_COMPLETED = "STEP_COMPLETED"
    STEP_FAILED = "STEP_FAILED"
    STEP_OUTCOME_UNKNOWN = "STEP_OUTCOME_UNKNOWN"
    STEP_RESOLVED = "STEP_RESOLVED"
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
        ForeignKeyConstraint(["workflow_name", "workflow_version"], ["workflows.name", "workflows.version"]),
        CheckConstraint(f"status IN ({_sql_list(RunStatus)})", name="ck_workflow_runs_status"),
        Index("ix_workflow_runs_status", "status"),
        Index("ix_workflow_runs_created_at", "created_at"),
        # Serves the worker's "find a claimable run" query.
        Index("ix_workflow_runs_claim", "status", "lease_expires_at", "created_at"),
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

    # Phase 4: who may execute this run right now.
    #   lease_owner       worker holding the lease (NULL: nobody; also NULL for runs executed
    #                     directly by execute_run()/resume_run(), which workers never touch)
    #   lease_expires_at  when the lease lapses unless the owner renews it (database clock)
    #   fencing_token     +1 on every claim. Every write by a worker must carry the token it was
    #                     given; a write carrying an older token is rejected (StaleLeaseError).
    # Set while status is WAITING on a signal: the signal name that wakes the run.
    waiting_signal: Mapped[str | None] = mapped_column(String(200))
    lease_owner: Mapped[str | None] = mapped_column(String(100))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    fencing_token: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default=text("0")
    )


class Worker(Base):
    """A worker process, for visibility only. Correctness never depends on this table: whether a
    run may be executed is decided by the lease on the run row."""

    __tablename__ = "workers"
    __table_args__ = (CheckConstraint("status IN ('ACTIVE', 'STOPPED')", name="ck_workers_status"),)

    worker_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    hostname: Mapped[str] = mapped_column(String(200), nullable=False)
    pid: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(10), nullable=False, default="ACTIVE")
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


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


class IdempotencyRecord(Base):
    """Tracks one external operation, keyed by its idempotency key (e.g. "<run_id>:process_refund").

    The row says what the runtime knows about the call: still in flight, succeeded, reconciled,
    failed or unknown, how many attempts were made, and the recorded result. It never claims the
    external system did or did not act; the external system's own ledger is the authority on that.
    """

    __tablename__ = "idempotency_records"
    __table_args__ = (
        CheckConstraint(f"status IN ({_sql_list(IdempotencyStatus)})", name="ck_idempotency_records_status"),
        Index("ix_idempotency_records_run_id", "run_id"),
    )

    idempotency_key: Mapped[str] = mapped_column(String(400), primary_key=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workflow_runs.run_id"), nullable=False
    )
    step_name: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    result_data: Mapped[Any] = _json_column()
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class WorkflowTimer(Base):
    """A durable timer: a ctx.sleep, or the timeout of a ctx.wait_for_signal.

    fire_at is computed once, from the database clock, when the wait is first registered, so a
    replay or a restart never moves it. One timer per (run, step).
    """

    __tablename__ = "workflow_timers"
    __table_args__ = (
        UniqueConstraint("run_id", "step_name", name="uq_workflow_timers_run_step"),
        CheckConstraint(f"status IN ({_sql_list(TimerStatus)})", name="ck_workflow_timers_status"),
        CheckConstraint("kind IN ('SLEEP', 'SIGNAL_TIMEOUT')", name="ck_workflow_timers_kind"),
        Index(
            "ix_workflow_timers_due",
            "fire_at",
            postgresql_where=text("status = 'PENDING'"),
        ),
    )

    timer_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workflow_runs.run_id"), nullable=False
    )
    step_name: Mapped[str] = mapped_column(String(200), nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    fire_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(10), nullable=False, default=TimerStatus.PENDING.value)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WorkflowSignal(Base):
    """A message sent to a run from outside (a human approval, a webhook).

    Stored the moment it arrives, even if the run is not waiting yet (an early signal is not lost).
    A wait consumes the oldest unconsumed signal with its name, once. idempotency_key makes a
    redelivered signal a no-op: unique per run.
    """

    __tablename__ = "workflow_signals"
    __table_args__ = (
        Index(
            "uq_workflow_signals_idempotency",
            "run_id",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        Index("ix_workflow_signals_run_name", "run_id", "name", "signal_id"),
    )

    signal_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workflow_runs.run_id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    payload: Mapped[Any] = _json_column()
    idempotency_key: Mapped[str | None] = mapped_column(String(200))
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    consumed_by_step: Mapped[str | None] = mapped_column(String(200))
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

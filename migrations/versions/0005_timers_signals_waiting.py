"""timers, signals, the WAITING status and the WORKFLOW_WAITING event

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-05
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None

OLD_STATUSES = "'PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'NEEDS_REVIEW'"
NEW_STATUSES = OLD_STATUSES + ", 'WAITING'"
OLD_EVENTS = (
    "'WORKFLOW_STARTED', 'WORKFLOW_RESUMED', 'STEP_STARTED', 'STEP_COMPLETED', 'STEP_FAILED', "
    "'STEP_OUTCOME_UNKNOWN', 'STEP_RESOLVED', 'WORKFLOW_COMPLETED', 'WORKFLOW_FAILED'"
)
NEW_EVENTS = OLD_EVENTS.replace("'WORKFLOW_RESUMED', ", "'WORKFLOW_RESUMED', 'WORKFLOW_WAITING', ")


def upgrade() -> None:
    op.drop_constraint("ck_workflow_runs_status", "workflow_runs", type_="check")
    op.create_check_constraint("ck_workflow_runs_status", "workflow_runs", f"status IN ({NEW_STATUSES})")
    op.drop_constraint("ck_workflow_events_type", "workflow_events", type_="check")
    op.create_check_constraint("ck_workflow_events_type", "workflow_events", f"event_type IN ({NEW_EVENTS})")
    op.add_column("workflow_runs", sa.Column("waiting_signal", sa.String(200), nullable=True))

    op.create_table(
        "workflow_timers",
        sa.Column("timer_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("step_name", sa.String(200), nullable=False),
        sa.Column("kind", sa.String(20), nullable=False),
        sa.Column("fire_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(10), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("fired_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("timer_id"),
        sa.ForeignKeyConstraint(["run_id"], ["workflow_runs.run_id"]),
        sa.UniqueConstraint("run_id", "step_name", name="uq_workflow_timers_run_step"),
        sa.CheckConstraint("status IN ('PENDING', 'FIRED', 'CANCELLED')", name="ck_workflow_timers_status"),
        sa.CheckConstraint("kind IN ('SLEEP', 'SIGNAL_TIMEOUT')", name="ck_workflow_timers_kind"),
    )
    op.create_index(
        "ix_workflow_timers_due",
        "workflow_timers",
        ["fire_at"],
        postgresql_where=sa.text("status = 'PENDING'"),
    )

    op.create_table(
        "workflow_signals",
        sa.Column("signal_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("payload", postgresql.JSONB(none_as_null=True), nullable=True),
        sa.Column("idempotency_key", sa.String(200), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("consumed_by_step", sa.String(200), nullable=True),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("signal_id"),
        sa.ForeignKeyConstraint(["run_id"], ["workflow_runs.run_id"]),
    )
    op.create_index(
        "uq_workflow_signals_idempotency",
        "workflow_signals",
        ["run_id", "idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )
    op.create_index("ix_workflow_signals_run_name", "workflow_signals", ["run_id", "name", "signal_id"])


def downgrade() -> None:
    # Fails if WAITING runs or WORKFLOW_WAITING events exist (the event log is append-only).
    op.drop_table("workflow_signals")
    op.drop_table("workflow_timers")
    op.drop_column("workflow_runs", "waiting_signal")
    op.drop_constraint("ck_workflow_events_type", "workflow_events", type_="check")
    op.create_check_constraint("ck_workflow_events_type", "workflow_events", f"event_type IN ({OLD_EVENTS})")
    op.drop_constraint("ck_workflow_runs_status", "workflow_runs", type_="check")
    op.create_check_constraint("ck_workflow_runs_status", "workflow_runs", f"status IN ({OLD_STATUSES})")

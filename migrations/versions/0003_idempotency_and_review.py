"""idempotency_records, NEEDS_REVIEW status, outcome-unknown and resolved events

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-02
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

OLD_STATUSES = "'PENDING', 'RUNNING', 'COMPLETED', 'FAILED'"
NEW_STATUSES = OLD_STATUSES + ", 'NEEDS_REVIEW'"
OLD_EVENTS = (
    "'WORKFLOW_STARTED', 'WORKFLOW_RESUMED', 'STEP_STARTED', 'STEP_COMPLETED', "
    "'STEP_FAILED', 'WORKFLOW_COMPLETED', 'WORKFLOW_FAILED'"
)
NEW_EVENTS = OLD_EVENTS.replace(
    "'STEP_FAILED', ", "'STEP_FAILED', 'STEP_OUTCOME_UNKNOWN', 'STEP_RESOLVED', "
)


def upgrade() -> None:
    op.drop_constraint("ck_workflow_runs_status", "workflow_runs", type_="check")
    op.create_check_constraint("ck_workflow_runs_status", "workflow_runs", f"status IN ({NEW_STATUSES})")
    op.drop_constraint("ck_workflow_events_type", "workflow_events", type_="check")
    op.create_check_constraint("ck_workflow_events_type", "workflow_events", f"event_type IN ({NEW_EVENTS})")

    op.create_table(
        "idempotency_records",
        sa.Column("idempotency_key", sa.String(400), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("step_name", sa.String(200), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("result_data", postgresql.JSONB(none_as_null=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("idempotency_key"),
        sa.ForeignKeyConstraint(["run_id"], ["workflow_runs.run_id"]),
        sa.CheckConstraint(
            "status IN ('IN_FLIGHT', 'SUCCEEDED', 'RECONCILED', 'FAILED', 'UNKNOWN')",
            name="ck_idempotency_records_status",
        ),
    )
    op.create_index("ix_idempotency_records_run_id", "idempotency_records", ["run_id"])


def downgrade() -> None:
    # Fails if NEEDS_REVIEW runs or the new event types exist. The event log is append-only, so
    # those rows cannot be removed; downgrade only a database that never used them.
    op.drop_table("idempotency_records")
    op.drop_constraint("ck_workflow_events_type", "workflow_events", type_="check")
    op.create_check_constraint("ck_workflow_events_type", "workflow_events", f"event_type IN ({OLD_EVENTS})")
    op.drop_constraint("ck_workflow_runs_status", "workflow_runs", type_="check")
    op.create_check_constraint("ck_workflow_runs_status", "workflow_runs", f"status IN ({OLD_STATUSES})")

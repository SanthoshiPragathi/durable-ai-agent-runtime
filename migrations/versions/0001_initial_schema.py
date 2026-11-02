"""initial schema: workflows, workflow_runs, workflow_events

Revision ID: 0001
Revises:
Create Date: 2026-11-02
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workflows",
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("name", "version"),
    )

    op.create_table(
        "workflow_runs",
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("workflow_name", sa.String(200), nullable=False),
        sa.Column("workflow_version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("input_data", postgresql.JSONB(none_as_null=True), nullable=False),
        sa.Column("result_data", postgresql.JSONB(none_as_null=True), nullable=True),
        sa.Column("current_step", sa.String(200), nullable=True),
        sa.Column("parent_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("run_id"),
        sa.ForeignKeyConstraint(
            ["workflow_name", "workflow_version"], ["workflows.name", "workflows.version"]
        ),
        sa.ForeignKeyConstraint(["parent_run_id"], ["workflow_runs.run_id"]),
        sa.CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')", name="ck_workflow_runs_status"
        ),
    )
    op.create_index("ix_workflow_runs_status", "workflow_runs", ["status"])
    op.create_index("ix_workflow_runs_created_at", "workflow_runs", ["created_at"])

    op.create_table(
        "workflow_events",
        sa.Column("event_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(30), nullable=False),
        sa.Column("step_name", sa.String(200), nullable=True),
        sa.Column("input_hash", sa.String(64), nullable=True),
        sa.Column("result_data", postgresql.JSONB(none_as_null=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("event_id"),
        sa.ForeignKeyConstraint(["run_id"], ["workflow_runs.run_id"]),
        sa.UniqueConstraint("run_id", "sequence_number", name="uq_workflow_events_run_seq"),
        sa.CheckConstraint(
            "event_type IN ('WORKFLOW_STARTED', 'STEP_STARTED', 'STEP_COMPLETED', "
            "'STEP_FAILED', 'WORKFLOW_COMPLETED', 'WORKFLOW_FAILED')",
            name="ck_workflow_events_type",
        ),
    )
    op.create_index(
        "uq_workflow_events_step_completed",
        "workflow_events",
        ["run_id", "step_name"],
        unique=True,
        postgresql_where=sa.text("event_type = 'STEP_COMPLETED'"),
    )

    # The event log is append-only. Enforce it in the database so that no bug in application
    # code (or a stray manual query) can rewrite history. TRUNCATE is deliberately not blocked
    # so tests can reset state.
    op.execute(
        """
        CREATE FUNCTION workflow_events_append_only() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'workflow_events is append-only: % is not allowed', TG_OP
                USING ERRCODE = 'restrict_violation';
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_workflow_events_append_only
        BEFORE UPDATE OR DELETE ON workflow_events
        FOR EACH ROW EXECUTE FUNCTION workflow_events_append_only()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_workflow_events_append_only ON workflow_events")
    op.execute("DROP FUNCTION IF EXISTS workflow_events_append_only()")
    op.drop_table("workflow_events")
    op.drop_table("workflow_runs")
    op.drop_table("workflows")

"""workers table, run leases and fencing tokens

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-05
"""

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("workflow_runs", sa.Column("lease_owner", sa.String(100), nullable=True))
    op.add_column("workflow_runs", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "workflow_runs",
        sa.Column("fencing_token", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
    )
    op.create_index(
        "ix_workflow_runs_claim", "workflow_runs", ["status", "lease_expires_at", "created_at"]
    )
    op.create_table(
        "workers",
        sa.Column("worker_id", sa.String(100), nullable=False),
        sa.Column("hostname", sa.String(200), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(10), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column(
            "last_heartbeat_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("worker_id"),
        sa.CheckConstraint("status IN ('ACTIVE', 'STOPPED')", name="ck_workers_status"),
    )


def downgrade() -> None:
    op.drop_table("workers")
    op.drop_index("ix_workflow_runs_claim", table_name="workflow_runs")
    op.drop_column("workflow_runs", "fencing_token")
    op.drop_column("workflow_runs", "lease_expires_at")
    op.drop_column("workflow_runs", "lease_owner")

"""allow the WORKFLOW_RESUMED event type

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-02
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

OLD_TYPES = (
    "'WORKFLOW_STARTED', 'STEP_STARTED', 'STEP_COMPLETED', "
    "'STEP_FAILED', 'WORKFLOW_COMPLETED', 'WORKFLOW_FAILED'"
)
NEW_TYPES = OLD_TYPES.replace("'WORKFLOW_STARTED', ", "'WORKFLOW_STARTED', 'WORKFLOW_RESUMED', ")


def upgrade() -> None:
    op.drop_constraint("ck_workflow_events_type", "workflow_events", type_="check")
    op.create_check_constraint(
        "ck_workflow_events_type", "workflow_events", f"event_type IN ({NEW_TYPES})"
    )


def downgrade() -> None:
    # Fails if WORKFLOW_RESUMED events exist. The event log is append-only, so they cannot be
    # deleted; downgrading past this revision is only possible on a database with no resumed runs.
    op.drop_constraint("ck_workflow_events_type", "workflow_events", type_="check")
    op.create_check_constraint(
        "ck_workflow_events_type", "workflow_events", f"event_type IN ({OLD_TYPES})"
    )

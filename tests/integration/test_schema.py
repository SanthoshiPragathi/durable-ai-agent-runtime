"""Schema-level guarantees, checked against a real PostgreSQL."""

import uuid

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import to_sync_url
from app.database.session import SessionFactory
from tests.conftest import alembic_config

pytestmark = pytest.mark.integration


async def _make_run(session: AsyncSession) -> uuid.UUID:
    run_id = uuid.uuid4()
    await session.execute(text("INSERT INTO workflows (name, version) VALUES ('wf', 1)"))
    await session.execute(
        text(
            "INSERT INTO workflow_runs (run_id, workflow_name, workflow_version, status, input_data) "
            "VALUES (:r, 'wf', 1, 'PENDING', '{}')"
        ),
        {"r": run_id},
    )
    return run_id


async def _add_event(session: AsyncSession, run_id, seq: int, event_type: str, step: str | None):
    await session.execute(
        text(
            "INSERT INTO workflow_events (run_id, sequence_number, event_type, step_name) "
            "VALUES (:r, :s, :t, :n)"
        ),
        {"r": run_id, "s": seq, "t": event_type, "n": step},
    )


async def test_event_log_rejects_update(session_factory: SessionFactory) -> None:
    async with session_factory() as session:
        async with session.begin():
            run_id = await _make_run(session)
            await _add_event(session, run_id, 1, "WORKFLOW_STARTED", None)
        with pytest.raises(DBAPIError, match="append-only"):
            async with session.begin():
                await session.execute(text("UPDATE workflow_events SET step_name = 'tampered'"))


async def test_event_log_rejects_delete(session_factory: SessionFactory) -> None:
    async with session_factory() as session:
        async with session.begin():
            run_id = await _make_run(session)
            await _add_event(session, run_id, 1, "WORKFLOW_STARTED", None)
        with pytest.raises(DBAPIError, match="append-only"):
            async with session.begin():
                await session.execute(text("DELETE FROM workflow_events"))


async def test_sequence_number_is_unique_per_run(session_factory: SessionFactory) -> None:
    async with session_factory() as session:
        async with session.begin():
            run_id = await _make_run(session)
            await _add_event(session, run_id, 1, "WORKFLOW_STARTED", None)
        with pytest.raises(IntegrityError):
            async with session.begin():
                await _add_event(session, run_id, 1, "STEP_STARTED", "a")


async def test_step_can_complete_only_once(session_factory: SessionFactory) -> None:
    async with session_factory() as session:
        async with session.begin():
            run_id = await _make_run(session)
            await _add_event(session, run_id, 1, "STEP_COMPLETED", "charge")
        with pytest.raises(IntegrityError):
            async with session.begin():
                await _add_event(session, run_id, 2, "STEP_COMPLETED", "charge")


async def test_unknown_status_and_event_type_are_rejected(session_factory: SessionFactory) -> None:
    async with session_factory() as session:
        async with session.begin():
            run_id = await _make_run(session)
        with pytest.raises(IntegrityError):
            async with session.begin():
                await _add_event(session, run_id, 1, "NOT_A_REAL_EVENT", None)
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(text("UPDATE workflow_runs SET status = 'BOGUS'"))


async def test_run_must_reference_registered_workflow(session_factory: SessionFactory) -> None:
    async with session_factory() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(
                    text(
                        "INSERT INTO workflow_runs "
                        "(run_id, workflow_name, workflow_version, status, input_data) "
                        "VALUES (:r, 'ghost', 1, 'PENDING', '{}')"
                    ),
                    {"r": uuid.uuid4()},
                )


def test_migrations_match_models(migrated_database: str) -> None:
    """Fails if the SQLAlchemy models drifted from the Alembic migrations."""
    command.check(alembic_config(to_sync_url(migrated_database)))

"""Real worker processes killed around a wait. The ledger file is the independent judge of which
step functions really ran; the event log must also stay a valid, replayable history."""

import uuid
from pathlib import Path

import pytest

from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.failpoints import CRASH_EXIT_CODE
from app.engine.signals import send_signal
from tests.fixtures.processes import start_worker

pytestmark = [pytest.mark.integration, pytest.mark.recovery, pytest.mark.concurrency]

MODULES = "tests.recovery.process_workflows"


def spawn(name: str, env: dict, **kw):
    return start_worker(name, **env, modules=MODULES, **kw)


def finish(proc, timeout: float = 90) -> int:
    proc.communicate(timeout=timeout)
    return proc.returncode


async def new_run(sf: SessionFactory) -> uuid.UUID:
    async with sf() as session, session.begin():
        repo = RunRepository(session)
        await repo.ensure_workflow("waiting_pipeline", 1)
        return (await repo.create_run("waiting_pipeline", 1, {"order_id": "A-1"})).run_id


async def state(sf: SessionFactory, run_id: uuid.UUID):
    async with sf() as session:
        run = await RunRepository(session).get_run(run_id)
        events = await EventRepository(session).list_events(run_id)
    assert run is not None
    return run, [e.event_type for e in events]


@pytest.fixture
def ledger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "ledger.txt"
    monkeypatch.setenv("DURABLE_TEST_LEDGER", str(path))
    return path


def lines(path: Path) -> list[str]:
    return path.read_text().split() if path.exists() else []


@pytest.fixture
def env(migrated_database: str, services_database_url: str):
    return {"db_url": migrated_database, "services_url": services_database_url}


async def test_a_worker_killed_after_parking_the_run_loses_nothing(
    session_factory: SessionFactory, env: dict, ledger: Path
) -> None:
    run_id = await new_run(session_factory)
    assert finish(spawn("w1", env, failpoint="after_wait_recorded:wait")) == CRASH_EXIT_CODE

    run, kinds = await state(session_factory, run_id)
    assert run.status == "WAITING" and run.lease_owner is None  # the park was committed
    await send_signal(session_factory, run_id, "go", "approved")
    assert finish(spawn("w2", env)) == 0

    run, kinds = await state(session_factory, run_id)
    assert run.status == "COMPLETED" and run.result_data["payload"] == "approved"
    assert lines(ledger) == ["fetch_order", "process_refund", "send_email"]


async def test_a_worker_killed_before_parking_is_replaced_and_parks_the_run(
    session_factory: SessionFactory, env: dict, ledger: Path
) -> None:
    run_id = await new_run(session_factory)
    assert finish(spawn("w1", env, failpoint="before_wait_recorded:wait")) == CRASH_EXIT_CODE
    run, _ = await state(session_factory, run_id)
    assert run.status == "RUNNING" and run.lease_owner == "w1"  # died holding the lease

    assert finish(spawn("w2", env)) == 0  # waits out the lease, takes over, parks, drains
    run, kinds = await state(session_factory, run_id)
    assert run.status == "WAITING"
    # fetch_order and the wait were each started once: the takeover did not start the wait again.
    assert kinds.count("STEP_STARTED") == 2 and kinds.count("WORKFLOW_WAITING") == 1

    await send_signal(session_factory, run_id, "go", "ok")
    assert finish(spawn("w3", env)) == 0
    run, _ = await state(session_factory, run_id)
    assert run.status == "COMPLETED"
    assert lines(ledger) == ["fetch_order", "process_refund", "send_email"]


async def test_a_worker_killed_right_after_consuming_the_signal_is_recovered(
    session_factory: SessionFactory, env: dict, ledger: Path
) -> None:
    run_id = await new_run(session_factory)
    await send_signal(session_factory, run_id, "go", "early")  # arrives before the wait
    assert finish(spawn("w1", env, failpoint="after_wait_recorded:wait")) == CRASH_EXIT_CODE

    assert finish(spawn("w2", env)) == 0

    run, kinds = await state(session_factory, run_id)
    assert run.status == "COMPLETED" and run.result_data["payload"] == "early"
    assert kinds.count("STEP_COMPLETED") == 4  # the wait was consumed once, not twice
    assert lines(ledger) == ["fetch_order", "process_refund", "send_email"]

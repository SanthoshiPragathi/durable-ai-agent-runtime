"""The leave-approval example, driven through both ways a wait can end.

Regression: the workflow passed its module-level outbox list to ctx.effect as an argument, and the
effect appends to it. The engine hashes effect arguments to detect non-determinism, so when ONE
process parked the run and later resumed it (a worker that is still alive when the timer fires, which is
what happens on a slow machine), the replayed call hashed differently and the run stopped with
NonDeterminismError. The demo's normal path hides it because each worker there is a fresh process.
"""

import asyncio

import pytest

import app.examples.leave_approval_workflow as leave
from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.signals import send_signal
from app.engine.worker import Worker


@pytest.fixture(autouse=True)
def empty_outbox():
    leave.OUTBOX.clear()
    yield
    leave.OUTBOX.clear()


def worker(sf: SessionFactory, name: str) -> Worker:
    return Worker(sf, name, lease_seconds=5.0, heartbeat_seconds=0.5, poll_seconds=0.02)


async def events_of(sf: SessionFactory, run_id) -> list[str]:
    async with sf() as session:
        return [e.event_type for e in await EventRepository(session).list_events(run_id)]


async def test_the_same_process_parks_then_resumes_after_the_timeout(session_factory: SessionFactory) -> None:
    """One worker object (one process, one outbox) parks the run and, once the timer is due, finishes it."""
    run = await runtime.create_run(
        session_factory,
        "leave_approval",
        {"employee": "john", "manager": "meena", "days": 1, "timeout_seconds": 0.3},
    )
    w = worker(session_factory, "w-same-process")
    await w.run_until_idle()
    async with session_factory() as session:
        assert (await RunRepository(session).get_run(run.run_id)).status == "WAITING"
    assert [m["to"] for m in leave.OUTBOX] == ["meena"]  # the first execution already filled the outbox

    await asyncio.sleep(0.4)
    await w.run_until_idle()  # fires the timer, wakes the run, replays, escalates, notifies

    async with session_factory() as session:
        done = await RunRepository(session).get_run(run.run_id)
    assert done.status == "COMPLETED", (done.status, await events_of(session_factory, run.run_id))
    assert done.result_data["outcome"] == "auto_rejected"
    assert [m["to"] for m in leave.OUTBOX] == ["meena", "meena-boss", "john"]  # each sent exactly once
    kinds = await events_of(session_factory, run.run_id)
    assert kinds.count("WORKFLOW_RESUMED") == 1 and kinds.count("WORKFLOW_COMPLETED") == 1


async def test_the_same_process_parks_then_resumes_after_a_signal(session_factory: SessionFactory) -> None:
    run = await runtime.create_run(
        session_factory, "leave_approval", {"employee": "asha", "manager": "ravi", "days": 3}
    )
    w = worker(session_factory, "w-same-process")
    await w.run_until_idle()
    await send_signal(session_factory, run.run_id, "manager_decision", {"approved": True})
    await w.run_until_idle()
    async with session_factory() as session:
        done = await RunRepository(session).get_run(run.run_id)
    assert done.status == "COMPLETED" and done.result_data["outcome"] == "approved"
    assert [m["to"] for m in leave.OUTBOX] == ["ravi", "asha"]

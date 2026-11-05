"""Timers and signals end to end against PostgreSQL, driven by workers in the same process."""

import uuid

import pytest
from sqlalchemy import text

from app.database.repositories import EventRepository, RunRepository, WaitRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.errors import InvalidRunStateError, RunNotFoundError
from app.engine.signals import send_signal
from app.engine.timers import fire_due_timers
from app.engine.worker import Worker
from app.sdk import workflow

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("isolated_registry")]


def make_worker(sf: SessionFactory, name: str = "w1") -> Worker:
    return Worker(sf, name, lease_seconds=30.0, heartbeat_seconds=10.0, poll_seconds=0.05)


async def run_of(sf: SessionFactory, run_id: uuid.UUID):
    async with sf() as session:
        run = await RunRepository(session).get_run(run_id)
        assert run is not None
        return run


async def events_of(sf: SessionFactory, run_id: uuid.UUID) -> list[str]:
    async with sf() as session:
        return [e.event_type for e in await EventRepository(session).list_events(run_id)]


async def make_timers_due(sf: SessionFactory, run_id: uuid.UUID) -> None:
    async with sf() as session, session.begin():
        await session.execute(
            text("UPDATE workflow_timers SET fire_at = now() - interval '1 second' WHERE run_id=:r"),
            {"r": run_id},
        )


def register_approval(calls: list[str]) -> None:
    @workflow
    async def approval(ctx, timeout: float | None = None):
        await ctx.step("before", lambda: calls.append("before") or 1)
        answer = await ctx.wait_for_signal("wait", signal="decision", timeout=timeout)
        await ctx.step("after", lambda: calls.append("after") or 2)
        return answer


async def start_until_waiting(sf: SessionFactory, name: str, data: dict) -> uuid.UUID:
    run = await runtime.create_run(sf, name, data)
    assert await make_worker(sf).run_until_idle() == 1
    return run.run_id


async def test_a_run_parks_as_waiting_and_holds_no_lease(session_factory: SessionFactory) -> None:
    calls: list[str] = []
    register_approval(calls)
    run_id = await start_until_waiting(session_factory, "approval", {})

    run = await run_of(session_factory, run_id)
    assert run.status == "WAITING"
    assert run.waiting_signal == "decision"
    assert run.lease_owner is None and run.lease_expires_at is None
    assert calls == ["before"]
    assert (await events_of(session_factory, run_id))[-1] == "WORKFLOW_WAITING"
    assert await make_worker(session_factory, "w2").run_until_idle() == 0  # nothing to claim


async def test_a_signal_wakes_the_run_and_another_worker_finishes_it(session_factory) -> None:
    calls: list[str] = []
    register_approval(calls)
    run_id = await start_until_waiting(session_factory, "approval", {})

    receipt = await send_signal(session_factory, run_id, "decision", {"approved": True})
    assert receipt.woke_run and not receipt.duplicate
    assert await make_worker(session_factory, "w2").run_until_idle() == 1

    run = await run_of(session_factory, run_id)
    assert run.status == "COMPLETED"
    assert run.result_data["payload"] == {"approved": True}
    assert calls == ["before", "after"]  # "before" was replayed, not run again
    kinds = await events_of(session_factory, run_id)
    assert kinds.count("WORKFLOW_STARTED") == 1 and kinds.count("WORKFLOW_RESUMED") == 1


async def test_a_signal_sent_before_the_wait_is_not_lost(session_factory) -> None:
    calls: list[str] = []
    register_approval(calls)
    run = await runtime.create_run(session_factory, "approval", {})
    receipt = await send_signal(session_factory, run.run_id, "decision", "early")
    assert not receipt.woke_run

    await make_worker(session_factory).run_until_idle()

    final = await run_of(session_factory, run.run_id)
    assert final.status == "COMPLETED" and final.result_data["payload"] == "early"
    assert "WORKFLOW_WAITING" not in await events_of(session_factory, run.run_id)


async def test_a_duplicate_idempotency_key_is_ignored(session_factory) -> None:
    register_approval([])
    run_id = await start_until_waiting(session_factory, "approval", {})

    first = await send_signal(session_factory, run_id, "decision", "a", idempotency_key="k1")
    second = await send_signal(session_factory, run_id, "decision", "b", idempotency_key="k1")

    assert first.signal_id == second.signal_id and second.duplicate and not second.woke_run
    async with session_factory() as session:
        assert len(await WaitRepository(session).list_signals(run_id)) == 1


async def test_signals_are_consumed_oldest_first_and_once(session_factory) -> None:
    @workflow
    async def two_waits(ctx):
        a = await ctx.wait_for_signal("w1", signal="s")
        b = await ctx.wait_for_signal("w2", signal="s")
        return [a["payload"], b["payload"]]

    run = await runtime.create_run(session_factory, "two_waits", {})
    for payload in ("first", "second"):
        await send_signal(session_factory, run.run_id, "s", payload)
    await make_worker(session_factory).run_until_idle()

    assert (await run_of(session_factory, run.run_id)).result_data == ["first", "second"]
    async with session_factory() as session:
        signals = await WaitRepository(session).list_signals(run.run_id)
    assert [(s.consumed_by_step) for s in signals] == ["w1", "w2"]


async def test_a_signal_with_another_name_does_not_wake_the_run(session_factory) -> None:
    register_approval([])
    run_id = await start_until_waiting(session_factory, "approval", {})

    receipt = await send_signal(session_factory, run_id, "something_else", 1)

    assert not receipt.woke_run
    assert (await run_of(session_factory, run_id)).status == "WAITING"


async def test_signals_to_finished_or_unknown_runs_are_rejected(session_factory) -> None:
    @workflow
    async def quick(ctx):
        return 1

    run = await runtime.create_run(session_factory, "quick", {})
    await make_worker(session_factory).run_until_idle()

    with pytest.raises(InvalidRunStateError):
        await send_signal(session_factory, run.run_id, "x", None)
    with pytest.raises(RunNotFoundError):
        await send_signal(session_factory, uuid.uuid4(), "x", None)


async def test_the_timeout_fires_and_the_workflow_sees_timed_out(session_factory) -> None:
    register_approval([])
    run_id = await start_until_waiting(session_factory, "approval", {"timeout": 3600})
    assert await fire_due_timers(session_factory) == 0  # not due yet

    await make_timers_due(session_factory, run_id)
    assert await make_worker(session_factory).run_until_idle() == 1  # run_once fires the timer first

    run = await run_of(session_factory, run_id)
    assert run.status == "COMPLETED"
    assert run.result_data == {"received": False, "timed_out": True}


async def test_a_stored_signal_beats_a_due_timeout(session_factory) -> None:
    register_approval([])
    run_id = await start_until_waiting(session_factory, "approval", {"timeout": 3600})
    await send_signal(session_factory, run_id, "decision", "in time")
    await make_timers_due(session_factory, run_id)  # the timeout is also due when the run replays

    await make_worker(session_factory).run_until_idle()

    result = (await run_of(session_factory, run_id)).result_data
    assert result["received"] is True and result["payload"] == "in time"


async def test_consuming_a_signal_cancels_the_timeout_timer(session_factory) -> None:
    register_approval([])
    run_id = await start_until_waiting(session_factory, "approval", {"timeout": 3600})
    await send_signal(session_factory, run_id, "decision", "in time")

    await make_worker(session_factory).run_until_idle()

    async with session_factory() as session:
        (timer,) = await WaitRepository(session).list_timers(run_id)
    assert timer.status == "CANCELLED"


async def test_sleep_parks_then_continues_after_the_timer(session_factory) -> None:
    calls: list[str] = []

    @workflow
    async def napper(ctx):
        await ctx.step("a", lambda: calls.append("a") or 1)
        await ctx.sleep("nap", 3600)
        await ctx.step("b", lambda: calls.append("b") or 2)
        return "done"

    run = await runtime.create_run(session_factory, "napper", {})
    await make_worker(session_factory).run_until_idle()
    assert (await run_of(session_factory, run.run_id)).status == "WAITING"
    assert await make_worker(session_factory, "w2").run_until_idle() == 0

    await make_timers_due(session_factory, run.run_id)
    assert await make_worker(session_factory, "w2").run_until_idle() == 1

    assert (await run_of(session_factory, run.run_id)).status == "COMPLETED"
    assert calls == ["a", "b"]
    async with session_factory() as session:
        (timer,) = await WaitRepository(session).list_timers(run.run_id)
    assert timer.status == "FIRED" and timer.fired_at is not None


async def test_the_deadline_is_fixed_when_the_wait_is_first_registered(session_factory) -> None:
    @workflow
    async def napper(ctx):
        await ctx.sleep("nap", 3600)

    run = await runtime.create_run(session_factory, "napper", {})
    await make_worker(session_factory).run_until_idle()
    async with session_factory() as session:
        (before,) = await WaitRepository(session).list_timers(run.run_id)
    # A spurious wake: replay evaluates the wait again but must not move the deadline.
    async with session_factory() as session, session.begin():
        await RunRepository(session).wake(run.run_id)
    await make_worker(session_factory, "w2").run_until_idle()
    async with session_factory() as session:
        (after,) = await WaitRepository(session).list_timers(run.run_id)
        status = (await RunRepository(session).get_run(run.run_id)).status
    assert after.fire_at == before.fire_at and after.timer_id == before.timer_id
    assert status == "WAITING"
    kinds = await events_of(session_factory, run.run_id)
    assert kinds.count("WORKFLOW_WAITING") == 2 and kinds.count("STEP_STARTED") == 1


async def test_a_directly_executed_run_can_wait_and_be_resumed(session_factory) -> None:
    register_approval([])
    run = await runtime.create_run(session_factory, "approval", {})
    parked = await runtime.execute_run(session_factory, run.run_id)
    assert parked.status == "WAITING"

    await send_signal(session_factory, run.run_id, "decision", "ok")
    done = await make_worker(session_factory).run_until_idle()
    assert done == 1
    assert (await run_of(session_factory, run.run_id)).status == "COMPLETED"


async def test_a_draining_worker_waits_for_sleeping_runs_but_not_for_signals(session_factory) -> None:
    register_approval([])

    @workflow
    async def napper(ctx):
        await ctx.sleep("nap", 3600)

    await runtime.create_run(session_factory, "approval", {})
    await runtime.create_run(session_factory, "napper", {})
    worker = make_worker(session_factory)
    await worker.run_until_idle()
    assert await worker._drained() is False  # the napper's timer is pending
    async with session_factory() as session, session.begin():
        await session.execute(text("UPDATE workflow_timers SET status='CANCELLED'"))
    assert await worker._drained() is True  # only the signal wait remains: no worker can advance it

"""Races around waiting: timers fired by many workers, and signals racing the run's suspension."""

import asyncio
import uuid

import pytest
from sqlalchemy import text

from app.database.repositories import EventRepository, RunRepository, WaitRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.signals import send_signal
from app.engine.timers import fire_due_timers
from app.engine.worker import Worker
from app.sdk import workflow

pytestmark = [pytest.mark.integration, pytest.mark.concurrency, pytest.mark.usefixtures("isolated_registry")]


def make_worker(sf: SessionFactory, name: str) -> Worker:
    return Worker(sf, name, lease_seconds=30.0, heartbeat_seconds=10.0, poll_seconds=0.02)


async def test_many_firers_fire_each_timer_exactly_once(session_factory: SessionFactory) -> None:
    @workflow
    async def napper(ctx, n: int):
        await ctx.sleep("nap", 3600)
        return n

    runs = [await runtime.create_run(session_factory, "napper", {"n": i}) for i in range(8)]
    await make_worker(session_factory, "w0").run_until_idle()
    async with session_factory() as session, session.begin():
        await session.execute(text("UPDATE workflow_timers SET fire_at = now() - interval '1 second'"))

    fired = await asyncio.gather(*[fire_due_timers(session_factory) for _ in range(12)])

    assert sum(fired) == 8  # each timer fired once, by exactly one of the 12 callers
    async with session_factory() as session:
        for run in runs:
            got = await RunRepository(session).get_run(run.run_id)
            assert got is not None and got.status == "RUNNING" and got.lease_owner is None


async def test_a_signal_racing_the_suspension_is_never_lost(session_factory: SessionFactory) -> None:
    """The signal and the run's suspension race 30 times with random offsets. Whichever order the
    database picks, the run must end COMPLETED with the signal consumed exactly once."""

    @workflow
    async def waiter(ctx, i: int):
        got = await ctx.wait_for_signal("w", signal="go")
        return {"i": i, "payload": got["payload"]}

    for i in range(30):
        run = await runtime.create_run(session_factory, "waiter", {"i": i})
        worker = make_worker(session_factory, f"w{i}")

        async def deliver(
            run_id: uuid.UUID = run.run_id, delay: float = (i % 6) * 0.004, payload: int = i
        ) -> None:
            await asyncio.sleep(delay)
            await send_signal(session_factory, run_id, "go", payload)

        await asyncio.gather(worker.run_until_idle(max_runs=1), deliver())
        # If the signal landed while the run was RUNNING and before its wait, it is stored; if after
        # the suspension it woke the run. Either way one more pass finishes it.
        await worker.run_until_idle()

        async with session_factory() as session:
            final = await RunRepository(session).get_run(run.run_id)
            assert final is not None and final.status == "COMPLETED", f"iteration {i}: {final.status}"
            assert final.result_data == {"i": i, "payload": i}
            signals = await WaitRepository(session).list_signals(run.run_id)
            assert len(signals) == 1 and signals[0].consumed_by_step == "w"
            kinds = [e.event_type for e in await EventRepository(session).list_events(run.run_id)]
            assert kinds.count("STEP_COMPLETED") == 1

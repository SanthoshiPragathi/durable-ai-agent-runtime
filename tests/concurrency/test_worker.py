"""Worker behaviour in one process: normal execution, heartbeat, takeover and the zombie case."""

import asyncio
import uuid

import pytest
from sqlalchemy import text

from app.database.repositories import EventRepository, RunRepository, WorkerRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.worker import Worker
from app.sdk import workflow

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("isolated_registry")]


def make_worker(session_factory: SessionFactory, name: str, **kw) -> Worker:
    kw.setdefault("lease_seconds", 30.0)
    kw.setdefault("heartbeat_seconds", 10.0)
    kw.setdefault("poll_seconds", 0.05)
    return Worker(session_factory, name, **kw)


async def _events(session_factory: SessionFactory, run_id: uuid.UUID):
    async with session_factory() as session:
        return await EventRepository(session).list_events(run_id)


async def _run(session_factory: SessionFactory, run_id: uuid.UUID):
    async with session_factory() as session:
        run = await RunRepository(session).get_run(run_id)
        assert run is not None
        return run


async def _expire(session_factory: SessionFactory, run_id: uuid.UUID) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(
            text("UPDATE workflow_runs SET lease_expires_at = now() - interval '1 second' WHERE run_id=:r"),
            {"r": run_id},
        )


def _register_two_step() -> None:
    @workflow
    async def two_step(ctx, n: int):
        a = await ctx.step("first", lambda: n + 1)
        b = await ctx.step("second", lambda: a * 2)
        return {"n": n, "result": b}


async def test_a_worker_executes_a_pending_run_to_completion(session_factory: SessionFactory) -> None:
    _register_two_step()
    run = await runtime.create_run(session_factory, "two_step", {"n": 4})

    worker = make_worker(session_factory, "w1")
    assert await worker.run_until_idle() == 1

    final = await _run(session_factory, run.run_id)
    assert final.status == "COMPLETED" and final.result_data == {"n": 4, "result": 10}
    assert final.lease_owner is None and final.lease_expires_at is None  # released
    assert final.fencing_token == 1
    events = await _events(session_factory, run.run_id)
    assert [e.event_type for e in events] == [
        "WORKFLOW_STARTED",
        "STEP_STARTED",
        "STEP_COMPLETED",
        "STEP_STARTED",
        "STEP_COMPLETED",
        "WORKFLOW_COMPLETED",
    ]
    assert events[0].result_data == {"worker_id": "w1", "fencing_token": 1}


async def test_a_failing_workflow_is_recorded_failed_and_the_lease_released(
    session_factory: SessionFactory,
) -> None:
    @workflow
    async def boom(ctx):
        return await ctx.step("explode", lambda: 1 / 0)

    run = await runtime.create_run(session_factory, "boom", {})
    await make_worker(session_factory, "w1").run_until_idle()

    final = await _run(session_factory, run.run_id)
    assert final.status == "FAILED" and "ZeroDivisionError" in (final.error_message or "")
    assert final.lease_owner is None


async def test_several_workers_share_the_runs_and_each_runs_exactly_once(
    session_factory: SessionFactory,
) -> None:
    _register_two_step()
    ids = [(await runtime.create_run(session_factory, "two_step", {"n": i})).run_id for i in range(24)]

    workers = [make_worker(session_factory, f"w{i}") for i in range(6)]
    counts = await asyncio.gather(*(w.run_until_idle() for w in workers))

    assert sum(counts) == 24
    for run_id in ids:
        final = await _run(session_factory, run_id)
        assert final.status == "COMPLETED" and final.fencing_token == 1
        types = [e.event_type for e in await _events(session_factory, run_id)]
        assert types.count("WORKFLOW_STARTED") == 1 and types.count("STEP_COMPLETED") == 2


async def test_workers_register_and_deregister(session_factory: SessionFactory) -> None:
    worker = make_worker(session_factory, "w1")
    await worker.run_forever(idle_exit_seconds=0.2)

    async with session_factory() as session:
        (row,) = await WorkerRepository(session).list_workers()
    assert (row.worker_id, row.status) == ("w1", "STOPPED")


async def test_the_heartbeat_keeps_a_slow_run_from_being_taken_over(session_factory: SessionFactory) -> None:
    @workflow
    async def slow(ctx):
        await ctx.step("think", lambda: asyncio.sleep(1.6))
        return "done"

    run = await runtime.create_run(session_factory, "slow", {})
    # The lease (0.6 s) is much shorter than the step (1.6 s): only renewals keep it alive.
    owner = make_worker(session_factory, "owner", lease_seconds=0.6, heartbeat_seconds=0.15)
    rival = make_worker(session_factory, "rival", lease_seconds=0.6, heartbeat_seconds=0.15)

    async def rival_polls() -> int:
        stolen = 0
        for _ in range(20):
            await asyncio.sleep(0.1)
            if await rival.claim_one() is not None:
                stolen += 1
        return stolen

    _, stolen = await asyncio.gather(owner.run_until_idle(), rival_polls())

    assert stolen == 0
    final = await _run(session_factory, run.run_id)
    assert final.status == "COMPLETED" and final.fencing_token == 1


async def test_a_replacement_worker_resumes_the_run_and_completed_steps_do_not_rerun(
    session_factory: SessionFactory,
) -> None:
    calls: list[str] = []

    @workflow
    async def traced(ctx):
        await ctx.step("a", lambda: calls.append("a") or 1)
        await ctx.step("b", lambda: calls.append("b") or 2)
        return "ok"

    run = await runtime.create_run(session_factory, "traced", {})
    # w1 claims the run and records step "a", then "dies" (we simply stop using it).
    w1 = make_worker(session_factory, "w1")
    claim = await w1.claim_one()
    assert claim is not None
    history = await w1._begin(claim)
    from app.engine.execution_context import DbStepRecorder
    from app.engine.hashing import hash_step_input

    recorder = DbStepRecorder(run.run_id, session_factory, claim.fence)
    await recorder.step_started("a", hash_step_input((), {}))
    await recorder.step_completed("a", 1)
    assert len(history) == 0
    await _expire(session_factory, run.run_id)

    w2 = make_worker(session_factory, "w2")
    assert await w2.run_until_idle() == 1

    final = await _run(session_factory, run.run_id)
    assert final.status == "COMPLETED" and final.fencing_token == 2
    assert calls == ["b"]  # "a" came from the history
    events = await _events(session_factory, run.run_id)
    resumed = [e for e in events if e.event_type == "WORKFLOW_RESUMED"]
    assert len(resumed) == 1
    assert resumed[0].result_data["worker_id"] == "w2" and resumed[0].result_data["fencing_token"] == 2
    assert resumed[0].result_data["completed"] == 1


async def _zombie_scenario(session_factory: SessionFactory, *, zombie_heartbeat: float):
    """w1 starts a run and is stuck inside a step; w2 takes the run over and finishes it; then w1's
    step finally returns. Returns (zombie_outcome, zombie_worker, run_id)."""
    entered, release = asyncio.Event(), asyncio.Event()
    side_effects: list[str] = []

    async def stuck_then_work() -> str:
        side_effects.append("ran")
        entered.set()
        await release.wait()
        return "late"

    attempts = {"n": 0}

    @workflow
    async def zombie_target(ctx):
        attempts["n"] += 1
        if attempts["n"] == 1:
            value = await ctx.step("work", stuck_then_work)  # the zombie's attempt
        else:
            value = await ctx.step("work", lambda: "from-w2")  # the replacement's attempt
        await ctx.step("after", lambda: "tail")
        return value

    run = await runtime.create_run(session_factory, "zombie_target", {})
    w1 = make_worker(session_factory, "w1", lease_seconds=30.0, heartbeat_seconds=zombie_heartbeat)
    zombie_task = asyncio.create_task(w1.run_until_idle())
    await asyncio.wait_for(entered.wait(), timeout=5)

    await _expire(session_factory, run.run_id)  # w1 is "frozen" past its lease
    w2 = make_worker(session_factory, "w2")
    assert await w2.run_until_idle() == 1
    assert (await _run(session_factory, run.run_id)).status == "COMPLETED"

    release.set()  # w1 wakes up and tries to carry on
    await asyncio.wait_for(zombie_task, timeout=5)
    return w1, run.run_id


async def test_a_zombie_worker_cannot_write_after_takeover_fencing_only(
    session_factory: SessionFactory,
) -> None:
    """The zombie never notices (its heartbeat is effectively off); the database alone stops it."""
    w1, run_id = await _zombie_scenario(session_factory, zombie_heartbeat=25.0)

    assert w1.lost_leases == 1 and w1.completed == 0
    final = await _run(session_factory, run_id)
    assert final.status == "COMPLETED" and final.result_data == "from-w2" and final.fencing_token == 2
    events = await _events(session_factory, run_id)
    types = [e.event_type for e in events]
    assert types.count("STEP_COMPLETED") == 2  # work + after, each exactly once
    assert types.count("WORKFLOW_COMPLETED") == 1
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1))


async def test_a_zombie_stands_down_when_its_heartbeat_finds_the_lease_gone(
    session_factory: SessionFactory,
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    finished_step = {"value": False}

    async def long_step() -> str:
        entered.set()
        await release.wait()
        finished_step["value"] = True
        return "x"

    @workflow
    async def hb_target(ctx):
        return await ctx.step("long", long_step)

    run = await runtime.create_run(session_factory, "hb_target", {})
    w1 = make_worker(session_factory, "w1", lease_seconds=30.0, heartbeat_seconds=0.1)
    task = asyncio.create_task(w1.run_until_idle())
    await asyncio.wait_for(entered.wait(), timeout=5)

    # Someone else takes the lease; w1's next heartbeat must notice and cancel the execution.
    await _expire(session_factory, run.run_id)
    async with session_factory() as session, session.begin():
        claim = await RunRepository(session).claim_next(
            "w2", lease_seconds=30, workflows=[("hb_target", 1)]
        )
    assert claim is not None

    await asyncio.wait_for(task, timeout=5)
    assert w1.lost_leases == 1
    assert finished_step["value"] is False  # cancelled before it could finish
    assert [e.event_type for e in await _events(session_factory, run.run_id)] == [
        "WORKFLOW_STARTED",
        "STEP_STARTED",
    ]  # only what w1 had committed before losing the lease


async def test_a_workflow_that_swallows_the_stale_lease_error_still_cannot_continue(
    session_factory: SessionFactory,
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    wrote_after = {"value": False}

    async def hold() -> str:
        entered.set()
        await release.wait()
        return "held"

    @workflow
    async def stubborn(ctx):
        try:
            await ctx.step("hold", hold)
        except Exception:  # a careless workflow author swallows everything
            pass
        await ctx.step("after_swallow", lambda: wrote_after.update(value=True))
        return "carried on"

    run = await runtime.create_run(session_factory, "stubborn", {})
    w1 = make_worker(session_factory, "w1", heartbeat_seconds=25.0)
    task = asyncio.create_task(w1.run_until_idle())
    await asyncio.wait_for(entered.wait(), timeout=5)
    await _expire(session_factory, run.run_id)
    async with session_factory() as session, session.begin():
        await RunRepository(session).claim_next("w2", lease_seconds=30, workflows=[("stubborn", 1)])
    release.set()
    await asyncio.wait_for(task, timeout=5)

    assert w1.lost_leases == 1 and wrote_after["value"] is False
    types = [e.event_type for e in await _events(session_factory, run.run_id)]
    assert "WORKFLOW_COMPLETED" not in types and "STEP_COMPLETED" not in types


async def test_a_worker_does_not_start_a_step_after_it_lost_the_lease(
    session_factory: SessionFactory,
) -> None:
    """ensure_lease() runs before each step function, so a stale worker does not act at all."""
    ran: list[str] = []
    entered, release = asyncio.Event(), asyncio.Event()

    async def first() -> str:
        entered.set()
        await release.wait()
        return "1"

    @workflow
    async def two(ctx):
        await ctx.step("one", first)
        await ctx.step("two", lambda: ran.append("two"))
        return "done"

    run = await runtime.create_run(session_factory, "two", {})
    w1 = make_worker(session_factory, "w1", heartbeat_seconds=25.0)
    task = asyncio.create_task(w1.run_until_idle())
    await asyncio.wait_for(entered.wait(), timeout=5)
    await _expire(session_factory, run.run_id)
    async with session_factory() as session, session.begin():
        await RunRepository(session).claim_next("w2", lease_seconds=30, workflows=[("two", 1)])
    release.set()
    await asyncio.wait_for(task, timeout=5)

    assert ran == []  # step "two"'s function never ran on the zombie
    assert w1.lost_leases == 1

"""Claiming, leases and fencing at the database level (no workflow execution)."""

import asyncio
import uuid

import pytest
from sqlalchemy import text

from app.database.models import EventType
from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.errors import InvalidRunStateError, StaleLeaseError
from app.engine.lease import Fence
from app.sdk import workflow

pytestmark = [pytest.mark.integration, pytest.mark.concurrency, pytest.mark.usefixtures("isolated_registry")]

KNOWN = [("noop", 1)]


@pytest.fixture(autouse=True)
def _noop_workflow(isolated_registry) -> None:
    @workflow
    async def noop(ctx, n: int = 0):
        return await ctx.step("only", lambda: n)


async def _new_runs(session_factory: SessionFactory, count: int) -> list[uuid.UUID]:
    return [(await runtime.create_run(session_factory, "noop", {"n": i})).run_id for i in range(count)]


async def _claim(session_factory: SessionFactory, worker: str, lease: float = 30.0) -> dict | None:
    async with session_factory() as session, session.begin():
        return await RunRepository(session).claim_next(worker, lease_seconds=lease, workflows=KNOWN)


async def _expire(session_factory: SessionFactory, run_id: uuid.UUID) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(
            text("UPDATE workflow_runs SET lease_expires_at = now() - interval '1 second' WHERE run_id = :r"),
            {"r": run_id},
        )


async def _get(session_factory: SessionFactory, run_id: uuid.UUID):
    async with session_factory() as session:
        run = await RunRepository(session).get_run(run_id)
        assert run is not None
        return run


async def test_claim_takes_a_pending_run_and_sets_lease_and_token(session_factory: SessionFactory) -> None:
    (run_id,) = await _new_runs(session_factory, 1)

    claim = await _claim(session_factory, "w1")

    assert claim is not None
    assert claim["run_id"] == run_id and claim["fencing_token"] == 1 and claim["resumed"] is False
    run = await _get(session_factory, run_id)
    assert (run.status, run.lease_owner, run.fencing_token) == ("RUNNING", "w1", 1)
    assert run.lease_expires_at is not None and run.started_at is not None


async def test_nothing_to_claim_returns_none(session_factory: SessionFactory) -> None:
    assert await _claim(session_factory, "w1") is None


async def test_a_run_with_a_live_lease_cannot_be_claimed(session_factory: SessionFactory) -> None:
    await _new_runs(session_factory, 1)
    assert await _claim(session_factory, "w1") is not None
    assert await _claim(session_factory, "w2") is None


async def test_an_expired_lease_is_taken_over_with_a_higher_token(session_factory: SessionFactory) -> None:
    (run_id,) = await _new_runs(session_factory, 1)
    await _claim(session_factory, "w1")
    await _expire(session_factory, run_id)

    claim = await _claim(session_factory, "w2")

    assert claim is not None and claim["resumed"] is True and claim["fencing_token"] == 2
    run = await _get(session_factory, run_id)
    assert (run.lease_owner, run.fencing_token) == ("w2", 2)


async def test_only_registered_workflows_are_claimed(session_factory: SessionFactory) -> None:
    await _new_runs(session_factory, 1)
    async with session_factory() as session, session.begin():
        other = await RunRepository(session).claim_next(
            "w1", lease_seconds=30, workflows=[("some_other_workflow", 1)]
        )
        none_known = await RunRepository(session).claim_next("w1", lease_seconds=30, workflows=[])
    assert other is None and none_known is None
    assert await _claim(session_factory, "w1") is not None


async def test_finished_and_review_runs_are_never_claimed(session_factory: SessionFactory) -> None:
    done, review = await _new_runs(session_factory, 2)
    async with session_factory() as session, session.begin():
        await session.execute(
            text("UPDATE workflow_runs SET status='COMPLETED' WHERE run_id=:r"), {"r": done}
        )
        await session.execute(
            text("UPDATE workflow_runs SET status='NEEDS_REVIEW' WHERE run_id=:r"), {"r": review}
        )
    assert await _claim(session_factory, "w1") is None


async def test_directly_executed_runs_without_a_lease_are_left_alone(session_factory: SessionFactory) -> None:
    (run_id,) = await _new_runs(session_factory, 1)
    async with session_factory() as session, session.begin():
        assert await RunRepository(session).mark_running(run_id)  # execute_run() path: no lease
    assert await _claim(session_factory, "w1") is None


async def test_runs_are_claimed_oldest_first(session_factory: SessionFactory) -> None:
    ids = await _new_runs(session_factory, 3)
    claimed = [(await _claim(session_factory, "w1"))["run_id"] for _ in range(3)]
    assert claimed == ids


async def test_concurrent_claimers_never_receive_the_same_run(session_factory: SessionFactory) -> None:
    """10 claimers race for 40 runs. SKIP LOCKED means each run goes to exactly one of them."""
    ids = set(await _new_runs(session_factory, 40))

    async def claimer(name: str) -> list[uuid.UUID]:
        got = []
        while (claim := await _claim(session_factory, name)) is not None:
            got.append(claim["run_id"])
        return got

    results = await asyncio.gather(*(claimer(f"w{i}") for i in range(10)))
    all_claimed = [rid for got in results for rid in got]

    assert len(all_claimed) == len(set(all_claimed)) == 40
    assert set(all_claimed) == ids
    assert sum(1 for got in results if got) >= 2  # work really was shared


async def test_a_run_locked_by_an_in_flight_write_is_skipped_not_waited_for(
    session_factory: SessionFactory,
) -> None:
    (run_id,) = await _new_runs(session_factory, 1)
    async with session_factory() as holder, holder.begin():
        await holder.execute(text("SELECT 1 FROM workflow_runs WHERE run_id=:r FOR UPDATE"), {"r": run_id})
        assert await asyncio.wait_for(_claim(session_factory, "w1"), timeout=5) is None
    assert await _claim(session_factory, "w1") is not None  # lock released: claimable now


# ---- renewing -----------------------------------------------------------------------------


async def test_renew_extends_the_lease_for_the_holder_only(session_factory: SessionFactory) -> None:
    (run_id,) = await _new_runs(session_factory, 1)
    claim = await _claim(session_factory, "w1", lease=5)
    before = (await _get(session_factory, run_id)).lease_expires_at
    fence = Fence("w1", claim["fencing_token"])

    async with session_factory() as session, session.begin():
        assert await RunRepository(session).renew_lease(run_id, fence, 60)
        assert not await RunRepository(session).renew_lease(run_id, Fence("w2", fence.token), 60)
        assert not await RunRepository(session).renew_lease(run_id, Fence("w1", fence.token + 1), 60)

    assert (await _get(session_factory, run_id)).lease_expires_at > before


async def test_a_worker_whose_lease_was_taken_cannot_renew(session_factory: SessionFactory) -> None:
    (run_id,) = await _new_runs(session_factory, 1)
    old = await _claim(session_factory, "w1")
    await _expire(session_factory, run_id)
    await _claim(session_factory, "w2")

    async with session_factory() as session, session.begin():
        assert not await RunRepository(session).renew_lease(run_id, Fence("w1", old["fencing_token"]), 30)


# ---- fencing ------------------------------------------------------------------------------


async def _append(session_factory: SessionFactory, run_id: uuid.UUID, fence: Fence, step: str) -> None:
    async with session_factory() as session, session.begin():
        await RunRepository(session).check_fence(run_id, fence)
        await EventRepository(session).append(run_id, EventType.STEP_STARTED, step_name=step)


async def test_the_current_holder_may_write(session_factory: SessionFactory) -> None:
    (run_id,) = await _new_runs(session_factory, 1)
    claim = await _claim(session_factory, "w1")
    await _append(session_factory, run_id, Fence("w1", claim["fencing_token"]), "a")
    async with session_factory() as session:
        assert len(await EventRepository(session).list_events(run_id)) == 1


async def test_a_stale_token_is_rejected_and_nothing_is_written(session_factory: SessionFactory) -> None:
    (run_id,) = await _new_runs(session_factory, 1)
    old = await _claim(session_factory, "w1")
    await _expire(session_factory, run_id)
    new = await _claim(session_factory, "w2")

    with pytest.raises(StaleLeaseError):
        await _append(session_factory, run_id, Fence("w1", old["fencing_token"]), "zombie_write")
    await _append(session_factory, run_id, Fence("w2", new["fencing_token"]), "legit_write")

    async with session_factory() as session:
        names = [e.step_name for e in await EventRepository(session).list_events(run_id)]
    assert names == ["legit_write"]


async def test_the_right_token_with_the_wrong_worker_name_is_rejected(
    session_factory: SessionFactory,
) -> None:
    (run_id,) = await _new_runs(session_factory, 1)
    claim = await _claim(session_factory, "w1")
    with pytest.raises(StaleLeaseError):
        await _append(session_factory, run_id, Fence("impostor", claim["fencing_token"]), "x")


async def test_a_finished_run_rejects_late_writes_from_its_old_worker(
    session_factory: SessionFactory,
) -> None:
    (run_id,) = await _new_runs(session_factory, 1)
    claim = await _claim(session_factory, "w1")
    fence = Fence("w1", claim["fencing_token"])
    async with session_factory() as session, session.begin():
        await RunRepository(session).mark_completed(run_id, {"ok": True})  # releases the lease
    assert (await _get(session_factory, run_id)).lease_owner is None

    with pytest.raises(StaleLeaseError):
        await _append(session_factory, run_id, fence, "too_late")


async def test_tokens_keep_increasing_across_takeovers(session_factory: SessionFactory) -> None:
    (run_id,) = await _new_runs(session_factory, 1)
    seen = []
    for worker in ("w1", "w2", "w3"):
        claim = await _claim(session_factory, worker)
        seen.append(claim["fencing_token"])
        await _expire(session_factory, run_id)
    assert seen == [1, 2, 3]


async def test_resume_run_refuses_a_run_that_has_a_worker_lease(session_factory: SessionFactory) -> None:
    (run_id,) = await _new_runs(session_factory, 1)
    await _claim(session_factory, "w1")
    with pytest.raises(InvalidRunStateError, match="leased"):
        await runtime.resume_run(session_factory, run_id)


async def test_the_database_orders_a_takeover_after_an_in_flight_write(
    session_factory: SessionFactory,
) -> None:
    """The old owner's open transaction holds the row lock. A takeover must not slip in between its
    fence check and its write: the claim skips the locked row, and once the write commits the token
    is still the old one, so the commit stands. After the takeover the old token is dead."""
    (run_id,) = await _new_runs(session_factory, 1)
    old = await _claim(session_factory, "w1")
    fence = Fence("w1", old["fencing_token"])
    await _expire(session_factory, run_id)

    async with session_factory() as session, session.begin():
        await RunRepository(session).check_fence(run_id, fence)  # takes the row lock
        assert await asyncio.wait_for(_claim(session_factory, "w2"), timeout=5) is None  # skipped
        await EventRepository(session).append(run_id, EventType.STEP_STARTED, step_name="in_flight")

    assert await _claim(session_factory, "w2") is not None  # now it can be taken
    with pytest.raises(StaleLeaseError):
        await _append(session_factory, run_id, fence, "after_takeover")

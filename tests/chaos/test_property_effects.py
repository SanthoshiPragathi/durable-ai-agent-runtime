"""Property test: crash a random workflow at random points; the payment ledger must always end up
with exactly one refund per payment, and the run must always complete.

Hypothesis generates the workflow size (1-4 payments), the provider kind, where the worker 'dies'
(before or after the provider acts, on any payment, several times) and which provider calls lose their
reply. The judge is the payment provider's own ledger, a separate database from the runtime's.
"""

import asyncio
import os
import uuid
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.config import to_sync_url
from app.database.repositories import EventRepository, RunRepository
from app.database.session import async_connect_args, create_session_factory, create_sync_engine
from app.engine import runtime
from app.integrations.tools import IdempotentPaymentService, NonIdempotentPaymentService
from app.integrations.tools.fake_ledger import AMBIGUOUS_AFTER_COMMIT, AMBIGUOUS_BEFORE_COMMIT
from app.sdk import workflow
from app.sdk.workflow import registry
from tests.fixtures.crash import CrashPlan, SimulatedCrash

pytestmark = [pytest.mark.chaos, pytest.mark.integration]

MODES = ["idempotent", "verify", "review"]
steps_st = st.integers(min_value=1, max_value=4)
mode_st = st.sampled_from(MODES)
crashes_st = st.lists(
    st.tuples(st.integers(0, 3), st.sampled_from(["before", "after"])), max_size=6
)
faults_st = st.dictionaries(
    st.integers(0, 3), st.sampled_from([AMBIGUOUS_BEFORE_COMMIT, AMBIGUOUS_AFTER_COMMIT]), max_size=3
)


def reset(db_url: str, services_url: str) -> None:
    for url, tables in (
        (db_url, "workflow_events, idempotency_records, workflow_runs, workflows"),
        (services_url, "payment_refunds, emails"),
    ):
        engine = create_sync_engine(to_sync_url(url), isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
        engine.dispose()


async def scenario(
    n_steps: int,
    mode: str,
    crashes: list[tuple[int, str]],
    faults: dict[int, str],
    db_url: str,
    services_url: str,
) -> dict[str, Any]:
    engine = create_async_engine(db_url, poolclass=NullPool, connect_args=async_connect_args())
    services_engine = create_async_engine(services_url, poolclass=NullPool, connect_args=async_connect_args())
    sf, ssf = create_session_factory(engine), create_session_factory(services_engine)
    saved = registry.snapshot()
    registry.clear()
    try:
        service = IdempotentPaymentService(ssf) if mode == "idempotent" else NonIdempotentPaymentService(ssf)

        crash = CrashPlan()
        for index, phase in crashes:
            if index < n_steps:
                (crash.before if phase == "before" else crash.after).add(f"pay_{index}")
        pending_faults = {f"pay_{i}": f for i, f in faults.items() if i < n_steps}

        def tool_for(name: str):
            async def tool(*args: Any, **kwargs: Any):
                if name in pending_faults:
                    service.faults.inject(pending_faults.pop(name))
                return await service.refund(*args, **kwargs)

            return crash.wrap(name, tool)

        @workflow(name="prop_payments")
        async def prop_payments(ctx):
            refunds = []
            for i in range(n_steps):
                name = f"pay_{i}"
                if mode == "idempotent":
                    refund = await ctx.effect(name, tool_for(name), f"o{i}", 100 * (i + 1), idempotent=True)
                else:
                    reference = ctx.idempotency_key(name)

                    async def verify(order_id: str, amount: int, reference: str):
                        return await service.find_by_reference(reference)

                    refund = await ctx.effect(
                        name,
                        tool_for(name),
                        f"o{i}",
                        100 * (i + 1),
                        reference,
                        idempotent=False,
                        verify=verify if mode == "verify" else None,
                    )
                refunds.append(refund["refund_id"])
            return refunds

        run_id = (await runtime.create_run(sf, "prop_payments", {})).run_id

        status, attempts = "PENDING", 0
        limit = len(crashes) + len(faults) + 2 * n_steps + 4
        while status not in ("COMPLETED", "FAILED") and attempts < limit:
            try:
                if attempts == 0:
                    run = await runtime.execute_run(sf, run_id)
                else:
                    run = await runtime.resume_run(sf, run_id)
                status = run.status
            except SimulatedCrash:
                status = "RUNNING"
            attempts += 1

            # INVARIANT AT EVERY MOMENT: never more than one refund for any payment.
            for i in range(n_steps):
                assert await service.count_refunds(f"o{i}") <= 1, f"duplicate refund for o{i}"

            if status == "NEEDS_REVIEW":
                # A perfect reviewer: look at the provider's books and say what really happened.
                async with sf() as session:
                    events = await EventRepository(session).list_events(run_id)
                step_name = next(
                    e.step_name for e in reversed(events) if e.event_type == "STEP_OUTCOME_UNKNOWN"
                )
                found = await service.find_by_reference(f"{run_id}:{step_name}")
                if found:
                    await runtime.resolve_unknown_step(
                        sf, run_id, step_name, outcome="completed", result=found, resolved_by="reviewer"
                    )
                else:
                    await runtime.resolve_unknown_step(sf, run_id, step_name, outcome="not_executed")
                status = "RUNNING"

        async with sf() as session:
            run = await RunRepository(session).get_run(run_id)
            events = await EventRepository(session).list_events(run_id)
        ledger = await service.list_refunds()
        return {"run": run, "events": events, "ledger": ledger, "attempts": attempts, "limit": limit}
    finally:
        registry.restore(saved)
        await engine.dispose()
        await services_engine.dispose()


@settings(
    max_examples=int(os.environ.get("CHAOS_EXAMPLES", "45")),
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(n_steps=steps_st, mode=mode_st, crashes=crashes_st, faults=faults_st)
def test_no_crash_pattern_can_duplicate_or_lose_a_payment(
    migrated_database: str,
    services_database_url: str,
    n_steps: int,
    mode: str,
    crashes: list[tuple[int, str]],
    faults: dict[int, str],
) -> None:
    reset(migrated_database, services_database_url)
    result = asyncio.run(
        scenario(n_steps, mode, crashes, faults, migrated_database, services_database_url)
    )
    run, events, ledger = result["run"], result["events"], result["ledger"]

    # The run always completes (nothing is lost) within a bounded number of resumes.
    assert run.status == "COMPLETED", f"{mode}: ended {run.status} after {result['attempts']} attempts"
    assert result["attempts"] < result["limit"]

    # Exactly one refund per payment, with the right amount, in the provider's own ledger.
    assert [(r["order_id"], r["amount"]) for r in ledger] == [
        (f"o{i}", 100 * (i + 1)) for i in range(n_steps)
    ]
    assert run.result_data == [r["refund_id"] for r in ledger]

    # The runtime's history agrees: each payment completed exactly once, sequence numbers gapless.
    # A step completes either by itself (STEP_COMPLETED) or because a reviewer confirmed it
    # (STEP_RESOLVED with outcome "completed", which also records who decided).
    completed = [
        e.step_name
        for e in events
        if e.event_type == "STEP_COMPLETED"
        or (e.event_type == "STEP_RESOLVED" and e.result_data["outcome"] == "completed")
    ]
    assert completed == [f"pay_{i}" for i in range(n_steps)]
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1))
    assert [e.event_type for e in events].count("WORKFLOW_COMPLETED") == 1


def test_the_property_test_has_teeth_a_plain_step_does_duplicate(
    migrated_database: str, services_database_url: str
) -> None:
    """Negative control: if payments were wrapped in ctx.step instead of ctx.effect, the same crash
    produces a duplicate, so a passing property test above really is evidence."""
    reset(migrated_database, services_database_url)

    async def naive() -> int:
        engine = create_async_engine(migrated_database, poolclass=NullPool, connect_args=async_connect_args())
        services_engine = create_async_engine(
            services_database_url, poolclass=NullPool, connect_args=async_connect_args()
        )
        sf, ssf = create_session_factory(engine), create_session_factory(services_engine)
        saved = registry.snapshot()
        registry.clear()
        try:
            service = NonIdempotentPaymentService(ssf)
            crash = CrashPlan()
            crash.after.add("pay")

            @workflow(name=f"naive_{uuid.uuid4().hex[:6]}")
            async def naive_flow(ctx):
                return await ctx.step("pay", crash.wrap("pay", service.refund), "o0", 100)

            name = next(iter(registry.snapshot()))[0]
            run_id = (await runtime.create_run(sf, name, {})).run_id
            with pytest.raises(SimulatedCrash):
                await runtime.execute_run(sf, run_id)
            await runtime.resume_run(sf, run_id)
            return await service.count_refunds("o0")
        finally:
            registry.restore(saved)
            await engine.dispose()
            await services_engine.dispose()

    assert asyncio.run(naive()) == 2

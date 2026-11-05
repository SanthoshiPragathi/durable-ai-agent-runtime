"""Chaos test: the database becomes unreachable, and acknowledgements get lost, while workers run.

Two kinds of fault, both injected where they hurt:
  * outage windows: for a few hundred milliseconds every statement on the runtime's connections fails
    (what a PostgreSQL restart or a network partition looks like to a worker);
  * lost acknowledgements: a recorder write is committed, then the caller is told it failed (the worst
    case: the database has the event, the worker believes it does not).

The judges are independent of the runtime: the payment and email ledgers (a separate database) and
the event log's own invariants. A run must never be recorded as FAILED because of an outage, never
refund or email twice, and always complete once the database is back.

CHAOS_SEEDS (default 4) controls how many random schedules run.
"""

import asyncio
import os
import random

import pytest
from sqlalchemy import event
from sqlalchemy.exc import OperationalError

import app.examples.refund_workflow  # noqa: F401  (registers "refund_workflow")
from app.database.repositories import RunRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.execution_context import DbStepRecorder
from app.engine.worker import Worker
from app.integrations.llm import MockLLMProvider
from tests.chaos import test_lease_chaos as lease_chaos
from tests.chaos.test_lease_chaos import RUNS, check_history, reset

payments = lease_chaos.payments  # the same fixtures, reused
mail = lease_chaos.mail

pytestmark = [pytest.mark.chaos, pytest.mark.integration]

SEEDS = int(os.environ.get("CHAOS_SEEDS", "4"))
WORKERS = 4


class Outage:
    def __init__(self) -> None:
        self.active = False
        self.rejected = 0

    def attach(self, session_factory: SessionFactory) -> None:
        engine = session_factory.kw["bind"].sync_engine

        @event.listens_for(engine, "before_cursor_execute")
        def _fail(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
            if self.active:
                self.rejected += 1
                raise OperationalError(statement, parameters, ConnectionResetError("simulated outage"))


async def outages(outage: Outage, rng: random.Random, stop: asyncio.Event) -> int:
    windows = 0
    while not stop.is_set():
        await asyncio.sleep(rng.uniform(0.15, 0.5))
        outage.active = True
        windows += 1
        await asyncio.sleep(rng.uniform(0.05, 0.4))
        outage.active = False
    return windows


def lose_acks(monkeypatch: pytest.MonkeyPatch, rng: random.Random) -> list[int]:
    """Make 8% of step_started / step_completed calls commit and then raise (ack lost)."""
    lost = [0]
    for name in ("step_started", "step_completed"):
        original = getattr(DbStepRecorder, name)

        async def wrapper(self, *args, _original=original, **kwargs):
            result = await _original(self, *args, **kwargs)
            if rng.random() < 0.08:
                lost[0] += 1
                raise OperationalError("COMMIT", {}, ConnectionResetError("ack lost"))
            return result

        monkeypatch.setattr(DbStepRecorder, name, wrapper)
    return lost


async def test_outages_and_lost_acks_never_fail_a_run_or_duplicate_an_effect(
    session_factory: SessionFactory,
    migrated_database: str,
    services_database_url: str,
    payments,
    mail,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outage = Outage()
    outage.attach(session_factory)
    total_windows = total_infra = total_lost = takeovers = 0

    for seed in range(SEEDS):
        outage.active = False
        reset(migrated_database, services_database_url)
        rng = random.Random(seed)
        run_ids = [
            (await runtime.create_run(session_factory, "refund_workflow", {"order_id": "ORD-1001"})).run_id
            for _ in range(RUNS)
        ]
        with monkeypatch.context() as m:
            lost = lose_acks(m, rng)
            workers = [
                Worker(
                    session_factory,
                    f"w{i}",
                    lease_seconds=0.8,
                    heartbeat_seconds=0.1,
                    poll_seconds=0.02,
                    llm_provider=MockLLMProvider("APPROVE"),
                )
                for i in range(WORKERS)
            ]
            stop = asyncio.Event()

            async def run_workers(pool: list[Worker] = workers, done: asyncio.Event = stop) -> None:
                await asyncio.gather(*(w.run_forever(drain=True) for w in pool))
                done.set()

            windows, _ = await asyncio.wait_for(
                asyncio.gather(outages(outage, rng, stop), run_workers()), timeout=120
            )
        outage.active = False

        total_windows += windows
        total_lost += lost[0]
        total_infra += sum(w.infra_errors for w in workers)

        # Independent judges.
        assert await payments.count_refunds() == RUNS, f"seed {seed}: refunds"
        assert len(await mail.list_emails()) == RUNS, f"seed {seed}: emails"
        for run_id in run_ids:
            await check_history(session_factory, run_id, seed)
            async with session_factory() as session:
                run = await RunRepository(session).get_run(run_id)
            takeovers += run.fencing_token - 1

    # Meaningful only if the faults really happened.
    assert outage.rejected > 0 and total_lost > 0 and total_infra > 0, (
        outage.rejected,
        total_lost,
        total_infra,
    )
    print(
        f"\noutage chaos: {SEEDS} seeds, {total_windows} outage windows, {outage.rejected} statements "
        f"rejected, {total_lost} lost acks, {total_infra} abandoned runs/loops, {takeovers} takeovers"
    )

"""Chaos test: leases expire at random moments while several workers run refunds.

A chaos task keeps forcing the leases of RUNNING runs to lapse, even though their workers are alive
and well (a false failure detection: the worst case for a lease system). Other workers take those
runs over while the original workers are still mid-step. The judges are independent of the runtime:
the payment and email ledgers (their own database) and the event log's own invariants.

CHAOS_SEEDS (default 6) controls how many random schedules run.
"""

import asyncio
import os
import random
import uuid
from collections.abc import Iterator

import pytest
from sqlalchemy import text

from app.config import to_sync_url
from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory, create_sync_engine
from app.engine import runtime
from app.engine.worker import Worker
from app.integrations.llm import MockLLMProvider
from app.integrations.tools import (
    EmailService,
    IdempotentPaymentService,
    set_email_service,
    set_payment_service,
)

import app.examples.refund_workflow  # noqa: F401  isort:skip  (registers "refund_workflow")

pytestmark = [pytest.mark.chaos, pytest.mark.integration]

SEEDS = int(os.environ.get("CHAOS_SEEDS", "6"))
RUNS = 6
WORKERS = 4


@pytest.fixture
def payments(services_session_factory: SessionFactory) -> Iterator[IdempotentPaymentService]:
    service = IdempotentPaymentService(services_session_factory)
    set_payment_service(service)
    yield service
    set_payment_service(None)


@pytest.fixture
def mail(services_session_factory: SessionFactory) -> Iterator[EmailService]:
    service = EmailService(services_session_factory)
    set_email_service(service)
    yield service
    set_email_service(None)


def reset(db_url: str, services_url: str) -> None:
    for url, tables in (
        (db_url, "workflow_events, idempotency_records, workflow_runs, workflows, workers"),
        (services_url, "payment_refunds, emails"),
    ):
        engine = create_sync_engine(to_sync_url(url), isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
        engine.dispose()


async def force_expiry(sf: SessionFactory, rng: random.Random, rounds: int) -> int:
    """Make random live leases lapse, `rounds` times. Returns how many leases it actually broke.

    The outer WHERE repeats the status test: if the chosen run finishes while this UPDATE waits for
    its row lock, the run must be left alone (a finished run has no lease to break)."""
    broken = 0
    for _ in range(rounds):
        await asyncio.sleep(rng.uniform(0.01, 0.08))
        async with sf() as session, session.begin():
            result = await session.execute(
                text(
                    """
                    UPDATE workflow_runs SET lease_expires_at = now() - interval '1 second'
                    WHERE status = 'RUNNING' AND lease_owner IS NOT NULL AND run_id = (
                        SELECT run_id FROM workflow_runs
                        WHERE status = 'RUNNING' AND lease_owner IS NOT NULL
                        ORDER BY random() LIMIT 1)
                    """
                )
            )
            broken += result.rowcount
    return broken


async def test_forced_lease_expiry_never_duplicates_a_refund_or_corrupts_a_history(
    session_factory: SessionFactory,
    services_session_factory: SessionFactory,
    migrated_database: str,
    services_database_url: str,
    payments: IdempotentPaymentService,
    mail: EmailService,
) -> None:
    takeovers = zombies = broken_total = 0
    for seed in range(SEEDS):
        reset(migrated_database, services_database_url)
        rng = random.Random(seed)
        run_ids = [
            (await runtime.create_run(session_factory, "refund_workflow", {"order_id": "ORD-1001"})).run_id
            for _ in range(RUNS)
        ]
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

        async def work(worker: Worker) -> None:
            await worker.run_forever(drain=True)

        *_, broken = await asyncio.wait_for(
            asyncio.gather(*(work(w) for w in workers), force_expiry(session_factory, rng, 40)),
            timeout=120,
        )
        broken_total += broken
        zombies += sum(w.lost_leases for w in workers)

        # The independent judges: exactly one refund and one email per run.
        assert await payments.count_refunds() == RUNS, f"seed {seed}"
        assert len(await mail.list_emails()) == RUNS, f"seed {seed}"

        for run_id in run_ids:
            await check_history(session_factory, run_id, seed)
            async with session_factory() as session:
                run = await RunRepository(session).get_run(run_id)
            takeovers += run.fencing_token - 1

    # The test is only meaningful if the chaos really caused takeovers and stale workers.
    assert broken_total > 0 and takeovers > 0 and zombies > 0, (broken_total, takeovers, zombies)
    print(f"\nchaos: {SEEDS} seeds, {broken_total} leases broken, {takeovers} takeovers, {zombies} zombies")


async def check_history(sf: SessionFactory, run_id: uuid.UUID, seed: int) -> None:
    async with sf() as session:
        run = await RunRepository(session).get_run(run_id)
        events = await EventRepository(session).list_events(run_id)
    where = f"seed {seed}, run {run_id}"
    assert run is not None and run.status == "COMPLETED", where
    assert run.result_data["status"] == "REFUNDED", where
    assert run.lease_owner is None and run.lease_expires_at is None, where
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1)), where
    types = [e.event_type for e in events]
    assert types.count("WORKFLOW_COMPLETED") == 1 and types[-1] == "WORKFLOW_COMPLETED", where
    completed = [e.step_name for e in events if e.event_type == "STEP_COMPLETED"]
    assert sorted(completed) == sorted(set(completed)) and len(completed) == 5, where  # once each
    # Tokens in the history only ever go up, and none exceeds the run's final token. (Some claims leave
    # no event: a worker can be taken over again before it wrote anything, so the number of
    # WORKFLOW_RESUMED events may be smaller than the number of takeovers, never larger.)
    assert types[0] == "WORKFLOW_STARTED" and types.count("WORKFLOW_STARTED") == 1, where
    tokens = [
        e.result_data["fencing_token"]
        for e in events
        if e.event_type in ("WORKFLOW_STARTED", "WORKFLOW_RESUMED")
    ]
    assert tokens == sorted(set(tokens)), where
    assert tokens[-1] <= run.fencing_token, where

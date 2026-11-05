"""Chaos test: PostgreSQL itself is restarted while real worker processes are mid-run.

This needs a way to restart the database, which depends on where it runs, so it only runs when
PG_RESTART_CMD is set, for example:

    Docker Compose:  PG_RESTART_CMD="docker compose restart postgres"
    local server:    PG_RESTART_CMD="pg_ctl -D /path/to/data restart -m fast -w"

(scripts/verify.ps1 sets it for you.) The runtime database and the fake payment/email ledgers live on
the same server in this setup, so the restart also cuts the "provider" connections: those surface as
ambiguous outcomes, exactly like a dropped network connection to a real provider.

Judges: the ledgers (one refund and one email per run) and the event log (each run completed, each
step completed once, no run failed because of the outage).
"""

import asyncio
import os
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.integrations.tools import EmailService, IdempotentPaymentService
from tests.conftest import ROOT
from tests.fixtures.processes import start_worker

pytestmark = [
    pytest.mark.chaos,
    pytest.mark.integration,
    pytest.mark.skipif(not os.environ.get("PG_RESTART_CMD"), reason="set PG_RESTART_CMD to run"),
]

RUNS = 12
WORKERS = 3
MODULES = "tests.recovery.process_workflows"


async def wait_until_database_answers(sf: SessionFactory, timeout: float = 90) -> None:
    """`docker compose restart` returns before PostgreSQL accepts connections again."""
    from sqlalchemy import text

    deadline = time.monotonic() + timeout
    while True:
        try:
            async with sf() as session:
                await session.execute(text("SELECT 1"))
            return
        except Exception:
            if time.monotonic() > deadline:
                raise
            await asyncio.sleep(0.5)


async def test_restarting_postgres_mid_run_loses_nothing_and_duplicates_nothing(
    session_factory: SessionFactory,
    services_session_factory: SessionFactory,
    migrated_database: str,
    services_database_url: str,
    tmp_path: Path,
) -> None:
    async with session_factory() as session, session.begin():
        repo = RunRepository(session)
        await repo.ensure_workflow("slow_refund", 1)
        run_ids: list[uuid.UUID] = [
            (await repo.create_run("slow_refund", 1, {"order_id": f"ORD-{i}"})).run_id for i in range(RUNS)
        ]

    procs = [
        start_worker(
            f"pg-{i}", db_url=migrated_database, services_url=services_database_url, modules=MODULES
        )
        for i in range(WORKERS)
    ]
    await asyncio.sleep(2.0)  # let the workers get into their runs
    started = time.monotonic()
    restart = subprocess.run(
        os.environ["PG_RESTART_CMD"], shell=True, cwd=ROOT, capture_output=True, text=True, timeout=120
    )
    outage_seconds = time.monotonic() - started
    assert restart.returncode == 0, restart.stderr

    await wait_until_database_answers(session_factory)
    codes = []
    for proc in procs:
        proc.communicate(timeout=180)
        codes.append(proc.returncode)
    assert codes == [0] * WORKERS, codes  # no worker crashed because the database went away

    completed = takeovers = 0
    for run_id in run_ids:
        async with session_factory() as session:
            run = await RunRepository(session).get_run(run_id)
            events = await EventRepository(session).list_events(run_id)
        assert run is not None and run.status == "COMPLETED", (run_id, run and run.error_message)
        types = [e.event_type for e in events]
        done = [e.step_name for e in events if e.event_type == "STEP_COMPLETED"]
        assert sorted(done) == sorted(set(done)) and len(done) == 6, (run_id, done)
        assert types.count("WORKFLOW_COMPLETED") == 1 and "WORKFLOW_FAILED" not in types
        completed += 1
        takeovers += run.fencing_token - 1

    assert await IdempotentPaymentService(services_session_factory).count_refunds() == RUNS
    assert len(await EmailService(services_session_factory).list_emails()) == RUNS
    print(f"\npostgres restart: {outage_seconds:.1f}s outage, {completed}/{RUNS} runs completed, "
          f"{takeovers} takeovers, one refund and one email per run")

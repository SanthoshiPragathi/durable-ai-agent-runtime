"""Real worker processes: sharing work, killed workers, and zombie workers.

Judges are independent of the runtime: the fake payment and email ledgers (their own database) and
the event log. Lease timings are short (1.5 s) so takeovers happen quickly.
"""

import asyncio
import re
import time
import uuid

import pytest

from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.failpoints import CRASH_EXIT_CODE
from app.integrations.tools import EmailService, IdempotentPaymentService
from tests.fixtures.processes import start_worker

pytestmark = [pytest.mark.integration, pytest.mark.recovery]

LEASE = 1.5


@pytest.fixture
def env(migrated_database: str, services_database_url: str):
    return {"db_url": migrated_database, "services_url": services_database_url}


async def new_run(sf: SessionFactory, workflow: str, **input_data) -> uuid.UUID:
    async with sf() as session, session.begin():
        repo = RunRepository(session)
        await repo.ensure_workflow(workflow, 1)
        return (await repo.create_run(workflow, 1, input_data)).run_id


async def ledgers(services_sf: SessionFactory) -> tuple[int, int]:
    return (
        await IdempotentPaymentService(services_sf).count_refunds(),
        len(await EmailService(services_sf).list_emails()),
    )


async def wait_for_owner(sf: SessionFactory, run_id: uuid.UUID, owner: str, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        async with sf() as session:
            run = await RunRepository(session).get_run(run_id)
        if run is not None and run.lease_owner == owner:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"{owner} never claimed run {run_id}")


def finish(proc, timeout: float = 90):
    out, err = proc.communicate(timeout=timeout)
    return proc.returncode, out, err


async def history(sf: SessionFactory, run_id: uuid.UUID):
    async with sf() as session:
        run = await RunRepository(session).get_run(run_id)
        events = await EventRepository(session).list_events(run_id)
    assert run is not None
    return run, events


async def test_worker_processes_split_the_runs_and_each_runs_exactly_once(
    session_factory: SessionFactory, env: dict
) -> None:
    ids = [await new_run(session_factory, "order_summary", order_id="A-100") for _ in range(15)]

    procs = [start_worker(f"proc-{i}", **env) for i in range(3)]
    results = [finish(p) for p in procs]

    assert all(code == 0 for code, _, _ in results), results
    claimed = 0
    for run_id in ids:
        run, events = await history(session_factory, run_id)
        types = [e.event_type for e in events]
        assert run.status == "COMPLETED" and run.fencing_token == 1
        assert types.count("WORKFLOW_STARTED") == 1 and types.count("STEP_COMPLETED") == 3
        claimed += 1
    assert claimed == 15
    summaries = [re.search(r"completed=(\d+)", out).group(1) for _, out, _ in results]
    assert sum(int(n) for n in summaries) == 15  # the workers' own counts add up to the total


KILL_POINTS = [
    "after_workflow_started",
    "after_step_started:process_refund",
    "before_step_completed:process_refund",
    "before_step_completed:send_confirmation",
    "before_workflow_completed",
]


@pytest.mark.parametrize("failpoint", KILL_POINTS)
async def test_a_killed_worker_is_replaced_automatically_with_one_refund_and_one_email(
    session_factory: SessionFactory,
    services_session_factory: SessionFactory,
    env: dict,
    failpoint: str,
) -> None:
    run_id = await new_run(session_factory, "refund_workflow", order_id="ORD-1001")

    victim = start_worker("victim", failpoint=failpoint, **env)
    code, _, err = finish(victim)
    assert code == CRASH_EXIT_CODE, err

    rescuer = start_worker("rescuer", **env)  # nobody tells it the victim died: the lease does
    code, out, err = finish(rescuer)
    assert code == 0, err
    assert "completed=1" in out

    assert await ledgers(services_session_factory) == (1, 1)
    run, events = await history(session_factory, run_id)
    assert run.status == "COMPLETED" and run.fencing_token == 2 and run.lease_owner is None
    started = next(e for e in events if e.event_type == "WORKFLOW_STARTED")
    resumed = next(e for e in events if e.event_type == "WORKFLOW_RESUMED")
    assert started.result_data == {"worker_id": "victim", "fencing_token": 1}
    assert resumed.result_data["worker_id"] == "rescuer" and resumed.result_data["fencing_token"] == 2
    assert [e.event_type for e in events].count("STEP_COMPLETED") == 5  # every step exactly once


@pytest.mark.parametrize(
    "stall_at",
    [
        pytest.param("before_step_completed:process_refund", id="frozen-after-the-refund-succeeded"),
        pytest.param("after_step_started:process_refund", id="frozen-before-calling-the-payment-service"),
        pytest.param("before_workflow_completed", id="frozen-after-the-last-step"),
    ],
)
async def test_a_zombie_worker_wakes_up_and_cannot_change_anything(
    session_factory: SessionFactory,
    services_session_factory: SessionFactory,
    env: dict,
    stall_at: str,
) -> None:
    """The zombie freezes (event loop AND heartbeat) for far longer than its lease. A second worker
    takes the run and finishes it. When the zombie thaws it must be rejected, not carry on."""
    run_id = await new_run(session_factory, "refund_workflow", order_id="ORD-1001")

    zombie = start_worker("zombie", stall=f"{stall_at}/8", **env)
    await wait_for_owner(session_factory, run_id, "zombie")  # make sure the zombie really holds the run
    healthy = start_worker("healthy", **env)
    healthy_code, healthy_out, healthy_err = finish(healthy)
    zombie_code, zombie_out, zombie_err = finish(zombie)

    assert healthy_code == 0, healthy_err
    assert zombie_code == 0, zombie_err  # it stands down cleanly, it does not crash or retry
    assert "completed=1" in healthy_out
    assert "completed=0 lost_leases=1" in zombie_out, zombie_out + zombie_err

    assert await ledgers(services_session_factory) == (1, 1)  # never a second refund or email
    run, events = await history(session_factory, run_id)
    types = [e.event_type for e in events]
    assert run.status == "COMPLETED" and run.fencing_token == 2
    assert types.count("STEP_COMPLETED") == 5 and types.count("WORKFLOW_COMPLETED") == 1
    assert types[-1] == "WORKFLOW_COMPLETED"  # nothing was appended after the real completion
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1))
    started = next(e for e in events if e.event_type == "WORKFLOW_STARTED")
    resumed = next(e for e in events if e.event_type == "WORKFLOW_RESUMED")
    assert started.result_data["worker_id"] == "zombie" and resumed.result_data["worker_id"] == "healthy"
    assert run.result_data["status"] == "REFUNDED"

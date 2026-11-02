"""Demo 3 with real killed processes. A worker process is killed at an exact point; another process
resumes the run. Judge: the payment and email ledgers, which live in their own database."""

import json
import uuid

import pytest

from app.database.repositories import EventRepository, IdempotencyRepository, RunRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.failpoints import CRASH_EXIT_CODE
from app.integrations.tools import EmailService, IdempotentPaymentService, NonIdempotentPaymentService
from tests.fixtures.processes import run_worker

pytestmark = [pytest.mark.integration, pytest.mark.recovery]

REFUND = "refund_workflow"
LEGACY = "legacy_refund_pipeline"
LEGACY_MODULES = "tests.recovery.process_workflows"


async def new_run(sf: SessionFactory, workflow: str, order_id: str) -> uuid.UUID:
    # Created straight in the database: the test process does not register these workflows.
    from app.database.repositories import RunRepository as Runs

    async with sf() as session, session.begin():
        repo = Runs(session)
        await repo.ensure_workflow(workflow, 1)
        return (await repo.create_run(workflow, 1, {"order_id": order_id})).run_id


@pytest.fixture
def env(migrated_database: str, services_database_url: str):
    return {"db_url": migrated_database, "services_url": services_database_url}


async def ledgers(services_sf: SessionFactory) -> tuple[int, int]:
    return (
        await IdempotentPaymentService(services_sf).count_refunds(),
        len(await EmailService(services_sf).list_emails()),
    )


# (failpoint, refunds and emails that exist the moment the worker dies)
KILL_POINTS = [
    pytest.param("after_workflow_started", (0, 0), id="before-any-step"),
    pytest.param("before_step_started:process_refund", (0, 0), id="before-STEP_STARTED-of-the-refund"),
    pytest.param("after_step_started:process_refund", (0, 0), id="after-STEP_STARTED-before-the-call"),
    pytest.param("before_step_completed:process_refund", (1, 0), id="after-the-refund-SUCCEEDED"),
    pytest.param("after_step_completed:process_refund", (1, 0), id="right-after-STEP_COMPLETED"),
    pytest.param("after_step_started:send_confirmation", (1, 0), id="before-the-email-call"),
    pytest.param("before_step_completed:send_confirmation", (1, 1), id="after-the-email-was-sent"),
    pytest.param("before_workflow_completed", (1, 1), id="after-the-last-step"),
]


@pytest.mark.parametrize(("failpoint", "at_death"), KILL_POINTS)
async def test_refund_survives_a_kill_at_every_point_with_exactly_one_refund_and_one_email(
    session_factory: SessionFactory,
    services_session_factory: SessionFactory,
    env: dict,
    failpoint: str,
    at_death: tuple[int, int],
) -> None:
    run_id = await new_run(session_factory, REFUND, "ORD-1001")

    first = run_worker("execute", run_id, failpoint=failpoint, **env)
    assert first.returncode == CRASH_EXIT_CODE, first.stdout + first.stderr
    assert await ledgers(services_session_factory) == at_death  # what really happened before death

    second = run_worker("resume", run_id, **env)
    assert second.returncode == 0, second.stdout + second.stderr
    assert json.loads(second.stdout)["status"] == "COMPLETED"

    assert await ledgers(services_session_factory) == (1, 1)  # never 2
    async with session_factory() as session:
        run = await RunRepository(session).get_run(run_id)
        events = await EventRepository(session).list_events(run_id)
    ledger = (await IdempotentPaymentService(services_session_factory).list_refunds())[0]
    assert run.result_data["refund_id"] == ledger["refund_id"]
    assert ledger["idempotency_key"] == f"{run_id}:process_refund"
    completed = [e.step_name for e in events if e.event_type == "STEP_COMPLETED"]
    assert completed.count("process_refund") == 1 and completed.count("send_confirmation") == 1
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1))


async def test_three_workers_die_in_different_places_and_a_fourth_finishes_with_one_refund(
    session_factory: SessionFactory, services_session_factory: SessionFactory, env: dict
) -> None:
    run_id = await new_run(session_factory, REFUND, "ORD-1001")

    deaths = [
        ("execute", "before_step_completed:process_refund"),
        ("resume", "before_step_completed:process_refund"),  # dies in the same window AGAIN
        ("resume", "before_step_completed:send_confirmation"),
    ]
    for action, failpoint in deaths:
        assert run_worker(action, run_id, failpoint=failpoint, **env).returncode == CRASH_EXIT_CODE
    final = run_worker("resume", run_id, **env)

    assert final.returncode == 0, final.stdout + final.stderr
    assert await ledgers(services_session_factory) == (1, 1)
    async with session_factory() as session:
        ops = {o.step_name: o for o in await IdempotencyRepository(session).list_for_run(run_id)}
    assert ops["process_refund"].status == "SUCCEEDED" and ops["process_refund"].attempts == 3


async def test_a_rejected_order_moves_no_money_even_through_crashes(
    session_factory: SessionFactory, services_session_factory: SessionFactory, env: dict
) -> None:
    run_id = await new_run(session_factory, REFUND, "ORD-2002")  # outside the refund window
    first = run_worker("execute", run_id, failpoint="after_step_completed:fetch_order", **env)
    assert first.returncode == CRASH_EXIT_CODE
    final = run_worker("resume", run_id, **env)
    assert final.returncode == 0
    assert json.loads(final.stdout)["result"]["status"] == "REJECTED"
    assert await ledgers(services_session_factory) == (0, 0)


async def test_a_non_idempotent_provider_is_parked_for_review_and_never_repeated(
    session_factory: SessionFactory, services_session_factory: SessionFactory, env: dict
) -> None:
    run_id = await new_run(session_factory, LEGACY, "ORD-1")
    legacy = NonIdempotentPaymentService(services_session_factory)

    first = run_worker(
        "execute", run_id, failpoint="before_step_completed:process_refund", modules=LEGACY_MODULES, **env
    )
    assert first.returncode == CRASH_EXIT_CODE
    assert await legacy.count_refunds() == 1  # the refund happened; the runtime has no record of it

    parked = run_worker("resume", run_id, modules=LEGACY_MODULES, **env)
    assert parked.returncode == 3, parked.stdout + parked.stderr  # NEEDS_REVIEW
    assert json.loads(parked.stdout)["status"] == "NEEDS_REVIEW"
    assert await legacy.count_refunds() == 1  # NOT repeated

    # A person checks the provider's dashboard, finds the refund, and says so.
    refund = (await legacy.list_refunds())[0]
    await runtime.resolve_unknown_step(
        session_factory, run_id, "process_refund", outcome="completed", result=refund, resolved_by="asha"
    )
    done = run_worker("resume", run_id, modules=LEGACY_MODULES, **env)

    assert done.returncode == 0, done.stdout + done.stderr
    assert json.loads(done.stdout)["result"] == {"refund_id": refund["refund_id"]}
    assert await legacy.count_refunds() == 1  # still exactly one

"""Real processes killed at the lease-protocol moments: heartbeat, takeover and recovery.

The step-level windows (before/after STEP_STARTED, before STEP_COMPLETED, ...) are covered in
test_process_workers.py. These cover the windows around the lease itself:

  before_heartbeat    a worker dies mid-run, when its heartbeat is due
  after_lease_claimed a worker dies right after taking a run over, before recording anything
  during_recovery     a worker dies while recovering a run (its WORKFLOW_RESUMED never commits)

Each scenario ends with a healthy worker finishing the run. The judges are the payment and email
ledgers (their own database) and the event log.
"""

import uuid

import pytest

from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.failpoints import CRASH_EXIT_CODE
from app.integrations.tools import EmailService, IdempotentPaymentService
from tests.fixtures.processes import start_worker
from tests.recovery.test_process_workers import finish, new_run

pytestmark = [pytest.mark.integration, pytest.mark.recovery, pytest.mark.concurrency]

MODULES = "tests.recovery.process_workflows"


@pytest.fixture
def env(migrated_database: str, services_database_url: str):
    return {"db_url": migrated_database, "services_url": services_database_url, "modules": MODULES}


async def outcome(sf: SessionFactory, ssf: SessionFactory, run_id: uuid.UUID):
    async with sf() as session:
        run = await RunRepository(session).get_run(run_id)
        events = await EventRepository(session).list_events(run_id)
    assert run is not None
    refunds = await IdempotentPaymentService(ssf).count_refunds()
    return run, events, refunds, len(await EmailService(ssf).list_emails())


def assert_clean_finish(run, events, refunds: int, emails: int) -> None:
    kinds = [e.event_type for e in events]
    assert run.status == "COMPLETED" and run.lease_owner is None
    assert (refunds, emails) == (1, 1)
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1))
    assert kinds.count("WORKFLOW_COMPLETED") == 1
    done = [e.step_name for e in events if e.event_type == "STEP_COMPLETED"]
    assert done == ["look_up", "check", "process_refund", "review", "send_confirmation", "wrap_up"]


async def test_a_worker_killed_when_its_heartbeat_is_due_is_replaced(
    session_factory: SessionFactory, services_session_factory: SessionFactory, env: dict
) -> None:
    run_id = await new_run(session_factory, "slow_refund", order_id="ORD-HB")

    victim = start_worker("victim", failpoint="before_heartbeat", **env)
    code, _, err = finish(victim)
    assert code == CRASH_EXIT_CODE, err  # the run takes ~1.5 s, so a 0.3 s heartbeat fires mid-run

    code, out, err = finish(start_worker("rescuer", **env))
    assert code == 0, err

    run, events, refunds, emails = await outcome(session_factory, services_session_factory, run_id)
    assert_clean_finish(run, events, refunds, emails)
    assert run.fencing_token == 2
    assert [e.result_data["worker_id"] for e in events if e.event_type == "WORKFLOW_RESUMED"] == ["rescuer"]


async def test_a_worker_killed_right_after_taking_a_run_over_is_itself_replaced(
    session_factory: SessionFactory, services_session_factory: SessionFactory, env: dict
) -> None:
    """Two deaths in a row: the first worker dies mid-run; the second dies the moment it claims the
    run (lease committed, nothing recorded); the third finishes it. Fencing tokens: 1, 2, 3."""
    run_id = await new_run(session_factory, "slow_refund", order_id="ORD-TAKEOVER")

    code, _, err = finish(start_worker("first", failpoint="before_step_completed:process_refund", **env))
    assert code == CRASH_EXIT_CODE, err
    code, _, err = finish(start_worker("second", failpoint="after_lease_claimed", **env))
    assert code == CRASH_EXIT_CODE, err
    code, _, err = finish(start_worker("third", **env))
    assert code == 0, err

    run, events, refunds, emails = await outcome(session_factory, services_session_factory, run_id)
    assert_clean_finish(run, events, refunds, emails)
    assert run.fencing_token == 3
    resumed = [e.result_data["worker_id"] for e in events if e.event_type == "WORKFLOW_RESUMED"]
    assert resumed == ["third"]  # the second worker never recorded anything: it died after the claim


async def test_a_worker_killed_while_recovering_leaves_no_half_recovery(
    session_factory: SessionFactory, services_session_factory: SessionFactory, env: dict
) -> None:
    """The recovering worker dies after appending WORKFLOW_RESUMED but before its transaction commits.
    The append rolls back, so the log shows exactly one resume (by the worker that really finished)."""
    run_id = await new_run(session_factory, "slow_refund", order_id="ORD-RECOVER")

    code, _, err = finish(start_worker("first", failpoint="before_step_completed:process_refund", **env))
    assert code == CRASH_EXIT_CODE, err
    code, _, err = finish(start_worker("recovering", failpoint="during_recovery", **env))
    assert code == CRASH_EXIT_CODE, err
    code, _, err = finish(start_worker("finisher", **env))
    assert code == 0, err

    run, events, refunds, emails = await outcome(session_factory, services_session_factory, run_id)
    assert_clean_finish(run, events, refunds, emails)
    resumed = [e.result_data["worker_id"] for e in events if e.event_type == "WORKFLOW_RESUMED"]
    assert resumed == ["finisher"]
    assert run.fencing_token == 3

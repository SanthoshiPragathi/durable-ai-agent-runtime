"""Real crashes: a worker process is killed (exit 77, no cleanup) at an exact protocol point by a
failpoint, then a second process resumes the run. The ledger file says what truly executed."""

import json
import os
import subprocess
import sys
import uuid
from collections import Counter
from pathlib import Path

import pytest

from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.failpoints import CRASH_EXIT_CODE, ENV_VAR
from tests.conftest import ROOT

pytestmark = [pytest.mark.integration, pytest.mark.recovery]

WORKFLOW = "process_refund_pipeline"
STEPS = ["fetch_order", "check_eligibility", "process_refund", "send_email"]
FINAL_RESULT = {"order_id": "A1", "refund_id": "r-1", "eligible": True}
ONCE = dict.fromkeys(STEPS, 1)


async def new_run(sf: SessionFactory) -> uuid.UUID:
    # Created straight in the database: the workflow is registered only inside the subprocess.
    async with sf() as session, session.begin():
        repo = RunRepository(session)
        await repo.ensure_workflow(WORKFLOW, 1)
        return (await repo.create_run(WORKFLOW, 1, {"order_id": "A1"})).run_id


def runner(action: str, run_id: uuid.UUID, db_url: str, ledger: Path, failpoint: str | None):
    env = {
        **os.environ,
        "DATABASE_URL": db_url,
        "DURABLE_WORKFLOW_MODULES": "tests.recovery.process_workflows",
        "DURABLE_TEST_LEDGER": str(ledger),
    }
    env.pop(ENV_VAR, None)
    if failpoint:
        env[ENV_VAR] = failpoint
    return subprocess.run(
        [sys.executable, "-m", "app.engine.runner", action, str(run_id)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


def ledger_counts(ledger: Path) -> Counter:
    return Counter(ledger.read_text().split()) if ledger.exists() else Counter()


async def snapshot(sf: SessionFactory, run_id: uuid.UUID):
    async with sf() as session:
        run = await RunRepository(session).get_run(run_id)
        return run, await EventRepository(session).list_events(run_id)


# (failpoint, last event left behind by the crash, how often each step function really ran)
SCENARIOS = [
    pytest.param("after_workflow_started", ("WORKFLOW_STARTED", None), ONCE, id="crash-before-first-step"),
    pytest.param(
        "before_step_started:process_refund",
        ("STEP_COMPLETED", "check_eligibility"),
        ONCE,
        id="crash-before-STEP_STARTED",
    ),
    pytest.param(
        "after_step_started:process_refund",
        ("STEP_STARTED", "process_refund"),
        ONCE,
        id="crash-after-STEP_STARTED-before-the-step-ran",
    ),
    pytest.param(
        "after_step_completed:process_refund",
        ("STEP_COMPLETED", "process_refund"),
        ONCE,
        id="crash-right-after-STEP_COMPLETED",
    ),
    pytest.param(
        "before_workflow_completed",
        ("STEP_COMPLETED", "send_email"),
        ONCE,
        id="crash-after-last-step-before-WORKFLOW_COMPLETED",
    ),
]


@pytest.mark.parametrize(("failpoint", "last_event", "expected_runs"), SCENARIOS)
async def test_killed_worker_is_resumed_by_another_process(
    session_factory: SessionFactory,
    migrated_database: str,
    tmp_path: Path,
    failpoint: str,
    last_event: tuple[str, str | None],
    expected_runs: dict[str, int],
) -> None:
    ledger = tmp_path / "ledger.txt"
    run_id = await new_run(session_factory)

    first = runner("execute", run_id, migrated_database, ledger, failpoint)
    assert first.returncode == CRASH_EXIT_CODE, first.stderr  # the process really was killed

    run, events = await snapshot(session_factory, run_id)
    assert run.status == "RUNNING"
    assert (events[-1].event_type, events[-1].step_name) == last_event

    second = runner("resume", run_id, migrated_database, ledger, None)
    assert second.returncode == 0, second.stdout + second.stderr
    assert json.loads(second.stdout)["status"] == "COMPLETED"

    run, events = await snapshot(session_factory, run_id)
    assert run.status == "COMPLETED" and run.result_data == FINAL_RESULT
    assert ledger_counts(ledger) == expected_runs  # no completed step was executed again

    # One clean, gap-free history with exactly one completion per step.
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1))
    completed = [e.step_name for e in events if e.event_type == "STEP_COMPLETED"]
    assert completed == STEPS
    assert [e.event_type for e in events].count("WORKFLOW_COMPLETED") == 1
    assert [e.event_type for e in events].count("WORKFLOW_RESUMED") == 1


async def test_known_gap_a_crash_after_a_side_effect_runs_the_step_again(
    session_factory: SessionFactory, migrated_database: str, tmp_path: Path
) -> None:
    """THE gap Phase 3 closes, shown honestly.

    The refund step ran (its effect is in the ledger) but the worker died before STEP_COMPLETED
    was recorded. The history cannot tell "died before the call" from "died after the call", so
    Phase 2 runs the step again and the refund happens TWICE. Idempotency keys (Phase 3) make the
    second call return the first result instead of acting again.
    """
    ledger = tmp_path / "ledger.txt"
    run_id = await new_run(session_factory)

    first = runner(
        "execute", run_id, migrated_database, ledger, "before_step_completed:process_refund"
    )
    assert first.returncode == CRASH_EXIT_CODE
    assert ledger_counts(ledger)["process_refund"] == 1  # the effect happened...
    _, events = await snapshot(session_factory, run_id)
    last = (events[-1].event_type, events[-1].step_name)
    assert last == ("STEP_STARTED", "process_refund")  # ...but was never recorded

    second = runner("resume", run_id, migrated_database, ledger, None)
    assert second.returncode == 0, second.stdout + second.stderr

    assert ledger_counts(ledger) == {**ONCE, "process_refund": 2}  # duplicate side effect
    run, _ = await snapshot(session_factory, run_id)
    assert run.status == "COMPLETED"


async def test_two_workers_die_in_a_row_and_a_third_finishes(
    session_factory: SessionFactory, migrated_database: str, tmp_path: Path
) -> None:
    ledger = tmp_path / "ledger.txt"
    run_id = await new_run(session_factory)

    db = migrated_database
    first = runner("execute", run_id, db, ledger, "after_step_completed:check_eligibility")
    second = runner("resume", run_id, db, ledger, "after_step_completed:process_refund")
    assert first.returncode == CRASH_EXIT_CODE and second.returncode == CRASH_EXIT_CODE
    final = runner("resume", run_id, db, ledger, None)

    assert final.returncode == 0, final.stdout + final.stderr
    assert ledger_counts(ledger) == ONCE
    run, events = await snapshot(session_factory, run_id)
    assert run.result_data == FINAL_RESULT
    assert [e.event_type for e in events].count("WORKFLOW_RESUMED") == 2


async def test_runner_reports_runtime_errors_with_exit_code_2(
    session_factory: SessionFactory, migrated_database: str, tmp_path: Path
) -> None:
    ledger = tmp_path / "ledger.txt"
    run_id = await new_run(session_factory)
    assert runner("execute", run_id, migrated_database, ledger, None).returncode == 0

    again = runner("resume", run_id, migrated_database, ledger, None)  # already COMPLETED
    assert again.returncode == 2
    assert json.loads(again.stdout)["error"] == "InvalidRunStateError"

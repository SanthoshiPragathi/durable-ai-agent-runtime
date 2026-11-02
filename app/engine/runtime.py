"""Creating, executing and resuming workflow runs.

Limitations (all addressed in later phases, none hidden):
  * Execution happens in whatever process calls execute_run()/resume_run(); there are no worker
    processes or leases yet (Phase 4). resume_run() must only be called when the previous
    executor is known to be dead: two live executors of one run would both execute steps.
  * A plain ctx.step that was STARTED but has no recorded outcome (the worker died mid-step) is
    run again, so only repeat-safe work belongs in ctx.step. A ctx.effect step is reconciled
    instead: retried with its idempotency key, checked with its verify hook, or parked as
    OUTCOME_UNKNOWN (run status NEEDS_REVIEW) when nothing can tell whether it happened.
"""

import logging
import uuid
from typing import Any

from app.database.models import EventType, RunStatus, WorkflowRun
from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.errors import (
    InvalidRunStateError,
    NonDeterminismError,
    OutcomeUnknownError,
    RunNotFoundError,
    WorkflowVersionMismatchError,
    format_error,
)
from app.engine.execution_context import DbStepRecorder
from app.engine.failpoints import failpoint
from app.engine.hashing import normalize_json
from app.engine.replay import ReplayHistory, StepStatus
from app.integrations.llm import LLMProvider, get_llm_provider
from app.sdk.context import WorkflowContext
from app.sdk.workflow import WorkflowDefinition, registry

logger = logging.getLogger(__name__)


async def create_run(
    session_factory: SessionFactory,
    workflow_name: str,
    input_data: dict[str, Any],
    parent_run_id: uuid.UUID | None = None,
) -> WorkflowRun:
    """Validate the input and persist a new PENDING run. Does not execute it."""
    definition = registry.get(workflow_name)
    registry.validate_input(workflow_name, input_data)

    async with session_factory() as session, session.begin():
        runs = RunRepository(session)
        await runs.ensure_workflow(definition.name, definition.version)
        return await runs.create_run(
            definition.name, definition.version, input_data, parent_run_id=parent_run_id
        )


async def execute_run(
    session_factory: SessionFactory, run_id: uuid.UUID, *, llm_provider: LLMProvider | None = None
) -> WorkflowRun:
    """Run a PENDING workflow to completion, recording every event. Returns the final run row.

    A workflow that raises is not an error of the runtime: the run is recorded as FAILED and
    returned. Runtime problems (unknown run, wrong state, database down) are raised.
    """
    async with session_factory() as session, session.begin():
        runs = RunRepository(session)
        run = await runs.get_run(run_id)
        if run is None:
            raise RunNotFoundError(f"run {run_id} does not exist")
        definition = registry.get(run.workflow_name)
        input_data = dict(run.input_data)

        # The PENDING check is inside the UPDATE, so a duplicate execute_run loses here.
        if not await runs.mark_running(run_id):
            raise InvalidRunStateError(f"run {run_id} is not PENDING; refusing to start it again")
        await EventRepository(session).append(run_id, EventType.WORKFLOW_STARTED)

    failpoint("after_workflow_started")
    return await _drive(session_factory, run_id, definition, input_data, ReplayHistory(), llm_provider)


async def resume_run(
    session_factory: SessionFactory, run_id: uuid.UUID, *, llm_provider: LLMProvider | None = None
) -> WorkflowRun:
    """Continue a RUNNING run whose executor died, from its recorded history.

    The workflow function runs again from the top. Steps that already completed return their saved
    results without executing; execution continues live from the first step with no recorded
    outcome. If the workflow code no longer matches the history, NonDeterminismError is raised and
    the run is left RUNNING (fix or revert the code, then resume again).

    Only call this when the previous executor is dead (leases that prove it arrive in Phase 4).
    """
    async with session_factory() as session, session.begin():
        runs = RunRepository(session)
        run = await runs.get_run(run_id)
        if run is None:
            raise RunNotFoundError(f"run {run_id} does not exist")
        if run.status != RunStatus.RUNNING.value:
            raise InvalidRunStateError(
                f"run {run_id} is {run.status}; only RUNNING runs can be resumed"
            )
        definition = registry.get(run.workflow_name)
        if definition.version != run.workflow_version:
            raise WorkflowVersionMismatchError(
                f"run {run_id} was started with {run.workflow_name!r} v{run.workflow_version} "
                f"but v{definition.version} is registered"
            )

        events = await EventRepository(session).list_events(run_id)
        history = ReplayHistory.from_events(events)
        await EventRepository(session).append(
            run_id,
            EventType.WORKFLOW_RESUMED,
            result_data={
                "recorded_steps": len(history),
                "completed": history.count(StepStatus.COMPLETED),
                "failed": history.count(StepStatus.FAILED),
                "in_doubt": history.count(StepStatus.IN_DOUBT),
            },
        )
        input_data = dict(run.input_data)

    return await _drive(session_factory, run_id, definition, input_data, history, llm_provider)


async def _drive(
    session_factory: SessionFactory,
    run_id: uuid.UUID,
    definition: WorkflowDefinition,
    input_data: dict[str, Any],
    history: ReplayHistory,
    llm_provider: LLMProvider | None,
) -> WorkflowRun:
    ctx = WorkflowContext(
        run_id,
        DbStepRecorder(run_id, session_factory),
        history,
        llm=llm_provider or get_llm_provider(),
    )
    try:
        result = normalize_json(await definition.fn(ctx, **input_data))
        ctx.verify_replay_complete()
    except Exception as exc:
        halt = ctx.halt
        if isinstance(halt, NonDeterminismError):
            # The code no longer matches the history. This is not the workflow failing, so the run
            # stays RUNNING for someone to fix the code and resume; it is never silently "failed".
            logger.error("replay diverged", extra={"run_id": str(run_id), "error": str(halt)})
            raise halt from None
        if isinstance(halt, OutcomeUnknownError):
            # The recorder already moved the run to NEEDS_REVIEW. Nothing more executes until a
            # person resolves the step; this is not a failed workflow either.
            logger.warning("run needs review", extra={"run_id": str(run_id), "error": str(halt)})
        else:
            logger.info("workflow run failed", extra={"run_id": str(run_id), "error": repr(exc)})
            await _record_failure(session_factory, run_id, exc)
    else:
        failpoint("before_workflow_completed")
        await _record_success(session_factory, run_id, result)

    async with session_factory() as session:
        final = await RunRepository(session).get_run(run_id)
    assert final is not None
    return final


async def _record_success(session_factory: SessionFactory, run_id: uuid.UUID, result: Any) -> None:
    # Event and status change commit together or not at all.
    async with session_factory() as session, session.begin():
        await EventRepository(session).append(
            run_id, EventType.WORKFLOW_COMPLETED, result_data=result
        )
        await RunRepository(session).mark_completed(run_id, result)


async def _record_failure(
    session_factory: SessionFactory, run_id: uuid.UUID, exc: BaseException
) -> None:
    details = format_error(exc)
    async with session_factory() as session, session.begin():
        await EventRepository(session).append(run_id, EventType.WORKFLOW_FAILED, result_data=details)
        await RunRepository(session).mark_failed(
            run_id, f"{details['error_type']}: {details['message']}"
        )

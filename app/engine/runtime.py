"""Creating, executing and resuming workflow runs.

Two ways to execute a run:
  * Directly: execute_run()/resume_run() in whatever process calls them. Nothing proves the
    previous executor is dead, so resume_run() must only be called when it is known to be, and it
    refuses a run that carries a worker lease.
  * Through workers (engine/worker.py): a worker claims a run with a lease and a fencing token,
    renews the lease while it works, and every write is rejected once another worker has taken the
    run over. This is the safe way to run more than one process. Workers use _drive() below.

Remaining limitations (addressed in later phases, none hidden):
  * A plain ctx.step that was STARTED but has no recorded outcome (the worker died mid-step) is
    run again, so only repeat-safe work belongs in ctx.step. A ctx.effect step is reconciled
    instead: retried with its idempotency key, checked with its verify hook, or parked as
    OUTCOME_UNKNOWN (run status NEEDS_REVIEW) when nothing can tell whether it happened.
"""

import logging
import uuid
from typing import Any

from app.config import get_settings
from app.database.models import EventType, IdempotencyStatus, RunStatus, WorkflowRun
from app.database.repositories import EventRepository, IdempotencyRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.errors import (
    InvalidResolutionError,
    InvalidRunStateError,
    NonDeterminismError,
    OutcomeUnknownError,
    RunNotFoundError,
    RunSuspendedError,
    StaleLeaseError,
    WorkflowNotFoundError,
    WorkflowVersionMismatchError,
    format_error,
    is_infrastructure_error,
)
from app.engine.execution_context import DbStepRecorder
from app.engine.failpoints import failpoint
from app.engine.hashing import normalize_json
from app.engine.lease import Fence
from app.engine.replay import ReplayHistory, StepStatus
from app.integrations.llm import LLMProvider, get_llm_provider
from app.sdk.context import WorkflowContext
from app.sdk.retry import RetryPolicy
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
        definition = registry.get(run.workflow_name, run.workflow_version)
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
        if run.lease_owner is not None:
            raise InvalidRunStateError(
                f"run {run_id} is leased to worker {run.lease_owner!r}; resume_run() would race it. "
                "Let a worker recover the run once its lease expires."
            )
        try:
            definition = registry.get(run.workflow_name, run.workflow_version)
        except WorkflowNotFoundError:
            registered = registry.versions(run.workflow_name)
            if not registered:
                raise
            raise WorkflowVersionMismatchError(
                f"run {run_id} was started with {run.workflow_name!r} v{run.workflow_version} "
                f"but only {', '.join(f'v{v}' for v in registered)} is registered"
            ) from None

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


RESOLUTIONS = ("completed", "failed", "not_executed")


async def resolve_unknown_step(
    session_factory: SessionFactory,
    run_id: uuid.UUID,
    step_name: str,
    *,
    outcome: str,
    result: Any = None,
    message: str | None = None,
    resolved_by: str = "operator",
    make_claimable: bool = False,
) -> WorkflowRun:
    """Record a reviewer's answer for a step whose outcome was unknown, and reopen the run.

    outcome="completed"     the effect DID happen; `result` is what the step should have returned
    outcome="failed"        the effect did not happen and should not be retried; the step fails
    outcome="not_executed"  the effect did NOT happen; it is safe to run the step now

    The run moves NEEDS_REVIEW -> RUNNING. Call resume_run to continue it directly, or pass
    make_claimable=True to let a worker pick it up. The decision is recorded
    as a STEP_RESOLVED event (with who made it) so the history stays complete and replayable.
    """
    if outcome not in RESOLUTIONS:
        raise InvalidResolutionError(f"outcome must be one of {RESOLUTIONS}, not {outcome!r}")
    normalized = normalize_json(result) if outcome == "completed" else None

    async with session_factory() as session, session.begin():
        runs = RunRepository(session)
        run = await runs.get_run(run_id)
        if run is None:
            raise RunNotFoundError(f"run {run_id} does not exist")
        if run.status != RunStatus.NEEDS_REVIEW.value:
            raise InvalidRunStateError(f"run {run_id} is {run.status}, not NEEDS_REVIEW")

        history = ReplayHistory.from_events(await EventRepository(session).list_events(run_id))
        step = next((s for s in history.steps if s.name == step_name), None)
        if step is None or step.status != StepStatus.UNKNOWN:
            raise InvalidResolutionError(f"step {step_name!r} is not awaiting resolution")

        await EventRepository(session).append(
            run_id,
            EventType.STEP_RESOLVED,
            step_name=step_name,
            result_data={
                "outcome": outcome,
                "result": normalized,
                "message": message,
                "resolved_by": resolved_by,
            },
        )
        ops = IdempotencyRepository(session)
        key = f"{run_id}:{step_name}"
        if await ops.get(key) is not None:
            if outcome == "completed":
                await ops.set_status(key, IdempotencyStatus.RECONCILED, normalized)
            elif outcome == "failed":
                await ops.set_status(key, IdempotencyStatus.FAILED)
        await runs.reopen(run_id, claimable=make_claimable)
        reopened = await runs.get_run(run_id)
        assert reopened is not None
        await session.refresh(reopened)  # the UPDATE used SQL expressions; reload before the session closes
    return reopened


async def _drive(
    session_factory: SessionFactory,
    run_id: uuid.UUID,
    definition: WorkflowDefinition,
    input_data: dict[str, Any],
    history: ReplayHistory,
    llm_provider: LLMProvider | None,
    fence: Fence | None = None,
) -> WorkflowRun:
    ctx = WorkflowContext(
        run_id,
        DbStepRecorder(run_id, session_factory, fence),
        history,
        llm=llm_provider or get_llm_provider(),
        retry_policy=RetryPolicy.from_settings(get_settings()),
    )
    try:
        result = normalize_json(await definition.fn(ctx, **input_data))
        ctx.verify_replay_complete()
    except Exception as exc:
        if is_infrastructure_error(exc):
            # The database (or the network to it) failed. Do not record a failure: leave the run as
            # it is and let the caller decide (a worker abandons it; its lease then lapses).
            logger.warning("infrastructure error, run left as is", extra={"run_id": str(run_id)})
            raise
        halt = ctx.halt
        if isinstance(halt, StaleLeaseError) or isinstance(exc, StaleLeaseError):
            # Another worker owns the run now. Record nothing (every write would be rejected anyway)
            # and let the worker stand down.
            raise (halt if isinstance(halt, StaleLeaseError) else exc) from None
        if isinstance(halt, NonDeterminismError):
            # The code no longer matches the history. This is not the workflow failing, so the run
            # stays RUNNING for someone to fix the code and resume; it is never silently "failed".
            logger.error("replay diverged", extra={"run_id": str(run_id), "error": str(halt)})
            raise halt from None
        if isinstance(halt, RunSuspendedError):
            # The recorder parked the run as WAITING and released its lease. Not a failure: a timer
            # or a signal will make it claimable again and a worker will replay it from the history.
            logger.info("run waiting", extra={"run_id": str(run_id), "step": halt.step_name})
        elif isinstance(halt, OutcomeUnknownError):
            # The recorder already moved the run to NEEDS_REVIEW. Nothing more executes until a
            # person resolves the step; this is not a failed workflow either.
            logger.warning("run needs review", extra={"run_id": str(run_id), "error": str(halt)})
        else:
            logger.info("workflow run failed", extra={"run_id": str(run_id), "error": repr(exc)})
            await _record_failure(session_factory, run_id, exc, fence)
    else:
        failpoint("before_workflow_completed")
        await _record_success(session_factory, run_id, result, fence)

    async with session_factory() as session:
        final = await RunRepository(session).get_run(run_id)
    assert final is not None
    return final


async def _record_success(
    session_factory: SessionFactory, run_id: uuid.UUID, result: Any, fence: Fence | None = None
) -> None:
    # Event and status change commit together or not at all.
    async with session_factory() as session, session.begin():
        if fence is not None:
            await RunRepository(session).check_fence(run_id, fence)
        await EventRepository(session).append(
            run_id, EventType.WORKFLOW_COMPLETED, result_data=result
        )
        await RunRepository(session).mark_completed(run_id, result)


async def _record_failure(
    session_factory: SessionFactory,
    run_id: uuid.UUID,
    exc: BaseException,
    fence: Fence | None = None,
) -> None:
    details = format_error(exc)
    async with session_factory() as session, session.begin():
        if fence is not None:
            await RunRepository(session).check_fence(run_id, fence)
        await EventRepository(session).append(run_id, EventType.WORKFLOW_FAILED, result_data=details)
        await RunRepository(session).mark_failed(
            run_id, f"{details['error_type']}: {details['message']}"
        )

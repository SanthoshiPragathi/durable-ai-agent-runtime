"""Creating and executing workflow runs.

Phase 1 limitations (all addressed in later phases, none hidden):
  * Execution happens in whatever process calls execute_run(); there are no worker processes,
    leases or crash recovery yet (Phases 4 and 2).
  * ctx.step() records but does not replay: a run that dies midway stays RUNNING and cannot be
    resumed yet (Phase 2).
"""

import logging
import uuid
from typing import Any

from app.database.models import EventType, WorkflowRun
from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.errors import InvalidRunStateError, RunNotFoundError, format_error
from app.engine.execution_context import DbStepRecorder
from app.engine.hashing import normalize_json
from app.sdk.context import WorkflowContext
from app.sdk.workflow import registry

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


async def execute_run(session_factory: SessionFactory, run_id: uuid.UUID) -> WorkflowRun:
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

    ctx = WorkflowContext(run_id, DbStepRecorder(run_id, session_factory))
    try:
        result = normalize_json(await definition.fn(ctx, **input_data))
    except Exception as exc:
        logger.info("workflow run failed", extra={"run_id": str(run_id), "error": repr(exc)})
        await _record_failure(session_factory, run_id, exc)
    else:
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

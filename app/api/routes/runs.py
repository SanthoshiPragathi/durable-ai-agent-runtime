import logging
import uuid
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

from app.api.deps import get_session_factory
from app.api.schemas import ErrorOut, EventOut, ExternalOperationOut, ResolveStepRequest, RunOut
from app.database.models import RunStatus
from app.database.repositories import EventRepository, IdempotencyRepository, RunRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.errors import RunNotFoundError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/runs", tags=["runs"])


async def _resume_in_background(session_factory: SessionFactory, run_id: uuid.UUID) -> None:
    # TEMPORARY (until Phase 4 workers): resumes inside the API process after a reviewer's decision.
    try:
        await runtime.resume_run(session_factory, run_id)
    except Exception:
        logger.exception("background resume failed", extra={"run_id": str(run_id)})


@router.get("", response_model=list[RunOut], summary="List runs, newest first")
async def list_runs(
    status: RunStatus | None = None,
    workflow_name: str | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    session_factory: SessionFactory = Depends(get_session_factory),
) -> list[RunOut]:
    async with session_factory() as session:
        runs = await RunRepository(session).list_runs(
            status=status.value if status else None,
            workflow_name=workflow_name,
            limit=limit,
            offset=offset,
        )
    return [RunOut.model_validate(r) for r in runs]


@router.get("/{run_id}", response_model=RunOut, responses={404: {"model": ErrorOut}}, summary="Get a run")
async def get_run(
    run_id: uuid.UUID, session_factory: SessionFactory = Depends(get_session_factory)
) -> RunOut:
    async with session_factory() as session:
        run = await RunRepository(session).get_run(run_id)
    if run is None:
        raise RunNotFoundError(f"run {run_id} does not exist")
    return RunOut.model_validate(run)


@router.get(
    "/{run_id}/events",
    response_model=list[EventOut],
    responses={404: {"model": ErrorOut}},
    summary="The run's event timeline, in order",
)
async def get_run_events(
    run_id: uuid.UUID, session_factory: SessionFactory = Depends(get_session_factory)
) -> list[EventOut]:
    async with session_factory() as session:
        if await RunRepository(session).get_run(run_id) is None:
            raise RunNotFoundError(f"run {run_id} does not exist")
        events = await EventRepository(session).list_events(run_id)
    return [EventOut.model_validate(e) for e in events]


@router.get(
    "/{run_id}/external-operations",
    response_model=list[ExternalOperationOut],
    responses={404: {"model": ErrorOut}},
    summary="Status of the run's external operations (idempotency records)",
)
async def get_external_operations(
    run_id: uuid.UUID, session_factory: SessionFactory = Depends(get_session_factory)
) -> list[ExternalOperationOut]:
    async with session_factory() as session:
        if await RunRepository(session).get_run(run_id) is None:
            raise RunNotFoundError(f"run {run_id} does not exist")
        records = await IdempotencyRepository(session).list_for_run(run_id)
    return [ExternalOperationOut.model_validate(r) for r in records]


@router.post(
    "/{run_id}/steps/{step_name}/resolve",
    response_model=RunOut,
    status_code=202,
    responses={404: {"model": ErrorOut}, 409: {"model": ErrorOut}, 422: {"model": ErrorOut}},
    summary="Resolve a step whose outcome is unknown and resume the run",
)
async def resolve_step(
    run_id: uuid.UUID,
    step_name: str,
    body: ResolveStepRequest,
    background_tasks: BackgroundTasks,
    session_factory: SessionFactory = Depends(get_session_factory),
) -> RunOut:
    if body.outcome == "completed" and "result" not in body.model_fields_set:
        raise HTTPException(422, "outcome 'completed' requires a `result`")
    run = await runtime.resolve_unknown_step(
        session_factory,
        run_id,
        step_name,
        outcome=body.outcome,
        result=body.result,
        message=body.message,
        resolved_by=body.resolved_by,
    )
    background_tasks.add_task(_resume_in_background, session_factory, run_id)
    return RunOut.model_validate(run)

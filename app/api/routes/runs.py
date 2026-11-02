import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.api.deps import get_session_factory
from app.api.schemas import ErrorOut, EventOut, RunOut
from app.database.models import RunStatus
from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.errors import RunNotFoundError

router = APIRouter(prefix="/runs", tags=["runs"])


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

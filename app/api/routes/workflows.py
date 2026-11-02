import logging
import uuid

from fastapi import APIRouter, BackgroundTasks, Depends

from app.api.deps import get_session_factory
from app.api.schemas import CreateRunRequest, ErrorOut, RunOut, WorkflowOut
from app.database.session import SessionFactory
from app.engine import runtime
from app.sdk.workflow import registry

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/workflows", tags=["workflows"])


@router.get("", response_model=list[WorkflowOut], summary="List registered workflows")
async def list_workflows() -> list[WorkflowOut]:
    return [WorkflowOut(name=d.name, version=d.version) for d in registry.list()]


async def _execute_in_background(session_factory: SessionFactory, run_id: uuid.UUID) -> None:
    # TEMPORARY (Phase 1): runs inside the API process. Phase 4 replaces this with worker processes.
    try:
        await runtime.execute_run(session_factory, run_id)
    except Exception:
        logger.exception("background execution failed", extra={"run_id": str(run_id)})


@router.post(
    "/{workflow_name}/runs",
    response_model=RunOut,
    status_code=202,
    responses={404: {"model": ErrorOut}, 422: {"model": ErrorOut}},
    summary="Start a workflow run",
)
async def start_run(
    workflow_name: str,
    body: CreateRunRequest,
    background_tasks: BackgroundTasks,
    session_factory: SessionFactory = Depends(get_session_factory),
) -> RunOut:
    run = await runtime.create_run(session_factory, workflow_name, body.input)
    background_tasks.add_task(_execute_in_background, session_factory, run.run_id)
    return RunOut.model_validate(run)

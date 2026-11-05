from datetime import UTC, datetime

from fastapi import APIRouter, Depends

from app.api.deps import get_session_factory
from app.api.schemas import WorkerOut
from app.config import get_settings
from app.database.repositories import WorkerRepository
from app.database.session import SessionFactory

router = APIRouter(prefix="/workers", tags=["workers"])


@router.get("", response_model=list[WorkerOut], summary="Worker processes that have registered")
async def list_workers(session_factory: SessionFactory = Depends(get_session_factory)) -> list[WorkerOut]:
    async with session_factory() as session:
        workers = await WorkerRepository(session).list_workers()
    now = datetime.now(UTC)
    window = get_settings().lease_duration_seconds
    return [
        WorkerOut(
            worker_id=w.worker_id,
            hostname=w.hostname,
            pid=w.pid,
            status=w.status,
            started_at=w.started_at,
            last_heartbeat_at=w.last_heartbeat_at,
            alive=w.status == "ACTIVE" and (now - w.last_heartbeat_at).total_seconds() < window,
        )
        for w in workers
    ]

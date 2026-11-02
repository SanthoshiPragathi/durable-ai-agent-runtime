"""PostgreSQL-backed StepRecorder.

Every call commits its own short transaction. That is the durability rule: once ctx.step() has
moved past a recording call, the event is on disk, so a crash cannot lose it.
"""

import uuid
from typing import Any

from app.database.models import EventType
from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.errors import format_error


class DbStepRecorder:
    def __init__(self, run_id: uuid.UUID, session_factory: SessionFactory) -> None:
        self._run_id = run_id
        self._session_factory = session_factory

    async def step_started(self, step_name: str, input_hash: str) -> None:
        async with self._session_factory() as session, session.begin():
            await EventRepository(session).append(
                self._run_id, EventType.STEP_STARTED, step_name=step_name, input_hash=input_hash
            )
            await RunRepository(session).set_current_step(self._run_id, step_name)

    async def step_completed(self, step_name: str, result: Any) -> None:
        async with self._session_factory() as session, session.begin():
            await EventRepository(session).append(
                self._run_id, EventType.STEP_COMPLETED, step_name=step_name, result_data=result
            )

    async def step_failed(self, step_name: str, error: BaseException) -> None:
        async with self._session_factory() as session, session.begin():
            await EventRepository(session).append(
                self._run_id,
                EventType.STEP_FAILED,
                step_name=step_name,
                result_data=format_error(error),
            )

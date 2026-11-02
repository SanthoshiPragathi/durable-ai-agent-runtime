"""Data access. Repositories never commit: the caller owns the transaction boundary, so related
changes (e.g. a completion event plus the run's status) commit or roll back together."""

import uuid
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import EventType, RunStatus, Workflow, WorkflowEvent, WorkflowRun
from app.engine.errors import DuplicateEventError, InvalidRunStateError, RunNotFoundError


class RunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def ensure_workflow(self, name: str, version: int) -> None:
        """Register (name, version) if it is not already known."""
        existing = await self._session.get(Workflow, (name, version))
        if existing is None:
            self._session.add(Workflow(name=name, version=version))
            await self._session.flush()

    async def create_run(
        self,
        workflow_name: str,
        workflow_version: int,
        input_data: dict[str, Any],
        parent_run_id: uuid.UUID | None = None,
    ) -> WorkflowRun:
        run = WorkflowRun(
            workflow_name=workflow_name,
            workflow_version=workflow_version,
            status=RunStatus.PENDING.value,
            input_data=input_data,
            parent_run_id=parent_run_id,
        )
        self._session.add(run)
        await self._session.flush()
        await self._session.refresh(run)  # load server-generated created_at
        return run

    async def get_run(self, run_id: uuid.UUID) -> WorkflowRun | None:
        return await self._session.get(WorkflowRun, run_id)

    async def list_runs(
        self,
        *,
        status: str | None = None,
        workflow_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[WorkflowRun]:
        stmt = select(WorkflowRun).order_by(WorkflowRun.created_at.desc(), WorkflowRun.run_id)
        if status is not None:
            stmt = stmt.where(WorkflowRun.status == status)
        if workflow_name is not None:
            stmt = stmt.where(WorkflowRun.workflow_name == workflow_name)
        result = await self._session.execute(stmt.limit(limit).offset(offset))
        return list(result.scalars())

    async def mark_running(self, run_id: uuid.UUID) -> bool:
        """PENDING -> RUNNING. Returns False if the run was not PENDING.

        The state check lives in the UPDATE itself, so two callers racing to start the same run
        cannot both succeed: PostgreSQL lets exactly one of them match the WHERE clause.
        """
        result = await self._session.execute(
            update(WorkflowRun)
            .where(WorkflowRun.run_id == run_id, WorkflowRun.status == RunStatus.PENDING.value)
            .values(status=RunStatus.RUNNING.value, started_at=func.now())
            .returning(WorkflowRun.run_id)
        )
        return result.scalar_one_or_none() is not None

    async def set_current_step(self, run_id: uuid.UUID, step_name: str) -> None:
        await self._session.execute(
            update(WorkflowRun).where(WorkflowRun.run_id == run_id).values(current_step=step_name)
        )

    async def mark_completed(self, run_id: uuid.UUID, result: Any) -> None:
        await self._finish(run_id, RunStatus.COMPLETED, result_data=result)

    async def mark_failed(self, run_id: uuid.UUID, error_message: str) -> None:
        await self._finish(run_id, RunStatus.FAILED, error_message=error_message)

    async def _finish(self, run_id: uuid.UUID, status: RunStatus, **values: Any) -> None:
        result = await self._session.execute(
            update(WorkflowRun)
            .where(WorkflowRun.run_id == run_id, WorkflowRun.status == RunStatus.RUNNING.value)
            .values(status=status.value, completed_at=func.now(), **values)
            .returning(WorkflowRun.run_id)
        )
        if result.scalar_one_or_none() is None:
            raise InvalidRunStateError(f"run {run_id} is not RUNNING; cannot mark {status.value}")


class EventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(
        self,
        run_id: uuid.UUID,
        event_type: EventType,
        *,
        step_name: str | None = None,
        input_hash: str | None = None,
        result_data: Any = None,
    ) -> WorkflowEvent:
        """Append one event with the next sequence number for the run.

        Concurrency: we lock the run's row first (SELECT ... FOR UPDATE). Two writers appending
        to the same run take turns, so "max(sequence_number) + 1" cannot be computed twice for
        the same value. Appends to different runs do not block each other. The lock is released
        when the caller's transaction commits.
        """
        locked = await self._session.execute(
            select(WorkflowRun.run_id).where(WorkflowRun.run_id == run_id).with_for_update()
        )
        if locked.scalar_one_or_none() is None:
            raise RunNotFoundError(f"run {run_id} does not exist")

        next_seq = (
            await self._session.execute(
                select(func.coalesce(func.max(WorkflowEvent.sequence_number), 0) + 1).where(
                    WorkflowEvent.run_id == run_id
                )
            )
        ).scalar_one()

        event = WorkflowEvent(
            run_id=run_id,
            sequence_number=next_seq,
            event_type=event_type.value,
            step_name=step_name,
            input_hash=input_hash,
            result_data=result_data,
        )
        self._session.add(event)
        try:
            await self._session.flush()
        except IntegrityError as exc:
            # The row lock rules out sequence collisions, so this is the "step completed twice"
            # index (or a check constraint). The caller's transaction must roll back.
            raise DuplicateEventError(
                f"event {event_type.value} for step {step_name!r} violates an event-log rule"
            ) from exc
        await self._session.refresh(event)  # load server-generated created_at
        return event

    async def list_events(self, run_id: uuid.UUID) -> list[WorkflowEvent]:
        result = await self._session.execute(
            select(WorkflowEvent)
            .where(WorkflowEvent.run_id == run_id)
            .order_by(WorkflowEvent.sequence_number)
        )
        return list(result.scalars())

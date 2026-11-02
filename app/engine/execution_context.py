"""PostgreSQL-backed StepRecorder.

Every call commits its own short transaction. That is the durability rule: once ctx has moved past a
recording call, the event is on disk, so a crash cannot lose it. Where a step has an idempotency
key, its idempotency_records row is updated in the SAME transaction as the matching event, so the
event log and the operation's status can never disagree.
"""

import uuid
from typing import Any

from app.database.models import EventType, IdempotencyStatus, RunStatus
from app.database.repositories import EventRepository, IdempotencyRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.errors import format_error
from app.engine.failpoints import failpoint


class DbStepRecorder:
    def __init__(self, run_id: uuid.UUID, session_factory: SessionFactory) -> None:
        self._run_id = run_id
        self._session_factory = session_factory

    async def step_started(
        self, step_name: str, input_hash: str, idempotency_key: str | None = None
    ) -> None:
        failpoint("before_step_started", step_name)
        async with self._session_factory() as session, session.begin():
            await EventRepository(session).append(
                self._run_id, EventType.STEP_STARTED, step_name=step_name, input_hash=input_hash
            )
            await RunRepository(session).set_current_step(self._run_id, step_name)
            if idempotency_key:
                await IdempotencyRepository(session).mark_in_flight(
                    idempotency_key, self._run_id, step_name
                )
        failpoint("after_step_started", step_name)

    async def step_completed(
        self,
        step_name: str,
        result: Any,
        *,
        idempotency_key: str | None = None,
        reconciled: bool = False,
    ) -> None:
        # The step function has already run: a crash here is the dangerous window where its effect
        # happened but was never recorded.
        failpoint("before_step_completed", step_name)
        async with self._session_factory() as session, session.begin():
            await EventRepository(session).append(
                self._run_id, EventType.STEP_COMPLETED, step_name=step_name, result_data=result
            )
            if idempotency_key:
                status = IdempotencyStatus.RECONCILED if reconciled else IdempotencyStatus.SUCCEEDED
                await IdempotencyRepository(session).set_status(idempotency_key, status, result)
        failpoint("after_step_completed", step_name)

    async def step_failed(
        self, step_name: str, error: BaseException, *, idempotency_key: str | None = None
    ) -> None:
        async with self._session_factory() as session, session.begin():
            await EventRepository(session).append(
                self._run_id,
                EventType.STEP_FAILED,
                step_name=step_name,
                result_data=format_error(error),
            )
            if idempotency_key:
                await IdempotencyRepository(session).set_status(
                    idempotency_key, IdempotencyStatus.FAILED
                )

    async def step_outcome_unknown(
        self,
        step_name: str,
        reason: str,
        *,
        idempotency_key: str | None = None,
        already_recorded: bool = False,
    ) -> None:
        """Record that a step's outcome is unknown and park the run in NEEDS_REVIEW.

        Event, idempotency status and run status change in one transaction. With
        already_recorded=True (a replay found the event already in the history) only the run status
        is repaired, in case a crash separated the two.
        """
        async with self._session_factory() as session, session.begin():
            runs = RunRepository(session)
            if not already_recorded:
                await EventRepository(session).append(
                    self._run_id,
                    EventType.STEP_OUTCOME_UNKNOWN,
                    step_name=step_name,
                    result_data={"reason": reason},
                )
                if idempotency_key:
                    await IdempotencyRepository(session).set_status(
                        idempotency_key, IdempotencyStatus.UNKNOWN
                    )
            run = await runs.get_run(self._run_id)
            if run is not None and run.status == RunStatus.RUNNING.value:
                await runs.mark_needs_review(self._run_id)

    async def effect_attempt(self, step_name: str, idempotency_key: str) -> None:
        async with self._session_factory() as session, session.begin():
            await IdempotencyRepository(session).add_attempt(idempotency_key)

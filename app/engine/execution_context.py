"""PostgreSQL-backed StepRecorder.

Every call commits its own short transaction. That is the durability rule: once ctx has moved past a
recording call, the event is on disk, so a crash cannot lose it. Where a step has an idempotency
key, its idempotency_records row is updated in the SAME transaction as the matching event, so the
event log and the operation's status can never disagree.

With a Fence (a worker executing a claimed run), every transaction first re-checks, under the run's
row lock, that the worker still holds the lease; a stale worker's write raises StaleLeaseError and
commits nothing.
"""

import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import EventType, IdempotencyStatus, RunStatus, TimerStatus
from app.database.repositories import (
    EventRepository,
    IdempotencyRepository,
    RunRepository,
    WaitRepository,
)
from app.database.session import SessionFactory
from app.engine.errors import format_error
from app.engine.failpoints import failpoint
from app.engine.lease import Fence
from app.sdk.context import WaitOutcome


class DbStepRecorder:
    def __init__(
        self, run_id: uuid.UUID, session_factory: SessionFactory, fence: Fence | None = None
    ) -> None:
        self._run_id = run_id
        self._session_factory = session_factory
        self._fence = fence

    async def _guard(self, session: AsyncSession) -> None:
        """First statement of every write transaction when running under a lease."""
        if self._fence is not None:
            await RunRepository(session).check_fence(self._run_id, self._fence)

    async def ensure_lease(self) -> None:
        """Raise StaleLeaseError if this worker no longer holds the run. Called just before a step's
        function runs, so a worker that was paused past its lease does not act in the world on behalf
        of a run that now belongs to someone else. It narrows that window; it cannot close it (the
        lease can still lapse a moment after this check), which is why effects also carry
        idempotency keys."""
        if self._fence is None:
            return
        async with self._session_factory() as session, session.begin():
            await self._guard(session)

    async def step_started(
        self, step_name: str, input_hash: str, idempotency_key: str | None = None
    ) -> None:
        failpoint("before_step_started", step_name)
        async with self._session_factory() as session, session.begin():
            await self._guard(session)
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
            await self._guard(session)
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
            await self._guard(session)
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
            await self._guard(session)
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
            await self._guard(session)
            await IdempotencyRepository(session).add_attempt(idempotency_key)

    async def wait(
        self, step_name: str, kind: str, *, signal_name: str | None, seconds: float | None
    ) -> WaitOutcome:
        """Evaluate a sleep / signal wait and either complete it or park the run, in ONE transaction
        that holds the run's row lock.

        The lock is what prevents a lost wakeup: send_signal and the timer firer take the same lock,
        so a signal either committed before this transaction (we see it and complete), or it arrives
        after we committed WAITING (it sees WAITING and wakes the run). There is no gap between.
        Order of checks: a stored signal wins over an expired timeout.
        """
        failpoint("before_wait_recorded", step_name)
        async with self._session_factory() as session, session.begin():
            await self._guard(session)  # with a fence this already locked the run row
            runs, waits, events = RunRepository(session), WaitRepository(session), EventRepository(session)
            await runs.lock_run(self._run_id)

            timer = None
            due = False
            if seconds is not None:
                timer_kind = "SLEEP" if kind == "sleep" else "SIGNAL_TIMEOUT"
                timer, due = await waits.ensure_timer(self._run_id, step_name, timer_kind, seconds)

            result: Any = None
            done = False
            if signal_name is not None:
                signal = await waits.oldest_unconsumed(self._run_id, signal_name)
                if signal is not None:
                    await waits.consume(signal.signal_id, step_name)
                    if timer is not None:
                        await waits.set_timer_status(timer, TimerStatus.CANCELLED)
                    result = {"received": True, "payload": signal.payload, "signal_id": signal.signal_id}
                    done = True
            if not done and timer is not None and due:
                await waits.set_timer_status(timer, TimerStatus.FIRED)
                if kind == "sleep":
                    result = {"fire_at": timer.fire_at.isoformat()}
                else:
                    result = {"received": False, "timed_out": True}
                done = True

            if done:
                await events.append(
                    self._run_id, EventType.STEP_COMPLETED, step_name=step_name, result_data=result
                )
            else:
                await events.append(
                    self._run_id,
                    EventType.WORKFLOW_WAITING,
                    step_name=step_name,
                    result_data={
                        "kind": kind,
                        "signal": signal_name,
                        "fire_at": timer.fire_at.isoformat() if timer is not None else None,
                    },
                )
                await runs.mark_waiting(self._run_id, signal_name)
        failpoint("after_wait_recorded", step_name)
        return WaitOutcome(done, result)

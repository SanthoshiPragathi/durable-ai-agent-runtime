"""Data access. Repositories never commit: the caller owns the transaction boundary, so related
changes (e.g. a completion event plus the run's status) commit or roll back together."""

import uuid
from typing import Any

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import (
    EventType,
    IdempotencyRecord,
    IdempotencyStatus,
    RunStatus,
    TimerStatus,
    Worker,
    Workflow,
    WorkflowEvent,
    WorkflowRun,
    WorkflowSignal,
    WorkflowTimer,
)
from app.engine.errors import (
    DuplicateEventError,
    InvalidRunStateError,
    RunNotFoundError,
    StaleLeaseError,
)
from app.engine.lease import Fence


def _seconds(value: float):
    """A SQL interval of `value` seconds."""
    return func.make_interval(0, 0, 0, 0, 0, 0, value)


_NO_LEASE = {"lease_owner": None, "lease_expires_at": None}

# Claim one run: a never-started run, or a RUNNING run whose lease lapsed (its worker died or
# stalled). FOR UPDATE SKIP LOCKED makes concurrent claimers pass over rows another claimer (or
# an in-flight write by the old owner) holds, so no run is handed to two workers. now() is the
# database's clock, so workers with skewed clocks cannot disagree about expiry. A NULL
# lease_expires_at means "executed directly, not by a worker" and is never claimed.
_CLAIM_SQL = text(
    """
    UPDATE workflow_runs AS r
    SET lease_owner = :worker_id,
        lease_expires_at = now() + make_interval(secs => :lease_seconds),
        fencing_token = r.fencing_token + 1,
        status = 'RUNNING',
        started_at = COALESCE(r.started_at, now())
    FROM (
        SELECT run_id, status AS prior_status
        FROM workflow_runs
        WHERE (workflow_name, workflow_version) IN (
                  SELECT * FROM unnest(CAST(:names AS text[]), CAST(:versions AS int[])))
          AND (status = 'PENDING'
               OR (status = 'RUNNING' AND lease_expires_at IS NOT NULL AND lease_expires_at < now()))
        ORDER BY created_at, run_id
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    ) AS c
    WHERE r.run_id = c.run_id
    RETURNING r.run_id, r.fencing_token, c.prior_status, r.workflow_name, r.workflow_version,
              r.input_data, r.lease_expires_at
    """
)


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

    # ---- leases and fencing (Phase 4) ------------------------------------------------------

    async def claim_next(
        self,
        worker_id: str,
        *,
        lease_seconds: float,
        workflows: list[tuple[str, int]],
    ) -> dict[str, Any] | None:
        """Atomically take one claimable run for `worker_id`. Returns its details, or None.

        `workflows` lists the (name, version) pairs this worker's code can run; others are skipped.
        The returned dict has run_id, fencing_token, resumed (True if this is a takeover of a run
        a previous worker had started), workflow_name, workflow_version, input_data.
        """
        if not workflows:
            return None
        row = (
            (
                await self._session.execute(
                    _CLAIM_SQL,
                    {
                        "worker_id": worker_id,
                        "lease_seconds": lease_seconds,
                        "names": [name for name, _ in workflows],
                        "versions": [version for _, version in workflows],
                    },
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return {
            "run_id": row["run_id"],
            "fencing_token": row["fencing_token"],
            "resumed": row["prior_status"] == RunStatus.RUNNING.value,
            "workflow_name": row["workflow_name"],
            "workflow_version": row["workflow_version"],
            "input_data": row["input_data"],
        }

    async def renew_lease(self, run_id: uuid.UUID, fence: Fence, lease_seconds: float) -> bool:
        """Extend the lease. False means the caller no longer holds it (reclaimed or finished)."""
        result = await self._session.execute(
            update(WorkflowRun)
            .where(
                WorkflowRun.run_id == run_id,
                WorkflowRun.lease_owner == fence.worker_id,
                WorkflowRun.fencing_token == fence.token,
                WorkflowRun.status == RunStatus.RUNNING.value,
            )
            .values(lease_expires_at=func.now() + _seconds(lease_seconds))
            .returning(WorkflowRun.run_id)
        )
        return result.scalar_one_or_none() is not None

    async def released_by(self, run_id: uuid.UUID, fence: Fence) -> bool:
        """True if the run's lease was released by the holder of `fence` itself: the run finished or
        was parked in NEEDS_REVIEW under the same token and nobody has claimed it since. This is how
        a worker tells "I am done" apart from "someone took my run" when a renewal is refused."""
        row = (
            await self._session.execute(
                select(WorkflowRun.fencing_token, WorkflowRun.lease_owner).where(
                    WorkflowRun.run_id == run_id
                )
            )
        ).first()
        return row is not None and row[0] == fence.token and row[1] is None

    async def check_fence(self, run_id: uuid.UUID, fence: Fence) -> None:
        """Raise StaleLeaseError unless `fence` is still the run's current lease holder.

        Takes the run's row lock (the same lock event appends and claims use), so the check and the
        writes that follow it in this transaction cannot interleave with a takeover: either the
        takeover committed first and this fails, or this transaction commits first and the
        takeover waits for it.
        """
        row = (
            await self._session.execute(
                select(WorkflowRun.fencing_token, WorkflowRun.lease_owner)
                .where(WorkflowRun.run_id == run_id)
                .with_for_update()
            )
        ).first()
        if row is None:
            raise RunNotFoundError(f"run {run_id} does not exist")
        token, owner = row
        if token != fence.token or owner != fence.worker_id:
            raise StaleLeaseError(
                f"worker {fence.worker_id!r} holds token {fence.token} for run {run_id}, but the "
                f"run is now at token {token} (owner {owner!r}); write rejected"
            )

    async def count_unfinished(self) -> int:
        """Work a draining worker should still wait for: PENDING or RUNNING runs, plus timers that
        have not fired (a sleeping run will need a worker once its timer is due). Runs waiting for a
        signal or a reviewer are not counted: no worker can advance them."""
        runs = await self._session.execute(
            select(func.count())
            .select_from(WorkflowRun)
            .where(WorkflowRun.status.in_([RunStatus.PENDING.value, RunStatus.RUNNING.value]))
        )
        timers = await self._session.execute(
            select(func.count())
            .select_from(WorkflowTimer)
            .join(WorkflowRun, WorkflowRun.run_id == WorkflowTimer.run_id)
            .where(
                WorkflowTimer.status == TimerStatus.PENDING.value,
                WorkflowRun.status == RunStatus.WAITING.value,
            )
        )
        return runs.scalar_one() + timers.scalar_one()

    # ---- waiting (Phase 5) -----------------------------------------------------------------

    async def lock_run(self, run_id: uuid.UUID) -> tuple[str, str | None]:
        """Take the run's row lock and return (status, waiting_signal). Every wait, signal and timer
        transaction starts here, which is what serialises "suspend" against "wake"."""
        row = (
            await self._session.execute(
                select(WorkflowRun.status, WorkflowRun.waiting_signal)
                .where(WorkflowRun.run_id == run_id)
                .with_for_update()
            )
        ).first()
        if row is None:
            raise RunNotFoundError(f"run {run_id} does not exist")
        return row[0], row[1]

    async def mark_waiting(self, run_id: uuid.UUID, waiting_signal: str | None) -> None:
        """RUNNING -> WAITING. Releases the lease: a waiting run belongs to no worker."""
        await self._transition(
            run_id, RunStatus.RUNNING, RunStatus.WAITING, waiting_signal=waiting_signal, **_NO_LEASE
        )

    async def wake(self, run_id: uuid.UUID, *, signal_name: str | None = None) -> bool:
        """WAITING -> RUNNING with an already expired lease and no owner, i.e. claimable by any
        worker, which replays the history (WORKFLOW_RESUMED). With signal_name, only wakes a run that
        is waiting for that signal. Returns whether the run was woken."""
        conditions = [WorkflowRun.run_id == run_id, WorkflowRun.status == RunStatus.WAITING.value]
        if signal_name is not None:
            conditions.append(WorkflowRun.waiting_signal == signal_name)
        result = await self._session.execute(
            update(WorkflowRun)
            .where(*conditions)
            .values(
                status=RunStatus.RUNNING.value,
                lease_owner=None,
                lease_expires_at=func.now() - _seconds(1),
                waiting_signal=None,
            )
            .returning(WorkflowRun.run_id)
        )
        return result.scalar_one_or_none() is not None

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

    async def mark_needs_review(self, run_id: uuid.UUID) -> None:
        """RUNNING -> NEEDS_REVIEW: a step has an unknown outcome and execution must stop.
        Releases the lease: nobody executes a run that is waiting for a person."""
        await self._transition(run_id, RunStatus.RUNNING, RunStatus.NEEDS_REVIEW, **_NO_LEASE)

    async def reopen(self, run_id: uuid.UUID, *, claimable: bool = False) -> None:
        """NEEDS_REVIEW -> RUNNING, after a person resolved the unknown step.

        claimable=True also makes the run immediately available to workers (an already-expired
        lease with no owner). Otherwise it is left for a direct resume_run() call.
        """
        extra = {"lease_expires_at": func.now() - _seconds(1)} if claimable else {}
        await self._transition(run_id, RunStatus.NEEDS_REVIEW, RunStatus.RUNNING, **extra)

    async def _transition(
        self, run_id: uuid.UUID, source: RunStatus, target: RunStatus, **values: Any
    ) -> None:
        result = await self._session.execute(
            update(WorkflowRun)
            .where(WorkflowRun.run_id == run_id, WorkflowRun.status == source.value)
            .values(status=target.value, **values)
            .returning(WorkflowRun.run_id)
        )
        if result.scalar_one_or_none() is None:
            raise InvalidRunStateError(f"run {run_id} is not {source.value}; cannot move to {target.value}")

    async def _finish(self, run_id: uuid.UUID, status: RunStatus, **values: Any) -> None:
        result = await self._session.execute(
            update(WorkflowRun)
            .where(WorkflowRun.run_id == run_id, WorkflowRun.status == RunStatus.RUNNING.value)
            .values(status=status.value, completed_at=func.now(), **_NO_LEASE, **values)
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


class WaitRepository:
    """Timers and signals. Always used inside a transaction that holds the run's row lock."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def ensure_timer(
        self, run_id: uuid.UUID, step_name: str, kind: str, seconds: float
    ) -> tuple[WorkflowTimer, bool]:
        """Create the (run, step) timer once, with fire_at taken from the database clock, and return
        (timer, is_due). A replay or a restart finds the existing row, so the deadline never moves."""
        await self._session.execute(
            pg_insert(WorkflowTimer)
            .values(
                run_id=run_id,
                step_name=step_name,
                kind=kind,
                fire_at=func.now() + _seconds(seconds),
                status=TimerStatus.PENDING.value,
            )
            .on_conflict_do_nothing(constraint="uq_workflow_timers_run_step")
        )
        row = (
            await self._session.execute(
                select(WorkflowTimer, WorkflowTimer.fire_at <= func.now())
                .where(WorkflowTimer.run_id == run_id, WorkflowTimer.step_name == step_name)
                .execution_options(populate_existing=True)
            )
        ).one()
        timer, due = row[0], bool(row[1])
        return timer, due or timer.status == TimerStatus.FIRED.value

    async def set_timer_status(self, timer: WorkflowTimer, status: TimerStatus) -> None:
        """PENDING -> FIRED/CANCELLED. A timer that already left PENDING is left alone."""
        values: dict[str, Any] = {"status": status.value}
        if status == TimerStatus.FIRED:
            values["fired_at"] = func.now()
        await self._session.execute(
            update(WorkflowTimer)
            .where(
                WorkflowTimer.timer_id == timer.timer_id,
                WorkflowTimer.status == TimerStatus.PENDING.value,
            )
            .values(**values)
        )

    async def oldest_unconsumed(self, run_id: uuid.UUID, name: str) -> WorkflowSignal | None:
        return (
            await self._session.execute(
                select(WorkflowSignal)
                .where(
                    WorkflowSignal.run_id == run_id,
                    WorkflowSignal.name == name,
                    WorkflowSignal.consumed_at.is_(None),
                )
                .order_by(WorkflowSignal.signal_id)
                .limit(1)
            )
        ).scalar_one_or_none()

    async def consume(self, signal_id: int, step_name: str) -> None:
        await self._session.execute(
            update(WorkflowSignal)
            .where(WorkflowSignal.signal_id == signal_id, WorkflowSignal.consumed_at.is_(None))
            .values(consumed_by_step=step_name, consumed_at=func.now())
        )

    async def add_signal(
        self, run_id: uuid.UUID, name: str, payload: Any, idempotency_key: str | None
    ) -> tuple[WorkflowSignal, bool]:
        """Store a signal. Returns (signal, is_new). A repeated idempotency_key returns the original."""
        stmt = pg_insert(WorkflowSignal).values(
            run_id=run_id, name=name, payload=payload, idempotency_key=idempotency_key
        )
        if idempotency_key is not None:
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["run_id", "idempotency_key"],
                index_where=text("idempotency_key IS NOT NULL"),
            )
        signal_id = (
            await self._session.execute(stmt.returning(WorkflowSignal.signal_id))
        ).scalar_one_or_none()
        if signal_id is None:
            existing = (
                await self._session.execute(
                    select(WorkflowSignal).where(
                        WorkflowSignal.run_id == run_id,
                        WorkflowSignal.idempotency_key == idempotency_key,
                    )
                )
            ).scalar_one()
            return existing, False
        created = (
            await self._session.execute(
                select(WorkflowSignal)
                .where(WorkflowSignal.signal_id == signal_id)
                .execution_options(populate_existing=True)
            )
        ).scalar_one()
        return created, True

    async def due_timers(self, limit: int) -> list[tuple[int, uuid.UUID]]:
        rows = await self._session.execute(
            select(WorkflowTimer.timer_id, WorkflowTimer.run_id)
            .where(
                WorkflowTimer.status == TimerStatus.PENDING.value,
                WorkflowTimer.fire_at <= func.now(),
            )
            .order_by(WorkflowTimer.fire_at, WorkflowTimer.timer_id)
            .limit(limit)
        )
        return [(r[0], r[1]) for r in rows]

    async def lock_due_timer(self, timer_id: int) -> WorkflowTimer | None:
        """Lock one timer if it is still PENDING and due. SKIP LOCKED: another firer has it."""
        return (
            await self._session.execute(
                select(WorkflowTimer)
                .where(
                    WorkflowTimer.timer_id == timer_id,
                    WorkflowTimer.status == TimerStatus.PENDING.value,
                    WorkflowTimer.fire_at <= func.now(),
                )
                .with_for_update(skip_locked=True)
            )
        ).scalar_one_or_none()

    async def list_timers(self, run_id: uuid.UUID) -> list[WorkflowTimer]:
        return list(
            (
                await self._session.execute(
                    select(WorkflowTimer)
                    .where(WorkflowTimer.run_id == run_id)
                    .order_by(WorkflowTimer.timer_id)
                )
            ).scalars()
        )

    async def list_signals(self, run_id: uuid.UUID) -> list[WorkflowSignal]:
        return list(
            (
                await self._session.execute(
                    select(WorkflowSignal)
                    .where(WorkflowSignal.run_id == run_id)
                    .order_by(WorkflowSignal.signal_id)
                )
            ).scalars()
        )


class IdempotencyRepository:
    """Status of external operations, one row per idempotency key."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def mark_in_flight(self, key: str, run_id: uuid.UUID, step_name: str) -> None:
        """Insert the row, or count another attempt if the key is already known."""
        stmt = pg_insert(IdempotencyRecord).values(
            idempotency_key=key,
            run_id=run_id,
            step_name=step_name,
            status=IdempotencyStatus.IN_FLIGHT.value,
            attempts=1,
        )
        await self._session.execute(
            stmt.on_conflict_do_update(
                index_elements=["idempotency_key"],
                set_={
                    "status": IdempotencyStatus.IN_FLIGHT.value,
                    "attempts": IdempotencyRecord.attempts + 1,
                    "updated_at": func.now(),
                },
            )
        )

    async def add_attempt(self, key: str) -> None:
        await self._session.execute(
            update(IdempotencyRecord)
            .where(IdempotencyRecord.idempotency_key == key)
            .values(attempts=IdempotencyRecord.attempts + 1, updated_at=func.now())
        )

    async def set_status(self, key: str, status: IdempotencyStatus, result: Any = None) -> None:
        values: dict[str, Any] = {"status": status.value, "updated_at": func.now()}
        if status in (IdempotencyStatus.SUCCEEDED, IdempotencyStatus.RECONCILED):
            values["result_data"] = result
        await self._session.execute(
            update(IdempotencyRecord).where(IdempotencyRecord.idempotency_key == key).values(**values)
        )

    async def get(self, key: str) -> IdempotencyRecord | None:
        return await self._session.get(IdempotencyRecord, key)

    async def list_for_run(self, run_id: uuid.UUID) -> list[IdempotencyRecord]:
        result = await self._session.execute(
            select(IdempotencyRecord)
            .where(IdempotencyRecord.run_id == run_id)
            .order_by(IdempotencyRecord.created_at, IdempotencyRecord.idempotency_key)
        )
        return list(result.scalars())


class WorkerRepository:
    """Worker registry. Informational: nothing here decides who may execute a run."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def register(self, worker_id: str, hostname: str, pid: int) -> None:
        stmt = pg_insert(Worker).values(worker_id=worker_id, hostname=hostname, pid=pid, status="ACTIVE")
        await self._session.execute(
            stmt.on_conflict_do_update(
                index_elements=["worker_id"],
                set_={
                    "hostname": hostname,
                    "pid": pid,
                    "status": "ACTIVE",
                    "started_at": func.now(),
                    "last_heartbeat_at": func.now(),
                },
            )
        )

    async def touch(self, worker_id: str) -> None:
        await self._session.execute(
            update(Worker).where(Worker.worker_id == worker_id).values(last_heartbeat_at=func.now())
        )

    async def mark_stopped(self, worker_id: str) -> None:
        await self._session.execute(
            update(Worker).where(Worker.worker_id == worker_id).values(status="STOPPED")
        )

    async def list_workers(self) -> list[Worker]:
        result = await self._session.execute(select(Worker).order_by(Worker.started_at, Worker.worker_id))
        return list(result.scalars())

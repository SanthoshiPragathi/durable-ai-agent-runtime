"""Worker processes: claim runs with a lease, execute them, hand them over safely when they fail.

    python -m app.engine.worker --id worker-1

How a run changes hands
-----------------------
1. A worker claims a claimable run in one atomic UPDATE (see RunRepository.claim_next): a PENDING run,
   or a RUNNING run whose lease has expired. The claim sets lease_owner, pushes lease_expires_at into
   the future and increments fencing_token. FOR UPDATE SKIP LOCKED makes concurrent workers pass over
   each other's rows, so two workers never receive the same run.
2. While the workflow runs, a heartbeat task renews the lease every `heartbeat_seconds`. If a renewal
   is refused (someone else holds the run), or the lease could not be renewed before it would have
   expired, the worker cancels its own execution.
3. If a worker dies or freezes, its lease runs out and another worker claims the run. The new claim
   has a higher fencing token and replays the recorded history, exactly like resume_run().
4. Every write to the run's history carries the worker's token (see DbStepRecorder). A worker that
   comes back from a freeze and tries to write with its old token is rejected by the database and
   stands down. This is what makes a "zombie" worker harmless to the event log.

What fencing does NOT protect: calls to the outside world. A zombie that already started a payment
call cannot be recalled. That is why effects carry idempotency keys (Phase 3); fencing and
idempotency together cover the two halves of the problem.
"""

import argparse
import asyncio
import logging
import os
import signal
import socket
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any

from app.config import get_settings
from app.database.models import EventType, RunStatus, WorkflowRun
from app.database.repositories import (
    EventRepository,
    RunRepository,
    WorkerRepository,
)
from app.database.session import SessionFactory, get_default_session_factory
from app.engine.errors import StaleLeaseError, is_infrastructure_error
from app.engine.failpoints import failpoint
from app.engine.lease import Fence
from app.engine.replay import ReplayHistory, StepStatus
from app.engine.runtime import _drive
from app.engine.timers import fire_due_timers
from app.integrations.llm import LLMProvider
from app.sdk.workflow import registry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Claim:
    run_id: uuid.UUID
    fence: Fence
    resumed: bool  # True: a previous worker had started this run; replay its history
    workflow_name: str
    workflow_version: int
    input_data: dict[str, Any]


def _heartbeat_gave_up(heartbeat: asyncio.Task) -> bool:
    """True if the heartbeat task ended by reporting that the lease is lost."""
    if not heartbeat.done() or heartbeat.cancelled() or heartbeat.exception() is not None:
        return False
    return heartbeat.result() is False


class Worker:
    def __init__(
        self,
        session_factory: SessionFactory,
        worker_id: str | None = None,
        *,
        lease_seconds: float | None = None,
        heartbeat_seconds: float | None = None,
        poll_seconds: float | None = None,
        llm_provider: LLMProvider | None = None,
    ) -> None:
        settings = get_settings()
        self.session_factory = session_factory
        self.worker_id = worker_id or f"worker-{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.lease_seconds = settings.lease_duration_seconds if lease_seconds is None else lease_seconds
        self.heartbeat_seconds = (
            settings.heartbeat_interval_seconds if heartbeat_seconds is None else heartbeat_seconds
        )
        self.poll_seconds = settings.worker_poll_interval_seconds if poll_seconds is None else poll_seconds
        if self.heartbeat_seconds >= self.lease_seconds:
            raise ValueError("heartbeat_seconds must be shorter than lease_seconds")
        self._llm = llm_provider
        self.stopping = asyncio.Event()
        # Counters, handy in tests and logs.
        self.completed = 0
        self.lost_leases = 0
        self.infra_errors = 0

    # ---- registry -------------------------------------------------------------------------

    async def register(self) -> None:
        async with self.session_factory() as session, session.begin():
            await WorkerRepository(session).register(self.worker_id, socket.gethostname(), os.getpid())

    async def deregister(self) -> None:
        try:
            async with self.session_factory() as session, session.begin():
                await WorkerRepository(session).mark_stopped(self.worker_id)
        except Exception as exc:
            if not is_infrastructure_error(exc):
                raise
            logger.warning("could not mark the worker stopped (database unavailable)")

    # ---- claiming -------------------------------------------------------------------------

    async def claim_one(self) -> Claim | None:
        """Claim one run this worker's code can execute, or None if there is nothing to do."""
        known = [(d.name, d.version) for d in registry.list()]
        async with self.session_factory() as session, session.begin():
            row = await RunRepository(session).claim_next(
                self.worker_id, lease_seconds=self.lease_seconds, workflows=known
            )
        if row is None:
            return None
        return Claim(
            run_id=row["run_id"],
            fence=Fence(self.worker_id, row["fencing_token"]),
            resumed=row["resumed"],
            workflow_name=row["workflow_name"],
            workflow_version=row["workflow_version"],
            input_data=dict(row["input_data"]),
        )

    # ---- executing ------------------------------------------------------------------------

    async def process(self, claim: Claim) -> WorkflowRun | None:
        """Execute a claimed run. Returns the final run row, or None if the lease was lost."""
        run_id, fence = claim.run_id, claim.fence
        definition = registry.get(claim.workflow_name)
        try:
            history = await self._begin(claim)
        except StaleLeaseError:
            return self._lost(run_id, "before the workflow started")
        except Exception as exc:
            if is_infrastructure_error(exc):
                return self._infra(run_id, exc)
            raise
        failpoint("after_workflow_started")

        drive = asyncio.create_task(
            _drive(
                self.session_factory, run_id, definition, claim.input_data, history, self._llm, fence
            ),
            name=f"run-{run_id}",
        )
        heartbeat = asyncio.create_task(self._keep_lease(run_id, fence, drive), name=f"hb-{run_id}")
        try:
            run = await drive
        except StaleLeaseError:
            return self._lost(run_id, "a write was rejected")
        except Exception as exc:
            if is_infrastructure_error(exc):
                return self._infra(run_id, exc)
            raise
        except asyncio.CancelledError:
            if _heartbeat_gave_up(heartbeat):
                return self._lost(run_id, "the heartbeat could not keep the lease")
            raise  # the worker itself is being shut down
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
        if run.status == RunStatus.COMPLETED.value:
            self.completed += 1
        return run

    async def _begin(self, claim: Claim) -> ReplayHistory:
        """Record that this worker started (or took over) the run, and load the history to replay.

        STARTED versus RESUMED is decided from the history itself, not from the run's status at claim
        time: a worker can be taken over right after claiming, before it wrote anything, and then the
        next worker is the one that really starts the run.
        """
        async with self.session_factory() as session, session.begin():
            runs, events = RunRepository(session), EventRepository(session)
            await runs.check_fence(claim.run_id, claim.fence)
            recorded = await events.list_events(claim.run_id)
            info = {"worker_id": claim.fence.worker_id, "fencing_token": claim.fence.token}
            if not any(e.event_type == EventType.WORKFLOW_STARTED.value for e in recorded):
                await events.append(claim.run_id, EventType.WORKFLOW_STARTED, result_data=info)
                return ReplayHistory()
            history = ReplayHistory.from_events(recorded)
            await events.append(
                claim.run_id,
                EventType.WORKFLOW_RESUMED,
                result_data={
                    **info,
                    "recorded_steps": len(history),
                    "completed": history.count(StepStatus.COMPLETED),
                    "failed": history.count(StepStatus.FAILED),
                    "in_doubt": history.count(StepStatus.IN_DOUBT),
                },
            )
            return history

    async def _keep_lease(self, run_id: uuid.UUID, fence: Fence, drive: asyncio.Task) -> bool:
        """Renew the lease until the run ends. Cancels `drive` and returns False if the lease is lost;
        returns True if the run ended by itself (it finished or was parked, releasing its own lease).

        Two ways to lose it: the database says someone else holds the run, or we could not reach the
        database for a whole lease period (then another worker may already have the run, so we stop
        acting on it rather than guess).

        A refused renewal is NOT automatically a loss: when the run completes, the completion
        transaction clears the lease, and a heartbeat that fires just after would be refused. That is
        checked before cancelling, otherwise a finished run would be reported as a lost lease.
        """
        last_confirmed = time.monotonic()
        while True:
            await asyncio.sleep(self.heartbeat_seconds)
            try:
                async with self.session_factory() as session, session.begin():
                    runs = RunRepository(session)
                    held = await runs.renew_lease(run_id, fence, self.lease_seconds)
                    await WorkerRepository(session).touch(self.worker_id)
                    released = not held and await runs.released_by(run_id, fence)
            except Exception:
                logger.warning("lease renewal failed", extra={"run_id": str(run_id)}, exc_info=True)
                if time.monotonic() - last_confirmed >= self.lease_seconds:
                    drive.cancel()
                    return False
                continue
            if released:
                return True  # our own completion released the lease; nothing was lost
            if not held:
                drive.cancel()
                return False
            last_confirmed = time.monotonic()

    def _infra(self, run_id: uuid.UUID, exc: BaseException) -> None:
        """The database failed under this worker. Abandon the run without recording anything: its
        lease lapses and a worker replays it from the history once the database is back."""
        self.infra_errors += 1
        logger.warning(
            "infrastructure error, abandoning the run (its lease will lapse)",
            extra={"run_id": str(run_id), "worker_id": self.worker_id, "error": repr(exc)},
        )
        return None

    def _lost(self, run_id: uuid.UUID, why: str) -> None:
        self.lost_leases += 1
        logger.warning(
            "lease lost, standing down",
            extra={"run_id": str(run_id), "worker_id": self.worker_id, "why": why},
        )
        return None

    # ---- loops ----------------------------------------------------------------------------

    async def run_once(self) -> bool:
        """Fire due timers, then claim and execute at most one run. True if a run was claimed."""
        await fire_due_timers(self.session_factory)
        claim = await self.claim_one()
        if claim is None:
            return False
        await self.process(claim)
        return True

    async def run_until_idle(self, max_runs: int | None = None) -> int:
        """Process runs until none are claimable. Returns how many were claimed."""
        count = 0
        while (max_runs is None or count < max_runs) and await self.run_once():
            count += 1
        return count

    async def _drained(self) -> bool:
        async with self.session_factory() as session:
            return await RunRepository(session).count_unfinished() == 0

    async def _touch(self) -> None:
        async with self.session_factory() as session, session.begin():
            await WorkerRepository(session).touch(self.worker_id)

    async def _survive(self, fn: Any, default: Any = False) -> Any:
        """Call fn(); if the database is unreachable, note it and return `default` so the loop
        simply tries again after the poll interval. Anything that is not an infrastructure error
        is a bug and propagates."""
        try:
            return await fn()
        except Exception as exc:
            if not is_infrastructure_error(exc):
                raise
            self.infra_errors += 1
            logger.warning("database unavailable, will retry", extra={"worker_id": self.worker_id})
            return default

    async def run_forever(self, idle_exit_seconds: float | None = None, drain: bool = False) -> None:
        """Poll for work until stopped.

        idle_exit_seconds: also stop after this long without claiming anything.
        drain: also stop once no run is PENDING or RUNNING anywhere (so a worker waiting for another
               worker's lease to expire keeps waiting, then exits after the work is done).
        """
        await self._survive(self.register)
        idle_since = time.monotonic()
        try:
            while not self.stopping.is_set():
                if await self._survive(self.run_once):
                    idle_since = time.monotonic()
                    continue
                if idle_exit_seconds is not None and time.monotonic() - idle_since >= idle_exit_seconds:
                    break
                if drain and await self._survive(self._drained, default=False):
                    break
                await self._survive(self._touch)
                try:
                    await asyncio.wait_for(self.stopping.wait(), timeout=self.poll_seconds)
                except TimeoutError:
                    pass
        finally:
            await self.deregister()


def main(argv: list[str] | None = None) -> int:
    from app.engine.runner import load_workflow_modules

    parser = argparse.ArgumentParser(description="Run a durable-workflow worker.")
    parser.add_argument("--id", dest="worker_id", help="worker name (default: generated)")
    parser.add_argument("--lease", type=float, help="lease length in seconds")
    parser.add_argument("--heartbeat", type=float, help="lease renewal interval in seconds")
    parser.add_argument("--poll", type=float, help="seconds between polls when idle")
    parser.add_argument("--idle-exit", type=float, help="exit after this many idle seconds")
    parser.add_argument(
        "--drain", action="store_true", help="exit once no run is PENDING or RUNNING (for tests and demos)"
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=get_settings().log_level, format="%(asctime)s %(levelname)s %(message)s")
    load_workflow_modules()

    async def _main() -> int:
        session_factory = get_default_session_factory()
        worker = Worker(
            session_factory,
            args.worker_id,
            lease_seconds=args.lease,
            heartbeat_seconds=args.heartbeat,
            poll_seconds=args.poll,
        )
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, worker.stopping.set)
            except NotImplementedError:  # Windows: Ctrl+C still raises KeyboardInterrupt
                pass
        try:
            await worker.run_forever(idle_exit_seconds=args.idle_exit, drain=args.drain)
        finally:
            await session_factory.kw["bind"].dispose()
        print(
            f"worker {worker.worker_id}: completed={worker.completed} lost_leases={worker.lost_leases}",
            flush=True,
        )
        return 0

    try:
        return asyncio.run(_main())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())

"""Firing due timers.

A ctx.sleep or a wait_for_signal timeout is a row in workflow_timers with a deadline. Nothing is
"sleeping" in a process. Workers call fire_due_timers() on every poll; for each timer that is due it
marks it FIRED and, in the SAME transaction, makes the waiting run claimable again.

Many workers may call this at once: each timer is locked with FOR UPDATE SKIP LOCKED and re-checked
under the lock, so it fires exactly once. A timer that fires for a run that is not waiting (it was
already woken by a signal) is just marked FIRED.

Lock order is always run row first, then timer row, the same order the wait recorder uses, so the
two cannot deadlock.
"""

import uuid

from app.database.models import TimerStatus
from app.database.repositories import RunRepository, WaitRepository
from app.database.session import SessionFactory


async def fire_due_timers(session_factory: SessionFactory, limit: int = 50) -> int:
    """Fire up to `limit` due timers. Returns how many this call fired."""
    async with session_factory() as session:
        due = await WaitRepository(session).due_timers(limit)
    fired = 0
    for timer_id, run_id in due:
        if await _fire_one(session_factory, timer_id, run_id):
            fired += 1
    return fired


async def _fire_one(session_factory: SessionFactory, timer_id: int, run_id: uuid.UUID) -> bool:
    async with session_factory() as session, session.begin():
        runs, waits = RunRepository(session), WaitRepository(session)
        await runs.lock_run(run_id)
        timer = await waits.lock_due_timer(timer_id)
        if timer is None:
            return False  # fired or cancelled meanwhile, or another firer holds it
        await waits.set_timer_status(timer, TimerStatus.FIRED)
        await runs.wake(run_id)
        return True

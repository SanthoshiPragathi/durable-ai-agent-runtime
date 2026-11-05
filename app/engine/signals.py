"""Sending signals to runs.

A signal is a named message (with an optional JSON payload) for a run: a manager's approval, a
webhook. It is stored the moment it arrives, so a signal that comes before the run reaches its wait
is not lost; the wait consumes it later. A wait consumes the oldest unconsumed signal with its name,
once.

Delivery is at-least-once safe: pass an idempotency_key and a redelivered request is a no-op that
returns the original signal.
"""

import uuid
from dataclasses import dataclass
from typing import Any

from app.database.models import RunStatus
from app.database.repositories import RunRepository, WaitRepository
from app.database.session import SessionFactory
from app.engine.errors import InvalidRunStateError
from app.engine.hashing import normalize_json


@dataclass(frozen=True)
class SignalReceipt:
    signal_id: int
    duplicate: bool  # True: this idempotency_key was already delivered; nothing was stored
    woke_run: bool  # True: the run was waiting for this signal and is now claimable


async def send_signal(
    session_factory: SessionFactory,
    run_id: uuid.UUID,
    name: str,
    payload: Any = None,
    *,
    idempotency_key: str | None = None,
) -> SignalReceipt:
    """Store a signal for a run and wake it if it is waiting for it.

    Raises RunNotFoundError, or InvalidRunStateError for a COMPLETED or FAILED run (nobody can ever
    consume the signal, and silently dropping it would hide a bug in the sender).
    """
    data = normalize_json(payload)
    async with session_factory() as session, session.begin():
        runs, waits = RunRepository(session), WaitRepository(session)
        status, _ = await runs.lock_run(run_id)  # serialises against the run suspending
        if status in (RunStatus.COMPLETED.value, RunStatus.FAILED.value):
            raise InvalidRunStateError(f"run {run_id} is {status}; it can no longer receive signals")
        signal, is_new = await waits.add_signal(run_id, name, data, idempotency_key)
        woke = False
        if is_new:
            woke = await runs.wake(run_id, signal_name=name)
        return SignalReceipt(signal.signal_id, not is_new, woke)

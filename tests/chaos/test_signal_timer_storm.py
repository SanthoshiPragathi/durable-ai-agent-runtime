"""Chaos test: duplicate signals, racing timeouts and many firers at once.

Each run waits for three signals in a row, each wait with a short timeout. Senders deliver every
signal several times concurrently under the same idempotency key, at random moments, some after the
timeout may already have fired. Workers and extra timer firers run throughout.

Invariants, whatever the schedule:
  * every run completes, and each wait step completes exactly once;
  * a signal is consumed at most once, and each (run, idempotency key) is stored once;
  * a wait that reports received=True carries the payload of a signal that was consumed by that step;
    a wait that reports timed_out consumed nothing.
"""

import asyncio
import os
import random

import pytest

from app.database.repositories import EventRepository, RunRepository, WaitRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.errors import InvalidRunStateError
from app.engine.signals import send_signal
from app.engine.timers import fire_due_timers
from app.engine.worker import Worker
from app.sdk import workflow

pytestmark = [pytest.mark.chaos, pytest.mark.integration, pytest.mark.usefixtures("isolated_registry")]

SEEDS = int(os.environ.get("CHAOS_SEEDS", "4"))
RUNS = 6
WAITS = 3


def register() -> None:
    @workflow
    async def three_waits(ctx):
        out = []
        for i in range(WAITS):
            got = await ctx.wait_for_signal(f"w{i}", signal=f"s{i}", timeout=0.4)
            out.append(got)
        return out


async def sender(sf: SessionFactory, run_id, rng: random.Random, i: int, stats: dict) -> None:
    if rng.random() < 0.35:
        return  # this signal is never sent: the wait must time out
    await asyncio.sleep(rng.uniform(0, 1.2))

    async def deliver() -> None:
        await asyncio.sleep(rng.uniform(0, 0.05))
        try:
            await send_signal(sf, run_id, f"s{i}", {"i": i}, idempotency_key=f"k{i}")
            stats["sent"] += 1
        except InvalidRunStateError:
            stats["too_late"] += 1  # the run finished (all waits timed out): correctly rejected

    await asyncio.gather(*(deliver() for _ in range(rng.randint(1, 4))))


async def firer(sf: SessionFactory, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await fire_due_timers(sf)
        await asyncio.sleep(0.01)


async def test_duplicate_signals_and_racing_timers_never_double_consume(
    session_factory: SessionFactory,
) -> None:
    register()
    received_total = timed_out_total = 0
    stats = {"sent": 0, "too_late": 0}

    for seed in range(SEEDS):
        rng = random.Random(seed)
        run_ids = [(await runtime.create_run(session_factory, "three_waits", {})).run_id for _ in range(RUNS)]
        workers = [
            Worker(session_factory, f"w{i}", lease_seconds=5, heartbeat_seconds=1, poll_seconds=0.02)
            for i in range(3)
        ]
        stop = asyncio.Event()

        async def run_workers(
            pool: list[Worker] = workers, done: asyncio.Event = stop, ids: list = run_ids
        ) -> None:
            # Not --drain: waits with timeouts are counted by it, signals-only waits are not, so poll
            # until every run is finished instead.
            async def until_finished() -> None:
                while True:
                    async with session_factory() as session:
                        runs = [await RunRepository(session).get_run(r) for r in ids]
                    if all(r.status in ("COMPLETED", "FAILED") for r in runs):
                        done.set()
                        for w in pool:
                            w.stopping.set()
                        return
                    await asyncio.sleep(0.05)

            await asyncio.gather(until_finished(), *(w.run_forever() for w in pool))

        senders = [sender(session_factory, rid, rng, i, stats) for rid in run_ids for i in range(WAITS)]
        await asyncio.wait_for(
            asyncio.gather(
                run_workers(), firer(session_factory, stop), firer(session_factory, stop), *senders
            ),
            timeout=120,
        )

        for run_id in run_ids:
            async with session_factory() as session:
                run = await RunRepository(session).get_run(run_id)
                events = await EventRepository(session).list_events(run_id)
                signals = await WaitRepository(session).list_signals(run_id)
                timers = await WaitRepository(session).list_timers(run_id)
            where = f"seed {seed} run {run_id}"
            assert run is not None and run.status == "COMPLETED", where
            done_steps = [e.step_name for e in events if e.event_type == "STEP_COMPLETED"]
            assert sorted(done_steps) == ["w0", "w1", "w2"], where
            keys = [s.idempotency_key for s in signals]
            assert len(keys) == len(set(keys)), f"{where}: a duplicate signal was stored"
            consumed = {s.consumed_by_step: s for s in signals if s.consumed_at is not None}
            assert len([s for s in signals if s.consumed_at is not None]) == len(consumed), where
            for i, answer in enumerate(run.result_data):
                if answer["received"]:
                    received_total += 1
                    assert consumed[f"w{i}"].signal_id == answer["signal_id"], where
                    assert answer["payload"] == {"i": i}, where
                else:
                    timed_out_total += 1
                    assert f"w{i}" not in consumed, f"{where}: a timed-out wait consumed a signal"
            assert all(t.status in ("FIRED", "CANCELLED") for t in timers), where

    # The test only means something if both outcomes happened.
    assert received_total > 0 and timed_out_total > 0, (received_total, timed_out_total)
    print(f"\nstorm: {SEEDS} seeds, {received_total} waits got a signal, {timed_out_total} timed out, "
          f"{stats['sent']} signal deliveries accepted, {stats['too_late']} rejected as too late")

"""Benchmarks. Prints a table you can paste into docs/benchmarks.md.

    python scripts/benchmark.py            # about 1-2 minutes

Measures, on whatever machine and database you run it on (so numbers are only comparable between runs
on the same setup):
  1. Throughput: runs/second and events/second with 1, 2 and 4 worker processes.
  2. Cost per step of a live run, and the cost of replaying a history of 10 / 100 / 500 steps.
  3. Claim latency (p50 / p99) with 1000 pending runs.
  4. Recovery time after a worker dies mid-run (lease 1.5 s).

Needs PostgreSQL running and migrated. It truncates the runtime tables first (use a dev database).
"""

import asyncio
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from app.database.session import get_default_session_factory  # noqa: E402
from app.engine import runtime  # noqa: E402
from app.engine.forks import fork_run  # noqa: E402
from app.engine.runner import load_workflow_modules  # noqa: E402
from app.engine.worker import Worker  # noqa: E402
from app.sdk.workflow import registry  # noqa: E402

RUNS = 200
STEPS = 5


async def reset(sf) -> None:
    async with sf() as session, session.begin():
        await session.execute(
            text("TRUNCATE workflow_events, workflow_runs, workflows, workers, idempotency_records, "
                 "workflow_timers, workflow_signals RESTART IDENTITY CASCADE")
        )


async def count(sf, sql: str) -> int:
    async with sf() as session:
        return (await session.execute(text(sql))).scalar_one()


def spawn(name: str, extra: list[str] | None = None, env: dict | None = None) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "app.engine.worker", "--id", name, "--poll", "0.05", "--idle-exit", "3",
         *(extra or [])],
        cwd=ROOT,
        env={**os.environ, **(env or {})},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


async def throughput(sf, workers: int) -> tuple[float, float]:
    await reset(sf)
    procs = [spawn(f"bench-{i}") for i in range(workers)]
    while await count(sf, "SELECT count(*) FROM workers") < workers:
        await asyncio.sleep(0.05)
    started = time.monotonic()
    for _ in range(RUNS):
        await runtime.create_run(sf, "bench_flow", {"steps": STEPS})
    while await count(sf, "SELECT count(*) FROM workflow_runs WHERE status <> 'COMPLETED'") > 0:
        await asyncio.sleep(0.05)
    elapsed = time.monotonic() - started
    events = await count(sf, "SELECT count(*) FROM workflow_events")
    for p in procs:
        p.terminate()
        p.communicate()
    return RUNS / elapsed, events / elapsed


async def replay_cost(sf, steps: int) -> tuple[float, float]:
    """(ms per step when run live, ms to replay `steps` recorded steps)."""
    await reset(sf)
    worker = Worker(sf, "bench", lease_seconds=60, heartbeat_seconds=20, poll_seconds=0.01)
    run = await runtime.create_run(sf, "bench_flow", {"steps": steps})
    t = time.monotonic()
    await worker.run_until_idle()
    live = (time.monotonic() - t) / steps * 1000
    await fork_run(sf, run.run_id, f"s{steps - 1}")  # copies s0..s{n-2}; one step runs live
    t = time.monotonic()
    await worker.run_until_idle()
    replay = (time.monotonic() - t) * 1000  # replay of n-1 steps + one live step + bookkeeping
    return live, replay


async def claim_latency(sf) -> tuple[float, float]:
    await reset(sf)
    for _ in range(1000):
        await runtime.create_run(sf, "bench_flow", {"steps": 1})
    worker = Worker(sf, "bench", lease_seconds=60, heartbeat_seconds=20)
    samples = []
    for _ in range(200):
        t = time.monotonic()
        await worker.claim_one()
        samples.append((time.monotonic() - t) * 1000)
    samples.sort()
    return statistics.median(samples), samples[int(len(samples) * 0.99) - 1]


async def recovery_time(sf) -> float:
    await reset(sf)
    run = await runtime.create_run(sf, "bench_flow", {"steps": 5})
    dying = spawn("dies", ["--lease", "1.5", "--heartbeat", "0.3"],
                  env={"DURABLE_FAILPOINT": "after_step_completed:s2"})
    dying.communicate(timeout=60)
    died = time.monotonic()
    taker = spawn("takes-over", ["--lease", "1.5", "--heartbeat", "0.3"])
    while await count(sf, f"SELECT count(*) FROM workflow_runs WHERE status='COMPLETED' "
                          f"AND run_id='{run.run_id}'") == 0:
        await asyncio.sleep(0.05)
    elapsed = time.monotonic() - died
    taker.terminate()
    taker.communicate()
    return elapsed


async def main() -> None:
    load_workflow_modules()
    assert registry.get("bench_flow")
    sf = get_default_session_factory()
    try:
        print(f"## Throughput ({RUNS} runs of {STEPS} steps)\n")
        print("| workers | runs/s | events/s |\n|---|---|---|")
        for n in (1, 2, 4):
            rps, eps = await throughput(sf, n)
            print(f"| {n} | {rps:.1f} | {eps:.0f} |")
        print("\n## Replay cost\n")
        print("| history length | ms per live step | ms to resume (replay + 1 live step) |\n|---|---|---|")
        for steps in (10, 100, 500):
            live, replay = await replay_cost(sf, steps)
            print(f"| {steps} | {live:.1f} | {replay:.0f} |")
        p50, p99 = await claim_latency(sf)
        print(f"\n## Claim latency (1000 pending runs)\n\np50 {p50:.1f} ms, p99 {p99:.1f} ms")
        rec = await recovery_time(sf)
        print(f"\n## Recovery after a worker dies mid-run (lease 1.5 s)\n\n"
              f"{rec:.1f} s from the crash to the run COMPLETED by a new worker "
              f"(dominated by waiting for the lease to expire)")
    finally:
        await sf.kw["bind"].dispose()


if __name__ == "__main__":
    asyncio.run(main())

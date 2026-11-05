"""Demo 4: several worker processes share a batch of runs.

Twelve runs are queued. Three worker processes start and claim runs concurrently (each claim is one
atomic UPDATE ... FOR UPDATE SKIP LOCKED, so no run is handed out twice). The event log then shows
which worker ran each run and that every step completed exactly once.

    python scripts/demo_multi_worker.py

Needs PostgreSQL running and migrated (docker compose up -d postgres; alembic upgrade head).
"""

import asyncio
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.database.repositories import EventRepository, RunRepository  # noqa: E402
from app.database.session import get_default_session_factory  # noqa: E402
from app.engine import runtime  # noqa: E402
from app.engine.runner import load_workflow_modules  # noqa: E402

RUNS = 12
WORKERS = 3
STEP_SECONDS = 0.4


async def main() -> int:
    load_workflow_modules()
    sf = get_default_session_factory()
    try:
        print(f"1. Queue {RUNS} runs of 'slow_report' (3 steps of {STEP_SECONDS}s each)")
        run_ids = [
            (await runtime.create_run(sf, "slow_report", {"report_id": i})).run_id for i in range(RUNS)
        ]

        print(f"\n2. Start {WORKERS} worker processes (python -m app.engine.worker --drain)")
        started = time.monotonic()
        procs = [
            subprocess.Popen(
                [sys.executable, "-m", "app.engine.worker", "--id", f"worker-{i + 1}", "--drain",
                 "--poll", "0.1"],
                cwd=ROOT, env={**os.environ}, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            for i in range(WORKERS)
        ]
        for proc in procs:
            proc.communicate(timeout=120)
        elapsed = time.monotonic() - started
        serial = RUNS * 3 * STEP_SECONDS

        print(f"\n3. All workers exited after {elapsed:.1f}s")
        print(f"   (the steps alone add up to {serial:.1f}s, so one worker could not finish sooner)")
        per_worker: Counter[str] = Counter()
        problems = 0
        for run_id in run_ids:
            async with sf() as session:
                run = await RunRepository(session).get_run(run_id)
                events = await EventRepository(session).list_events(run_id)
            types = [e.event_type for e in events]
            started_by = events[0].result_data["worker_id"]
            per_worker[started_by] += 1
            if run.status != "COMPLETED" or types.count("STEP_COMPLETED") != 3 or run.fencing_token != 1:
                problems += 1
        print("   runs executed per worker:")
        for name, count in sorted(per_worker.items()):
            print(f"     {name}: {count}  {'#' * count}")
        print(f"   completed: {RUNS - problems}/{RUNS}; every run was claimed exactly once (fencing token 1)")
        if problems:
            print(f"   {problems} run(s) did not complete cleanly")
            return 1
        print("\n   Nothing was run twice, and the work was shared without any coordinator process.")
        return 0
    finally:
        await sf.kw["bind"].dispose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

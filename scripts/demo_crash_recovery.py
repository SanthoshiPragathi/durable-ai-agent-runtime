"""Demo 2: crash recovery.

Starts a workflow in a worker process, kills that process at a controlled point, shows what is
saved in PostgreSQL, then resumes the run in a NEW process and prints the final timeline.

    python scripts/demo_crash_recovery.py

Needs PostgreSQL running and migrated (docker compose up -d postgres; alembic upgrade head).
"""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.database.repositories import EventRepository, RunRepository  # noqa: E402
from app.database.session import get_default_session_factory  # noqa: E402
from app.engine import runtime  # noqa: E402
from app.engine.failpoints import CRASH_EXIT_CODE, ENV_VAR  # noqa: E402
from app.engine.runner import load_workflow_modules  # noqa: E402

CRASH_AT = "after_step_completed:calculate_total"


def run_worker(action: str, run_id: str, failpoint: str | None = None) -> subprocess.CompletedProcess:
    env = {**os.environ}
    env.pop(ENV_VAR, None)
    if failpoint:
        env[ENV_VAR] = failpoint
    return subprocess.run(
        [sys.executable, "-m", "app.engine.runner", action, run_id],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )


async def timeline(session_factory, run_id) -> None:
    async with session_factory() as session:
        run = await RunRepository(session).get_run(run_id)
        events = await EventRepository(session).list_events(run_id)
    print(f"  run status: {run.status}   current step: {run.current_step}")
    for e in events:
        detail = f" {e.step_name}" if e.step_name else ""
        print(f"   {e.sequence_number:>2}  {e.event_type}{detail}")


async def main() -> int:
    load_workflow_modules()
    session_factory = get_default_session_factory()
    try:
        run = await runtime.create_run(session_factory, "order_summary", {"order_id": "A-100"})
        run_id = str(run.run_id)
        print(f"1. Created run {run_id} (workflow: order_summary)")

        print(f"\n2. Starting worker process 1, set to be killed at '{CRASH_AT}' ...")
        first = run_worker("execute", run_id, CRASH_AT)
        if first.returncode != CRASH_EXIT_CODE:
            print(f"   unexpected result (exit {first.returncode}): {first.stdout} {first.stderr}")
            return 1
        print(f"   worker 1 was killed (exit code {first.returncode}). What survived in PostgreSQL:")
        await timeline(session_factory, run.run_id)

        print("\n3. Starting worker process 2 to resume the same run ...")
        second = run_worker("resume", run_id)
        if second.returncode != 0:
            print(f"   resume failed: {second.stdout} {second.stderr}")
            return 1
        print(f"   worker 2 finished: {json.loads(second.stdout)['status']}")

        print("\n4. Final timeline. Note fetch_order and calculate_total each STARTED only once:")
        print("   worker 2 reused their saved results and only ran format_summary.")
        await timeline(session_factory, run.run_id)
        return 0
    finally:
        await session_factory.kw["bind"].dispose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

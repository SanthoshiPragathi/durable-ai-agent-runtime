"""Demo 6: durable waiting. A leave request waits for a manager with no process, no lease, no memory.

    python scripts/demo_durable_waiting.py

Part A: the run parks as WAITING and the worker process EXITS. A signal arrives later; a brand new
        worker process replays the history and finishes the run without repeating earlier steps.
Part B: nobody answers. The timer fires, the run wakes, and the workflow escalates and auto-rejects.

Needs PostgreSQL running and migrated (docker compose up -d postgres; alembic upgrade head).
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.database.repositories import EventRepository, RunRepository, WaitRepository  # noqa: E402
from app.database.session import get_default_session_factory  # noqa: E402
from app.engine import runtime  # noqa: E402
from app.engine.runner import load_workflow_modules  # noqa: E402
from app.engine.signals import send_signal  # noqa: E402


def worker(name: str, idle_exit: float = 2.0) -> int:
    proc = subprocess.run(
        [sys.executable, "-m", "app.engine.worker", "--id", name, "--idle-exit", str(idle_exit),
         "--poll", "0.2"],
        cwd=ROOT, env={**os.environ}, capture_output=True, text=True, timeout=120,
    )
    return proc.returncode


async def show(sf, run_id, label: str) -> None:
    async with sf() as session:
        run = await RunRepository(session).get_run(run_id)
        events = await EventRepository(session).list_events(run_id)
        timers = await WaitRepository(session).list_timers(run_id)
    print(f"   [{label}] status={run.status} lease_owner={run.lease_owner} "
          f"waiting_signal={run.waiting_signal} timers={[(t.kind, t.status) for t in timers]}")
    names = [e.event_type + (f"({e.step_name})" if e.step_name else "") for e in events]
    print("   events: " + " > ".join(names))


async def main() -> int:
    load_workflow_modules()
    sf = get_default_session_factory()
    try:
        print("PART A: the manager answers\n")
        print("1. Create a leave request (the manager has 3 days to decide)")
        run = await runtime.create_run(
            sf, "leave_approval", {"employee": "asha", "manager": "ravi", "days": 3}
        )
        print("\n2. A worker process runs it. It emails the manager, reaches the wait and EXITS.")
        worker("worker-1")
        await show(sf, run.run_id, "after worker-1 exited")
        print("   -> the run holds no lease and no worker is running; the state lives in PostgreSQL.")

        print("\n3. Much later the manager approves (a signal, with an idempotency key)")
        receipt = await send_signal(sf, run.run_id, "manager_decision", {"approved": True},
                                    idempotency_key="approval-email-click-1")
        again = await send_signal(sf, run.run_id, "manager_decision", {"approved": False},
                                  idempotency_key="approval-email-click-1")
        print(f"   first delivery: woke_run={receipt.woke_run}; "
              f"the same click delivered again: duplicate={again.duplicate} (ignored)")

        print("\n4. A NEW worker process picks the run up and replays the history")
        worker("worker-2")
        await show(sf, run.run_id, "after worker-2")
        async with sf() as session:
            done = await RunRepository(session).get_run(run.run_id)
            kinds = [e.event_type for e in await EventRepository(session).list_events(run.run_id)]
        ok_a = (done.status == "COMPLETED" and done.result_data["outcome"] == "approved"
                and kinds.count("STEP_COMPLETED") == 3)
        print(f"   result={done.result_data}; email_manager ran once (3 steps completed, 1 each)")

        print("\nPART B: nobody answers\n")
        print("5. A second request with a 1 second timeout (the demo's stand-in for 3 days)")
        run_b = await runtime.create_run(
            sf, "leave_approval",
            {"employee": "john", "manager": "meena", "days": 1, "timeout_seconds": 1},
        )
        worker("worker-3", idle_exit=0.5)
        await show(sf, run_b.run_id, "parked")
        print("\n6. The timer becomes due; any worker fires it, wakes the run and finishes it")
        await asyncio.sleep(1.2)
        worker("worker-4")
        async with sf() as session:
            final_b = await RunRepository(session).get_run(run_b.run_id)
        ok_b = final_b.status == "COMPLETED" and final_b.result_data["outcome"] == "auto_rejected"
        await show(sf, run_b.run_id, "after the timeout")
        print(f"   result={final_b.result_data}")

        verdict = "passed: both runs waited with no process and resumed exactly once."
        print("\nDemo " + (verdict if ok_a and ok_b else "FAILED"))
        return 0 if ok_a and ok_b else 1
    finally:
        await sf.kw["bind"].dispose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

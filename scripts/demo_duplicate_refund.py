"""Demo 3: duplicate refund prevention.

A worker calls the payment service; the refund SUCCEEDS; the worker is killed before the runtime
records it. A new worker resumes the run and retries with the same idempotency key. The payment
ledger, which is the payment service's own database, still shows exactly one refund.

    python scripts/demo_duplicate_refund.py

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

from app.database.repositories import EventRepository, IdempotencyRepository  # noqa: E402
from app.database.session import get_default_session_factory  # noqa: E402
from app.engine import runtime  # noqa: E402
from app.engine.failpoints import CRASH_EXIT_CODE, ENV_VAR  # noqa: E402
from app.engine.runner import load_workflow_modules  # noqa: E402
from app.integrations.tools import emails, payments  # noqa: E402

KILL_AT = "before_step_completed:process_refund"  # after the refund succeeded, before it is recorded


def worker(action: str, run_id: str, failpoint: str | None = None) -> subprocess.CompletedProcess:
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


async def show_ledger(run_id: str, title: str) -> None:
    refund = await payments().find_by_idempotency_key(f"{run_id}:process_refund")
    email_key = f"{run_id}:send_confirmation"
    sent = [e for e in await emails().list_emails() if e["idempotency_key"] == email_key]
    detail = f"  -> {refund['refund_id']}, {refund['amount'] / 100:.2f}" if refund else ""
    print(f"   {title}")
    print(f"   payment ledger: {1 if refund else 0} refund(s){detail}")
    print(f"   email ledger:   {len(sent)} email(s)")


async def main() -> int:
    load_workflow_modules()
    sf = get_default_session_factory()
    try:
        run = await runtime.create_run(sf, "refund_workflow", {"order_id": "ORD-1001"})
        run_id = str(run.run_id)
        print(f"1. Created refund run {run_id} for order ORD-1001")

        print(f"\n2. Worker 1 starts and is KILLED at '{KILL_AT}'")
        print("   (the payment service has already processed the refund)")
        first = worker("execute", run_id, KILL_AT)
        if first.returncode != CRASH_EXIT_CODE:
            print(f"   unexpected result (exit {first.returncode}): {first.stdout} {first.stderr}")
            return 1
        async with sf() as session:
            events = await EventRepository(session).list_events(run.run_id)
        last = f"{events[-1].event_type} {events[-1].step_name}"
        print(f"   worker 1 died (exit {first.returncode}). The runtime's last event: {last}")
        print("   ...so the runtime does NOT know the refund went through.")
        await show_ledger(run_id, "The payment service's own books at this moment:")

        print("\n3. Worker 2 resumes the run. It retries process_refund with the SAME idempotency key:")
        print(f"   key = {run_id}:process_refund")
        second = worker("resume", run_id)
        if second.returncode != 0:
            print(f"   resume failed: {second.stdout} {second.stderr}")
            return 1
        result = json.loads(second.stdout)
        print(f"   worker 2 finished: {result['status']}  result = {result['result']}")

        print("\n4. The payment service saw the key again and returned the ORIGINAL refund:")
        await show_ledger(run_id, "The payment service's own books now:")
        async with sf() as session:
            op = (await IdempotencyRepository(session).list_for_run(run.run_id))[0]
        print(f"   runtime's record of the operation: {op.step_name} {op.status}, attempts={op.attempts}")
        print("\n   One refund, even though the worker died after the money moved.")
        return 0
    finally:
        await sf.kw["bind"].dispose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

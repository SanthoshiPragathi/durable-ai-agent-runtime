"""Demo 5: a zombie worker is fenced off.

Worker "zombie" starts a refund run. The refund succeeds, then the whole process freezes for 8 seconds
(event loop and heartbeat included; think of a VM pause or a very long garbage collection). Its lease
runs out. Worker "healthy" takes the run over with a higher fencing token and finishes it. When the
zombie thaws and tries to record its result, the database rejects the write: its token is out of date.

    python scripts/demo_zombie_worker.py

Needs PostgreSQL running and migrated (docker compose up -d postgres; alembic upgrade head).
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.database.repositories import EventRepository, RunRepository  # noqa: E402
from app.database.session import get_default_session_factory  # noqa: E402
from app.engine import runtime  # noqa: E402
from app.engine.failpoints import ENV_VAR, STALL_ENV_VAR  # noqa: E402
from app.engine.runner import load_workflow_modules  # noqa: E402
from app.integrations.tools import emails, payments  # noqa: E402

FREEZE_AT = "before_step_completed:process_refund"  # the refund has succeeded; not yet recorded
FREEZE_SECONDS = 8
LEASE = "1.5"


def start(name: str, stall: str | None = None) -> subprocess.Popen:
    env = {**os.environ}
    env.pop(ENV_VAR, None)
    env.pop(STALL_ENV_VAR, None)
    if stall:
        env[STALL_ENV_VAR] = stall
    return subprocess.Popen(
        [sys.executable, "-m", "app.engine.worker", "--id", name, "--drain", "--lease", LEASE,
         "--heartbeat", "0.3", "--poll", "0.2"],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )


async def ledger(run_id: str, title: str) -> None:
    refund = await payments().find_by_idempotency_key(f"{run_id}:process_refund")
    sent = [e for e in await emails().list_emails() if e["idempotency_key"] == f"{run_id}:send_confirmation"]
    detail = f" -> {refund['refund_id']}" if refund else ""
    print(f"   {title} payment ledger: {1 if refund else 0} refund(s){detail}; email ledger: {len(sent)}")


async def main() -> int:
    load_workflow_modules()
    sf = get_default_session_factory()
    try:
        run = await runtime.create_run(sf, "refund_workflow", {"order_id": "ORD-1001"})
        run_id = str(run.run_id)
        print(f"1. Created refund run {run_id}")

        print(f"\n2. Worker 'zombie' claims it (fencing token 1) and freezes for {FREEZE_SECONDS}s")
        print(f"   right after the refund succeeded ({FREEZE_AT}).")
        zombie = start("zombie", f"{FREEZE_AT}/{FREEZE_SECONDS}")
        for _ in range(300):
            async with sf() as session:
                current = await RunRepository(session).get_run(run.run_id)
            if current.lease_owner == "zombie":
                break
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.7)
        await ledger(run_id, "While it is frozen:")

        print(f"\n3. Worker 'healthy' starts. After the {LEASE}s lease lapses it takes the run over.")
        healthy = start("healthy")
        healthy_out, _ = healthy.communicate(timeout=120)
        async with sf() as session:
            events = await EventRepository(session).list_events(run.run_id)
            final = await RunRepository(session).get_run(run.run_id)
        for e in events:
            if e.event_type in ("WORKFLOW_STARTED", "WORKFLOW_RESUMED"):
                d = e.result_data
                print(f"   {e.event_type:<17} by {d['worker_id']} with fencing token {d['fencing_token']}")
        print(f"   healthy finished the run: {final.status}, fencing token now {final.fencing_token}")
        print(f"   healthy says: {healthy_out.strip()}")
        await ledger(run_id, "After the takeover:")

        print("\n4. The zombie thaws and tries to record its refund with token 1...")
        zombie_out, _ = zombie.communicate(timeout=120)
        print(f"   zombie says: {zombie_out.strip()}")
        async with sf() as session:
            after = await EventRepository(session).list_events(run.run_id)
        print(f"   events in the history: {len(events)} before the zombie woke, {len(after)} after")
        await ledger(run_id, "Final:")

        ok = len(after) == len(events) and after[-1].event_type == "WORKFLOW_COMPLETED"
        print("\n   The zombie's late write was rejected: the history is exactly what 'healthy' recorded,")
        print("   and the payment service still shows one refund." if ok else "   UNEXPECTED!")
        return 0 if ok else 1
    finally:
        await sf.kw["bind"].dispose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

"""Demo 7: fork a finished run from one step with a different answer, and diff the two runs.

    python scripts/demo_forking.py

A support-triage workflow classifies a ticket and routes it. We fork the finished run at "classify"
and force a different classification, then compare the runs step by step. The fork reuses the earlier
steps from the source's history instead of executing them again. (The workflow has no real-world
effects on purpose: a fork that reaches an effect step performs it again under a new idempotency key.)

Needs PostgreSQL running and migrated (docker compose up -d postgres; alembic upgrade head).
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.database.repositories import RunRepository  # noqa: E402
from app.database.session import get_default_session_factory  # noqa: E402
from app.engine import runtime  # noqa: E402
from app.engine.forks import diff_runs, fork_run  # noqa: E402
from app.engine.runner import load_workflow_modules  # noqa: E402
from app.engine.worker import Worker  # noqa: E402
from app.sdk import workflow  # noqa: E402

EXECUTED: list[str] = []


@workflow
async def support_triage(ctx, ticket_id: str):
    ticket = await ctx.step("fetch_ticket", lambda: EXECUTED.append("fetch_ticket") or {"id": ticket_id})
    label = await ctx.step("classify", lambda: EXECUTED.append("classify") or "BILLING")
    queue = await ctx.step("route", lambda: EXECUTED.append("route") or f"{label.lower()}-queue")
    return {"ticket": ticket["id"], "label": label, "queue": queue}


async def main() -> int:
    load_workflow_modules()
    sf = get_default_session_factory()
    try:
        worker = Worker(sf, "demo-worker", lease_seconds=30, heartbeat_seconds=10, poll_seconds=0.1)
        print("1. Run the triage workflow normally")
        run = await runtime.create_run(sf, "support_triage", {"ticket_id": "T-42"})
        await worker.run_until_idle()
        async with sf() as s:
            source = await RunRepository(s).get_run(run.run_id)
        print(f"   source run {source.run_id}: {source.status}\n   result={source.result_data}")

        print("\n2. Fork it at 'classify' and force the answer to 'TECHNICAL'")
        fork = await fork_run(sf, run.run_id, "classify", override_result="TECHNICAL")
        print(f"   fork {fork.run_id} (parent {fork.parent_run_id}) is claimable, nothing executed yet")
        await worker.run_until_idle()
        async with sf() as s:
            forked = await RunRepository(s).get_run(fork.run_id)
        print(f"   fork finished: {forked.status}\n   result={forked.result_data}")

        print("\n3. Diff the two runs, step by step")
        diff = await diff_runs(sf, run.run_id, fork.run_id)
        for step in diff.steps:
            mark = "same   " if step.identical else "DIFFERS"
            print(f"   {mark} {step.step_name:<20} {step.status_a} / {step.status_b}")
        print(f"   first divergence: {diff.first_divergence}")
        print(f"   step functions executed in total: {EXECUTED}")
        print("   (fetch_ticket ran once: the fork replayed it; classify did not run in the fork)")
        ok = (diff.first_divergence == "classify" and forked.status == "COMPLETED"
              and EXECUTED == ["fetch_ticket", "classify", "route", "route"])
        verdict = "passed: the fork shares history up to the fork point and diverges after it."
        print("\nDemo " + (verdict if ok else "FAILED"))
        return 0 if ok else 1
    finally:
        await sf.kw["bind"].dispose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

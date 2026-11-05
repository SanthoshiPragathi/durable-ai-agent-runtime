"""Chaos campaign: many refund runs, several real worker processes, and a steady stream of failures.

    python scripts/chaos_campaign.py --runs 1000 --workers 4
    python scripts/chaos_campaign.py --runs 10000 --workers 6 \
        --pg-restart-cmd "docker compose restart postgres"

What it does to the system while the runs execute (all on real OS processes and a real PostgreSQL):
  * kills a random worker outright (process kill, no cleanup);
  * starts "doomed" workers that die at an exact protocol point (DURABLE_FAILPOINT: right after the
    refund succeeded but before it is recorded, right after a lease claim, during recovery, ...);
  * starts "zombie" workers that freeze past their lease at an exact point (DURABLE_STALL) and then wake up;
  * optionally restarts PostgreSQL every few seconds (--pg-restart-cmd);
  * keeps the worker pool topped up, so work is always being taken over.

Judges (none of them is the runtime's own opinion):
  * every run reached COMPLETED, none lost;
  * the payment and email providers' own ledgers hold exactly one refund and one email per run;
  * each event log is gapless, has one WORKFLOW_COMPLETED, every step completed once, and its fencing
    tokens only go up.

Needs PostgreSQL running and migrated. It TRUNCATES the runtime tables and the fake services'
ledgers first.
Works on Linux, macOS and Windows (kills use the process API, stalls use DURABLE_STALL, not SIGSTOP).
"""

import argparse
import asyncio
import os
import random
import re
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from app.database.repositories import EventRepository, RunRepository  # noqa: E402
from app.database.session import get_default_session_factory  # noqa: E402
from app.engine import runtime  # noqa: E402
from app.engine.failpoints import CRASH_EXIT_CODE  # noqa: E402
from app.engine.runner import load_workflow_modules  # noqa: E402
from app.integrations.tools import EmailService, IdempotentPaymentService  # noqa: E402
from app.integrations.tools.fake_ledger import get_services_session_factory  # noqa: E402

DOOMED_AT = [
    "after_workflow_started",
    "after_step_started:process_refund",
    "before_step_completed:process_refund",  # the refund happened, nobody wrote it down
    "after_step_completed:process_refund",
    "before_step_completed:send_confirmation",
    "before_workflow_completed",
    "after_lease_claimed",
    "during_recovery",
]
STALL_AT = [
    "before_step_completed:process_refund",
    "after_step_started:process_refund",
    "before_workflow_completed",
]
STEPS = ["fetch_order", "check_eligibility", "classify_complaint", "process_refund", "send_confirmation"]


def worker_env(extra: dict[str, str]) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in ("DURABLE_FAILPOINT", "DURABLE_STALL")}
    env["LLM_PROVIDER"] = "mock"
    return {**env, **extra}


class Pool:
    def __init__(self, args: argparse.Namespace, logdir: Path) -> None:
        self.args, self.logdir = args, logdir
        self.procs: dict[str, tuple[subprocess.Popen, str, float]] = {}  # id -> (proc, kind, started_at)
        self.counts: Counter[str] = Counter()
        self.exit_codes: Counter[str] = Counter()
        self.serial = 0

    def spawn(self, kind: str, extra_env: dict[str, str] | None = None) -> None:
        self.serial += 1
        worker_id = f"{kind}-{self.serial}"
        log = open(self.logdir / f"{worker_id}.log", "w")  # noqa: SIM115  (closed when the process is reaped)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "app.engine.worker",
                "--id",
                worker_id,
                "--lease",
                str(self.args.lease),
                "--heartbeat",
                str(self.args.heartbeat),
                "--poll",
                str(self.args.poll),
            ],
            cwd=ROOT,
            env=worker_env(extra_env or {}),
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.procs[worker_id] = (proc, kind, time.monotonic())
        self.counts[f"spawned_{kind}"] += 1

    def reap(self) -> None:
        for worker_id, (proc, kind, started) in list(self.procs.items()):
            code = proc.poll()
            if (
                code is None
                and kind == "zombie"
                and time.monotonic() - started > self.args.stall_seconds * 2 + 5
            ):
                proc.kill()  # it has woken up and been fenced out; retire it
                proc.wait()
                code = proc.returncode
            if code is not None:
                del self.procs[worker_id]
                self.exit_codes["failpoint crash" if code == CRASH_EXIT_CODE else "killed/other"] += 1

    def alive(self) -> int:
        return len(self.procs)

    def kill_random(self, rng: random.Random) -> None:
        normal = [w for w, (p, k, _) in self.procs.items() if k == "normal" and p.poll() is None]
        if len(normal) > 1:  # always leave one standing
            victim = rng.choice(normal)
            self.procs[victim][0].kill()
            self.counts["random_kills"] += 1

    def stop_all(self) -> None:
        for proc, _, _ in self.procs.values():
            proc.terminate()
        for proc, _, _ in self.procs.values():
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()


async def retry(coro_factory, tries: int = 60):
    """The supervisor's own database calls must survive the PostgreSQL restarts it causes."""
    for attempt in range(tries):
        try:
            return await coro_factory()
        except Exception:  # noqa: BLE001
            if attempt == tries - 1:
                raise
            await asyncio.sleep(0.5)


async def reset(sf, ssf) -> None:
    async def go() -> None:
        async with sf() as session, session.begin():
            await session.execute(
                text(
                    "TRUNCATE workflow_events, workflow_runs, workflows, workers, idempotency_records, "
                    "workflow_timers, workflow_signals RESTART IDENTITY CASCADE"
                )
            )
        async with ssf() as session, session.begin():
            await session.execute(text("TRUNCATE payment_refunds, emails RESTART IDENTITY CASCADE"))

    await retry(go)


async def create_runs(sf, n: int) -> None:
    sem = asyncio.Semaphore(16)

    async def one() -> None:
        async with sem:
            await runtime.create_run(sf, "refund_workflow", {"order_id": "ORD-1001"})

    await asyncio.gather(*(one() for _ in range(n)))


async def count_status(sf) -> dict[str, int]:
    async def go():
        async with sf() as session:
            rows = (
                await session.execute(text("SELECT status, count(*) FROM workflow_runs GROUP BY 1"))
            ).all()
            return {s: n for s, n in rows}

    return await retry(go)


async def verify(sf, ssf, expected: int) -> dict:
    problems: list[str] = []
    payments, mail = IdempotentPaymentService(ssf), EmailService(ssf)
    refunds = await retry(payments.count_refunds)
    emails = len(await retry(mail.list_emails))
    if refunds != expected:
        problems.append(f"refund ledger holds {refunds}, expected {expected}")
    if emails != expected:
        problems.append(f"email ledger holds {emails}, expected {expected}")
    distinct = await retry(lambda: _distinct_keys(ssf))
    if distinct != expected:
        problems.append(f"{distinct} distinct refund idempotency keys, expected {expected} (one per run)")

    takeovers = 0
    async with sf() as session:
        runs = await RunRepository(session).list_runs(limit=expected + 10)
    if len(runs) != expected:
        problems.append(f"{expected - len(runs)} runs are missing from the database")
    for run in runs:
        if run.status != "COMPLETED":
            problems.append(f"run {run.run_id} ended {run.status}")
            continue
        takeovers += run.fencing_token - 1
        async with sf() as session:
            events = await EventRepository(session).list_events(run.run_id)
        kinds = [e.event_type for e in events]
        done = [e.step_name for e in events if e.event_type == "STEP_COMPLETED"]
        tokens = [
            e.result_data["fencing_token"]
            for e in events
            if e.event_type in ("WORKFLOW_STARTED", "WORKFLOW_RESUMED")
            and e.result_data
            and "fencing_token" in e.result_data
        ]
        where = f"run {run.run_id}"
        if [e.sequence_number for e in events] != list(range(1, len(events) + 1)):
            problems.append(f"{where}: sequence numbers have gaps")
        if kinds.count("WORKFLOW_COMPLETED") != 1 or kinds[-1] != "WORKFLOW_COMPLETED":
            problems.append(f"{where}: not exactly one WORKFLOW_COMPLETED at the end")
        if done != STEPS:
            problems.append(f"{where}: steps completed {done}")
        if tokens != sorted(set(tokens)):
            problems.append(f"{where}: fencing tokens not increasing {tokens}")
    return {
        "problems": problems,
        "refunds": refunds,
        "emails": emails,
        "distinct_keys": distinct,
        "takeovers": takeovers,
        "runs_checked": len(runs),
    }


async def _distinct_keys(ssf) -> int:
    async with ssf() as session:
        return (
            await session.execute(text("SELECT count(DISTINCT idempotency_key) FROM payment_refunds"))
        ).scalar_one()


def stale_rejections(logdir: Path) -> int:
    total = 0
    for log in logdir.glob("*.log"):
        total += len(re.findall(r"lease lost, standing down", log.read_text(errors="ignore")))
    return total


async def main(args: argparse.Namespace) -> int:
    load_workflow_modules()
    sf, ssf = get_default_session_factory(), get_services_session_factory()
    rng = random.Random(args.seed)
    await reset(sf, ssf)
    started = time.monotonic()
    await create_runs(sf, args.runs)
    print(
        f"created {args.runs} runs in {time.monotonic() - started:.1f}s; starting {args.workers} workers",
        flush=True,
    )

    with tempfile.TemporaryDirectory(prefix="chaos-") as tmp:
        logdir = Path(tmp)
        pool = Pool(args, logdir)
        for _ in range(args.workers):
            pool.spawn("normal")
        next_kill = time.monotonic() + args.kill_every
        next_pg = time.monotonic() + args.pg_restart_every if args.pg_restart_cmd else float("inf")
        pg_restarts, last_report, finished = 0, 0.0, False

        try:
            while time.monotonic() - started < args.timeout:
                await asyncio.sleep(0.25)
                pool.reap()
                # keep the pool topped up; some replacements are doomed or zombie workers
                while sum(1 for _, k, _ in pool.procs.values() if k != "zombie") < args.workers:
                    roll = rng.random()
                    if roll < 0.35:
                        pool.spawn("doomed", {"DURABLE_FAILPOINT": rng.choice(DOOMED_AT)})
                    else:
                        pool.spawn("normal")
                now = time.monotonic()
                if now >= next_kill:
                    next_kill = now + rng.uniform(0.5, 1.5) * args.kill_every
                    if rng.random() < 0.6:
                        pool.kill_random(rng)
                    else:
                        where = rng.choice(STALL_AT)
                        pool.spawn("zombie", {"DURABLE_STALL": f"{where}/{args.stall_seconds}"})
                if now >= next_pg:
                    next_pg = now + rng.uniform(0.7, 1.3) * args.pg_restart_every
                    subprocess.run(args.pg_restart_cmd, shell=True, capture_output=True, timeout=120)
                    pg_restarts += 1
                if now - last_report > 15:
                    last_report = now
                    st = await count_status(sf)
                    print(
                        f"[{now - started:6.0f}s] {st}  workers={pool.alive()}  "
                        f"kills={pool.counts['random_kills']} pg_restarts={pg_restarts}",
                        flush=True,
                    )
                    finished = st.get("COMPLETED", 0) == args.runs
                if finished or (await count_status(sf)).get("COMPLETED", 0) == args.runs:
                    break
        finally:
            pool.stop_all()
        elapsed = time.monotonic() - started
        rejections = stale_rejections(logdir)

    result = await verify(sf, ssf, args.runs)
    status = await count_status(sf)
    ok = not result["problems"] and status.get("COMPLETED", 0) == args.runs
    lines = [
        f"## Chaos campaign: {args.runs} runs, {args.workers} workers, seed {args.seed}",
        "",
        "| | |",
        "|---|---|",
        f"| result | **{'PASS' if ok else 'FAIL'}** |",
        f"| runs completed / created | {status.get('COMPLETED', 0)} / {args.runs} |",
        f"| wall time | {elapsed:.0f} s ({args.runs / elapsed:.1f} runs/s) |",
        f"| worker processes started | {pool.serial} (normal {pool.counts['spawned_normal']}, doomed "
        f"{pool.counts['spawned_doomed']}, zombie {pool.counts['spawned_zombie']}) |",
        f"| random process kills | {pool.counts['random_kills']} |",
        f"| workers that died at an exact failpoint | {pool.exit_codes['failpoint crash']} |",
        f"| zombie workers frozen past their lease | {pool.counts['spawned_zombie']} |",
        f"| PostgreSQL restarts | {pg_restarts} |",
        f"| lease takeovers (sum of fencing token - 1) | {result['takeovers']} |",
        f'| stale workers fenced out ("lease lost" in logs) | {rejections} |',
        f"| refunds in the payment provider's ledger | {result['refunds']} (expected {args.runs}) |",
        f"| emails in the email provider's ledger | {result['emails']} (expected {args.runs}) |",
        f"| distinct refund idempotency keys (one per run expected) | {result['distinct_keys']} |",
        f"| event logs checked | {result['runs_checked']} |",
        f"| problems found | {len(result['problems'])} |",
    ]
    print("\n" + "\n".join(lines))
    for problem in result["problems"][:20]:
        print("PROBLEM:", problem)
    if args.report:
        Path(args.report).write_text("\n".join(lines) + "\n", encoding="utf-8")
    await sf.kw["bind"].dispose()
    await ssf.kw["bind"].dispose()
    return 0 if ok else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--runs", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--lease", type=float, default=3.0)
    parser.add_argument("--heartbeat", type=float, default=0.5)
    parser.add_argument("--poll", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--kill-every", type=float, default=4.0, help="mean seconds between random kills/zombies"
    )
    parser.add_argument(
        "--stall-seconds", type=float, default=8.0, help="how long a zombie freezes (> lease)"
    )
    parser.add_argument(
        "--pg-restart-cmd",
        default="",
        help="command that restarts PostgreSQL, e.g. 'docker compose restart postgres'",
    )
    parser.add_argument("--pg-restart-every", type=float, default=45.0)
    parser.add_argument("--timeout", type=float, default=3600.0)
    parser.add_argument("--report", default="", help="write the result table to this markdown file")
    sys.exit(asyncio.run(main(parser.parse_args())))

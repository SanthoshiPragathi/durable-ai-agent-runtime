# Demo script (about 8 minutes)

Start: `docker compose up -d postgres`, `alembic upgrade head`, run the API (`uvicorn app.api.main:app`) and open
`http://localhost:8000/dashboard`. Each demo is one command; say the sentence in bold, run it, point at the output.

| # | Command | Say | Point at |
|---|---|---|---|
| 1 | API docs at `/docs`: create a `refund_workflow` run (needs a worker: `python -m app.engine.worker`) | **A workflow is ordinary async Python; every step is recorded.** | the event timeline in the dashboard |
| 2 | `python scripts/demo_crash_recovery.py` | **I kill the worker mid-run; a new process finishes it from the log.** | completed steps are not re-executed |
| 3 | `python scripts/demo_duplicate_refund.py` | **The worker dies after the refund succeeded but before it was recorded. The refund still happens once.** | the payment ledger: one refund, same id before and after |
| 4 | `python scripts/demo_multi_worker.py` | **Three workers share twelve runs with no coordinator.** | per-worker counts, fencing token 1 everywhere |
| 5 | `python scripts/demo_zombie_worker.py` | **A frozen worker wakes up after its run was taken over; the database rejects it.** | `StaleLeaseError`, history unchanged |
| 6 | `python scripts/demo_durable_waiting.py` | **A run waits for a manager's approval with no process running, then continues.** | status WAITING, no lease; a duplicate signal ignored |
| 7 | `python scripts/demo_forking.py` | **I fork a finished run with a different answer and diff the two.** | first divergence, earlier step replayed |
| 8 | `scripts\verify.ps1` | **Every test, including a real PostgreSQL restart under load, passes.** | `docs/test-report.md`, `docs/benchmarks.md` |

Dashboard moment: open a WAITING run, type `{"approved": true}` into the signal box, press Send, and watch the
timeline grow while a worker finishes it.

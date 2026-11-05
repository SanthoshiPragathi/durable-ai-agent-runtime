# Durable AI Agent Runtime

A fault-tolerant execution engine for AI workflows: crash-safe, replayable, and forkable.

> **Status: Phase 4 (workers, leases, fencing).** Workflows run, every step is recorded in PostgreSQL, a
> crashed run resumes from its history, calls with real-world effects (payments, emails) are protected against
> duplicates where the external system allows it, and several worker processes share the work: a dead worker's
> runs are taken over automatically and a frozen "zombie" worker cannot write to a run it lost. Timers,
> signals and forking are **not implemented yet** (see the roadmap). This README is updated at every
> milestone so it never claims more than the code does.

## The problem

AI agents run long, multi-step jobs: call an LLM, call a tool, wait for a human, call another tool.
If the process dies midway, you either lose all progress or restart and repeat side effects
(a refund issued twice, an email sent twice).

## The idea

Record every step in PostgreSQL as an append-only event log. If a worker dies, another worker loads
the log, reuses results of finished steps (replay), and continues. A run can also be forked from any
point to try a different model or prompt.

## Guarantees (and honest limits)

This runtime aims for **at-least-once execution with idempotent external effects where supported**.
It does *not* claim universal exactly-once execution, which is impossible in general.

- Idempotency only prevents duplicates if the external service honors the idempotency key.
- Fencing tokens protect database writes only, not calls a stale worker makes to external APIs
  (idempotency keys cover those; see [docs/workers-and-leases.md](docs/workers-and-leases.md)).
- When an outcome is uncertain and the tool is not idempotent, the step is recorded as
  `OUTCOME_UNKNOWN` and routed to reconciliation instead of being blindly retried.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 0 | Environment, Git, project skeleton | done |
| 1 | Workflow SDK, schema, event persistence, basic API | done |
| 2 | Deterministic replay, recorded time/random/LLM | done |
| 3 | Idempotency, fake payment/email services, OUTCOME_UNKNOWN | done |
| 4 | Multi-worker, leases, heartbeats, fencing | todo |
| 5 | Durable timers and signals | todo |
| 6 | Forking and run diff | todo |
| 7 | Failure injection, chaos tests, benchmarks | todo |
| 8 | Dashboard and documentation | todo |

## Tech stack

Python 3.12+, FastAPI, PostgreSQL, SQLAlchemy, Alembic, asyncio, Pydantic, httpx, pytest, Hypothesis,
Docker Compose.

## Project layout

```text
app/
  api/            FastAPI app and routes
  engine/         runtime, workers, replay, leases, timers, signals, forking
  database/       models, sessions, repositories
  sdk/            @workflow decorator and execution context
  integrations/   LLM providers (mock + OpenAI) and fake external services
  examples/       refund and leave-approval workflows
tests/            unit, integration, recovery, concurrency, chaos
migrations/       Alembic migrations
docs/             design documents
scripts/          helper scripts
```

## What works today (Phase 4)

```python
from app.sdk import workflow

@workflow
async def refund(ctx, order_id: str):
    order = await ctx.step("fetch_order", fetch_order, order_id)          # safe to repeat
    verdict = await ctx.llm("classify", prompt=f"Refund order {order_id}? APPROVE or REJECT")
    if verdict != "APPROVE":
        return "rejected"
    refund = await ctx.effect("process_refund", payments.refund,           # real-world effect
                              order_id, order["amount"], idempotent=True)
    await ctx.effect("send_email", emails.send, order["email"], "Refunded", "...", idempotent=True)
    return refund
```

- **`ctx.step`** runs repeat-safe work and records it. A failing step raises `StepFailedError`.
- **`ctx.effect`** runs calls with real-world effects. An idempotent tool gets a stable key
  (`<run_id>:<step>`) and is retried with the *same* key after a crash or timeout, so it cannot act twice.
  A non-idempotent tool is never retried blindly: the runtime asks its `verify` hook whether the effect
  happened, and otherwise records `STEP_OUTCOME_UNKNOWN` and parks the run in `NEEDS_REVIEW` until a person
  resolves it through the API.
- **`ctx.now()`, `ctx.random()`, `ctx.llm()`** record their value, so a replay returns the same timestamp,
  number or model answer (LLM calls also record model, prompt hash, tokens, cost and latency).
- **Resume:** `resume_run` re-runs a crashed `RUNNING` run from its history; completed steps return their
  saved results without executing. A changed step name or input raises `NonDeterminismError`.
- **Independent ledgers:** the fake payment and email services keep their own books in a separate database,
  so tests compare the runtime's history against what the "outside world" actually recorded.
- The event log is append-only (enforced by a PostgreSQL trigger) with per-run sequence numbers.
- **Workers** (`python -m app.engine.worker`) claim runs with `FOR UPDATE SKIP LOCKED`, hold a **lease** they
  renew with a heartbeat, and write every event with a **fencing token**. If a worker dies, its lease
  expires and another worker resumes the run from the history. If a worker is merely frozen and wakes up
  later, the database rejects its writes (`StaleLeaseError`) and it stands down.

**Guarantee, stated honestly:** at-least-once execution with duplicate protection where the external system
supports idempotency keys (or can be asked via a verify hook). It does not claim exactly-once.

**Not yet:** a run cannot wait for a timer or a signal (`ctx.sleep`, `ctx.wait_for_signal`) and cannot be forked.
Idempotency keys are per run and step, so two separate runs for the same order are two operations. Fencing
cannot recall an external call a stale worker already started. A run executed directly with
`execute_run`/`resume_run` (not by a worker) is not recovered automatically. One run at a time per worker process.

See [docs/workers-and-leases.md](docs/workers-and-leases.md) (claiming, leases, fencing, failure table),
[docs/idempotency.md](docs/idempotency.md) (decision table, review path, what is and is not protected,
the bug the property test found), [docs/replay-and-recovery.md](docs/replay-and-recovery.md),
[docs/database-design.md](docs/database-design.md) and [docs/workflow-lifecycle.md](docs/workflow-lifecycle.md).

## Demos

```bash
python scripts/demo_crash_recovery.py     # kill a worker mid-run, resume in a new process
python scripts/demo_duplicate_refund.py   # worker dies after the refund succeeded: still ONE refund
python scripts/demo_multi_worker.py       # three worker processes share twelve runs, each run once
python scripts/demo_zombie_worker.py      # a frozen worker wakes up after a takeover and is rejected
```

The second demo kills the worker after the payment service processed the refund but before the runtime
recorded it, resumes in a new process, and shows the payment service's own ledger: one refund, the same
`refund_id` before and after. The fourth freezes a worker for 8 seconds right after its refund succeeded; a
second worker takes the run over once the lease lapses (fencing token 2) and finishes it; when the first
worker thaws, its write is rejected, the history is unchanged and the ledger still shows one refund.

## Getting started

Requirements: Python 3.12+, Git, and either Docker or a local PostgreSQL 16 (UTF-8 encoding).

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env             # Windows: copy .env.example .env  (never commit .env)

docker compose up -d postgres    # starts PostgreSQL and creates both databases
alembic upgrade head             # creates the tables
uvicorn app.api.main:app --reload
# interactive docs: http://localhost:8000/docs
```

By default the API process executes runs itself (`EXECUTION_MODE=inline`). To use worker processes instead:

```bash
EXECUTION_MODE=workers uvicorn app.api.main:app --reload     # Windows PowerShell: $env:EXECUTION_MODE="workers"
python -m app.engine.worker --id worker-1                    # in another terminal; start as many as you like
# or: docker compose up -d --build --scale worker=3
curl localhost:8000/workers                                  # who is registered and alive
```

Try it:

```bash
curl -X POST localhost:8000/workflows/order_summary/runs \
     -H "content-type: application/json" -d '{"input": {"order_id": "A-100"}}'
curl localhost:8000/runs                      # list runs
curl localhost:8000/runs/<run_id>/events      # the recorded timeline
```

## Running the tests

```bash
pytest                         # everything
pytest -m "not integration"    # unit tests only, no database needed
pytest -m recovery             # crash/resume tests, including killed worker processes
pytest -m chaos                # randomized tests (CHAOS_EXAMPLES=400, CHAOS_SEEDS=30 for longer runs)
pytest tests/concurrency       # claiming, leases, fencing, zombie workers
```

Integration tests use the database in `TEST_DATABASE_URL` (default
`postgresql+asyncpg://durable:durable@localhost:5432/durable_test`). It is created if missing and its
schema is **dropped and rebuilt** at the start of each test session, so the name must end in `_test`.
The fake services use `TEST_SERVICES_DATABASE_URL` (default `.../durable_services_test`) the same way.
If PostgreSQL is unreachable the integration tests are reported as skipped, not passed.

## License

To be decided.

# Durable AI Agent Runtime

A fault-tolerant execution engine for AI workflows: crash-safe, replayable, and forkable.

> **Status: Phase 2 (deterministic replay).** Workflows run, every step is recorded in PostgreSQL, and a
> crashed run can be resumed from its history. Idempotency, worker processes, timers, signals and forking
> are **not implemented yet** (see the roadmap). This README is updated at every milestone so it never claims more than the code does.

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
- Fencing tokens protect database writes only, not calls a stale worker makes to external APIs.
- When an outcome is uncertain and the tool is not idempotent, the step is recorded as
  `OUTCOME_UNKNOWN` and routed to reconciliation instead of being blindly retried.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 0 | Environment, Git, project skeleton | done |
| 1 | Workflow SDK, schema, event persistence, basic API | done |
| 2 | Deterministic replay, recorded time/random/LLM | done |
| 3 | Idempotency, fake payment/email services, OUTCOME_UNKNOWN | todo |
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

## What works today (Phase 2)

```python
from app.sdk import workflow

@workflow
async def refund(ctx, order_id: str):
    order = await ctx.step("fetch_order", fetch_order, order_id)
    decision = await ctx.llm("refund_decision", prompt=f"Refund order {order_id}?")
    when = await ctx.now()
    if decision == "APPROVE":
        return await ctx.step("process_refund", process_refund, order["amount"])
    return "rejected"
```

- `ctx.step(name, fn, *args)` runs `fn` and records `STEP_STARTED` and `STEP_COMPLETED`/`STEP_FAILED`
  (one committed transaction per event). A failing step raises `StepFailedError`.
- `await ctx.now()`, `await ctx.random()` and `await ctx.llm(name, prompt=...)` record their value, so a
  replay returns the same timestamp, number or model answer. LLM calls also record model, prompt hash,
  tokens, estimated cost and latency. A free mock provider is the default; OpenAI is optional.
- **Resume:** `resume_run` re-runs a crashed `RUNNING` run from its history. Completed steps return
  their saved results without executing; execution continues at the first step with no outcome.
- **Divergence detection:** a changed step name, changed input (e.g. refund `2000` -> `3000`) or removed
  step raises `NonDeterminismError` instead of silently reusing an unrelated result.
- The event log is append-only (enforced by a PostgreSQL trigger) with per-run sequence numbers.
- Step arguments and results must be JSON-serializable. Step names must be unique within a run.

**Not yet:** if a worker dies *after* a step's side effect but *before* it is recorded, resume runs
that step again, so the effect can happen twice (idempotency is Phase 3). Nothing resumes runs
automatically and `resume_run` must only be called when the old executor is dead (workers and leases
are Phase 4). Execution still happens inside the API process. `ctx.sleep/wait_for_signal/fork` do
not exist yet.

See [docs/replay-and-recovery.md](docs/replay-and-recovery.md) (what each crash leaves behind and how
resume handles it), [docs/database-design.md](docs/database-design.md) and
[docs/workflow-lifecycle.md](docs/workflow-lifecycle.md).

## Demo: crash and recovery

```bash
python scripts/demo_crash_recovery.py
```

Starts a run in a worker process, kills the process at a controlled point, shows what survived in
PostgreSQL, resumes the run in a new process, and prints the final timeline: completed steps appear
once because the second process reused their saved results.

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
```

Integration tests use the database in `TEST_DATABASE_URL` (default
`postgresql+asyncpg://durable:durable@localhost:5432/durable_test`). It is created if missing and its
schema is **dropped and rebuilt** at the start of each test session, so the name must end in `_test`.
If PostgreSQL is unreachable the integration tests are reported as skipped, not passed.

## License

To be decided.

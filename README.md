# Durable AI Agent Runtime

A fault-tolerant execution engine for AI workflows: crash-safe, replayable, and forkable.

> **Status: Phase 1 (basic workflow engine).** Workflows run and every step is recorded in PostgreSQL.
> Replay, crash recovery, idempotency, workers, timers, signals and forking are **not implemented yet**
> (see the roadmap). This README is updated at every milestone so it never claims more than the code does.

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
| 2 | Deterministic replay, recorded time/random/LLM | todo |
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

## What works today (Phase 1)

```python
from app.sdk import workflow

@workflow
async def order_summary(ctx, order_id: str):
    items = await ctx.step("fetch_order", fetch_order, order_id)
    total = await ctx.step("calculate_total", calculate_total, items)
    return {"order_id": order_id, "total": total}
```

- `@workflow` registers an async function; `ctx.step(name, fn, *args)` runs `fn` and records
  `STEP_STARTED` and `STEP_COMPLETED`/`STEP_FAILED` events (one committed transaction per event).
- Step arguments and results must be JSON-serializable. Step names must be unique within a run.
- The event log is append-only, enforced by a PostgreSQL trigger, with per-run sequence numbers.
- A run moves `PENDING -> RUNNING -> COMPLETED | FAILED`; starting the same run twice is rejected.

**Not yet:** if the process dies mid-run the run stays `RUNNING` and cannot be resumed (replay is
Phase 2), execution happens inside the API process (worker processes are Phase 4), and
`ctx.llm/now/random/sleep/wait_for_signal/fork` do not exist yet.

See [docs/database-design.md](docs/database-design.md) and
[docs/workflow-lifecycle.md](docs/workflow-lifecycle.md).

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
pytest                         # unit tests + integration tests
pytest -m "not integration"    # unit tests only, no database needed
```

Integration tests use the database in `TEST_DATABASE_URL` (default
`postgresql+asyncpg://durable:durable@localhost:5432/durable_test`). It is created if missing and its
schema is **dropped and rebuilt** at the start of each test session, so the name must end in `_test`.
If PostgreSQL is unreachable the integration tests are reported as skipped, not passed.

## License

To be decided.

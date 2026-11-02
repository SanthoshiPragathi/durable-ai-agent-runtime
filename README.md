# Durable AI Agent Runtime

A fault-tolerant execution engine for AI workflows: crash-safe, replayable, and forkable.

> **Status: Phase 0 (project skeleton).** Nothing below the "Roadmap" heading is implemented yet.
> This README is updated at every milestone so it never claims more than the code does.

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
| 1 | Workflow SDK, schema, event persistence, basic API | todo |
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

## Getting started

Requirements: Python 3.12+, Git, and either Docker or a local PostgreSQL 16.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # then edit if needed; never commit .env

docker compose up -d postgres # starts PostgreSQL and creates both databases
pytest                        # Phase 0: runs the smoke tests
uvicorn app.api.main:app --reload
# open http://localhost:8000/health and http://localhost:8000/docs
```

## License

To be decided.

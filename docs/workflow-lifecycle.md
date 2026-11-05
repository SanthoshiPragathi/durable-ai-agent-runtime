# Workflow lifecycle (Phase 1)

## Run states

```mermaid
stateDiagram-v2
    [*] --> PENDING: create_run
    PENDING --> RUNNING: execute_run (atomic)
    RUNNING --> COMPLETED: workflow returned
    RUNNING --> FAILED: workflow or step raised
    RUNNING --> RUNNING: resume_run after a crash
    RUNNING --> NEEDS_REVIEW: a step's outcome is unknown
    NEEDS_REVIEW --> RUNNING: a person resolves the step
```

`WAITING` and `CANCELLED` arrive with timers, signals and cancellation in later phases.

## What happens for one run

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant R as Runtime
    participant DB as PostgreSQL
    C->>A: POST /workflows/{name}/runs
    A->>R: create_run (validate input)
    R->>DB: INSERT run (PENDING)
    A-->>C: 202 + run
    A->>R: execute_run (background task)
    R->>DB: PENDING->RUNNING + WORKFLOW_STARTED (one tx)
    loop each ctx.step
        R->>DB: STEP_STARTED (commit)
        R->>R: call the step function
        R->>DB: STEP_COMPLETED or STEP_FAILED (commit)
    end
    R->>DB: WORKFLOW_COMPLETED/FAILED + run status (one tx)
    C->>A: GET /runs/{id}/events
```

## Event types

| Event | When |
|---|---|
| `WORKFLOW_STARTED` | The run moved from PENDING to RUNNING |
| `STEP_OUTCOME_UNKNOWN` | A non-idempotent effect's outcome cannot be established; the run moves to `NEEDS_REVIEW` |
| `STEP_RESOLVED` | A reviewer answered (`completed`, `failed` or `not_executed`) and who decided; for `completed` this is the step's completion record |
| `WORKFLOW_RESUMED` | `resume_run` started replaying a crashed run; carries counts of completed, failed and in-doubt steps |
| `STEP_STARTED` | Before the step function is called; carries the input hash |
| `STEP_COMPLETED` | The step returned; carries the JSON result |
| `STEP_FAILED` | The step raised; carries error type and message (no stack trace) |
| `WORKFLOW_COMPLETED` / `WORKFLOW_FAILED` | The workflow function returned or raised |

## Rules for workflow authors

- Define workflows with `async def`; the first parameter is the context, the rest come from the run input.
- Arguments and results of `ctx.step` must be JSON-serializable.
- Step names must be unique within a run; in a loop use `f"fetch_{i}"`.
- Put anything with side effects or nondeterminism inside a step, never directly in the workflow body.
  Use `await ctx.now()`, `await ctx.random()` and `await ctx.llm(...)` for time, randomness and models.
- Handle step failures with `except StepFailedError`.
- Use `ctx.effect`, not `ctx.step`, for anything with a real-world side effect.

The full list, with the reasons, is in [replay-and-recovery.md](replay-and-recovery.md).

## Current limitations (honest list)

- Protection against duplicate effects depends on the external system honoring idempotency keys (or a
  verify hook, or a human); see [idempotency.md](idempotency.md).
- Workers (Phase 4) recover crashed runs automatically, see [workers-and-leases.md](workers-and-leases.md).
  A run executed directly with `execute_run`/`resume_run` is not recovered automatically, and
  `resume_run` refuses a run that has a worker lease.
- With `EXECUTION_MODE=inline` (the default) the API process still executes runs itself.
- Failed steps are not retried; there are no retry limits or backoff yet.

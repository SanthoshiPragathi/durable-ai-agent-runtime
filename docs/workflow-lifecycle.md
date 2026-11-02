# Workflow lifecycle (Phase 1)

## Run states

```mermaid
stateDiagram-v2
    [*] --> PENDING: create_run
    PENDING --> RUNNING: execute_run (atomic)
    RUNNING --> COMPLETED: workflow returned
    RUNNING --> FAILED: workflow or step raised
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
| `STEP_STARTED` | Before the step function is called; carries the input hash |
| `STEP_COMPLETED` | The step returned; carries the JSON result |
| `STEP_FAILED` | The step raised; carries error type and message (no stack trace) |
| `WORKFLOW_COMPLETED` / `WORKFLOW_FAILED` | The workflow function returned or raised |

## Rules for workflow authors

- Define workflows with `async def`; the first parameter is the context, the rest come from the run input.
- Arguments and results of `ctx.step` must be JSON-serializable.
- Step names must be unique within a run; in a loop use `f"fetch_{i}"`.
- Put anything with side effects or nondeterminism inside a step, never directly in the workflow body.
  Replay (Phase 2) will re-run the workflow body, so only steps are safe to skip.

## Current limitations (honest list)

- No replay: a crash mid-run leaves the run `RUNNING`; it is not resumed yet. (Phase 2)
- Execution runs inside the API process, not in worker processes. (Phase 4)
- A step that is `STEP_STARTED` with no outcome after a crash cannot be reconciled yet. (Phase 3)
- Failed steps are not retried; there are no retry limits or backoff yet.

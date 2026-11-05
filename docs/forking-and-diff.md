# Forking and run diff (Phase 6)

## Fork

`fork_run(session_factory, run_id, from_step, override_result=..., input_overrides=...)` or
`POST /runs/{id}/fork` creates a **new run** that starts from part of an existing run's history.

1. The steps **before** `from_step` are copied into the new run as already-completed events.
2. The new run is made claimable (`RUNNING`, no owner, expired lease). Any worker claims it, replays the copied
   steps (their functions are **not** called again) and runs live from `from_step` onward.
3. With `override_result`, `from_step` itself is recorded as completed with your value instead of being run.
   This answers "what if the LLM had said REJECT?" without calling the LLM again.

The source run is never touched. The fork has its own run id, `parent_run_id` set, and a `WORKFLOW_STARTED`
event recording `forked_from`, `from_step`, `overridden` and which inputs were overridden.

| Case | Result |
|---|---|
| Unknown step, or run not started | `ForkError` (HTTP 409) |
| An earlier step failed / is unknown | `ForkError`: it has no result to reuse |
| Forking AT a failed step | allowed: it is re-executed |
| `input_overrides` that an earlier recorded step depended on | the fork stops with `NonDeterminismError` at replay (correct: that is not a valid fork) |

### Effects and forks (read this)

A fork does not undo anything the source did, and an `effect` step that runs in the fork uses the **fork's**
run id in its idempotency key (`<fork_id>:<step>`). So re-running a refund step in a fork refunds again. That is
the right behaviour for "run it again" and the wrong one for "just explore"; use `override_result` on the
effect step to skip it.

## Diff

`diff_runs(session_factory, a, b)` or `GET /runs/{a}/diff/{b}` compares two runs by step name: status, whether the
recorded input hash matches, whether the result matches, the first step that differs, and both runs' final
status and result. It reads recorded history only, so it works on any two runs, forks or not.

## Tests

`tests/integration/test_forks.py` (7 tests): earlier steps replayed not re-run; override skips the step and changes
later results; diff of a run with itself is clean; all error cases; fork at a failed step; input override; API.
Deliberate bugs that the tests catch: copying one step too many, a diff that ignores results, forgetting
`parent_run_id`.

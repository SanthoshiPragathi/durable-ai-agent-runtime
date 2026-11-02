# Test report (through Phase 3)

Only results that were actually observed are listed here. Re-run them yourself with the commands at the end.

## Environment of the recorded run

| | |
|---|---|
| OS | Linux (Ubuntu 24.04), container |
| Python | 3.13.18 |
| PostgreSQL | 16.15 (UTF-8) |
| Key packages | pytest 9.2.0, Hypothesis 6.170.14, SQLAlchemy 2.1.6, asyncpg 0.32.0 |
| Also confirmed by the author | Phases 1 and 2 (64 and 121 tests) pass on Windows, Python 3.12.6, PostgreSQL 16 in Docker. Phase 3 on Windows: not yet recorded here. |

## Results

| Suite | Command | Tests | Result |
|---|---|---|---|
| Everything | `pytest` | 229 | **229 passed** (about 70 s) |
| Unit (no database) | `pytest -m "not integration"` | 91 | passed |
| Integration (PostgreSQL) | `pytest -m integration` | 138 | passed |
| of which crash/resume (`recovery`) | `pytest -m recovery` | 71 | passed |
| of which randomized (`chaos`) | `pytest -m chaos` | 2 | passed |
| Property test, extended run | `CHAOS_EXAMPLES=400 pytest -m chaos -k no_crash_pattern` | 400 random scenarios | **passed** (3 min 41 s) |
| Lint | `ruff check .` | | clean |

(Markers overlap: recovery and chaos tests are also integration tests.)

## What the failure tests prove

For every crash point the question is the same: what is on disk, who resumes, and can an effect happen twice?
The "world" column is read from the fake services' own ledger, a separate database.

| # | Failure | Persisted at the moment of failure | After restart | World (ledger) | Duplicate possible? How handled |
|---|---|---|---|---|---|
| 1 | Crash before `STEP_STARTED` | earlier steps completed | new process replays them, runs the step | 0 → 1 refund | No: step never started |
| 2 | Crash after `STEP_STARTED` | `STEP_STARTED`, no outcome | step in doubt, called again with the same key | 0 → 1 | No: effect had not happened |
| 3 | Crash before an external call | same as 2 | same as 2 | 0 → 1 | No |
| 4 | Crash after the external call succeeded | `STEP_STARTED`, no outcome | step in doubt, **same idempotency key**, provider returns the original | 1 → 1 | **Prevented** by the key |
| 5 | Crash before `STEP_COMPLETED` is persisted | same as 4 | same as 4 | 1 → 1 | **Prevented** by the key |
| 6 | Crash right after `STEP_COMPLETED` | result recorded | replayed, not executed again | 1 → 1 | No: step is complete |
| 7 | Crash after the last step | all steps recorded | replays everything, records completion | unchanged | No |
| 8 | Three workers die in a row | whatever each committed | fourth finishes; attempts = 3 | 1 → 1 | **Prevented** |
| 9 | Provider reply lost after commit | refund in ledger; caller saw a timeout | retried with the same key | 1 | **Prevented** |
| 10 | Provider request lost before commit | nothing | retried (3 attempts) | 1 | No |
| 11 | Payment declined | `STEP_FAILED`, run `FAILED` | not resumable | 0 | No: nothing happened |
| 12 | Email fails after the refund | refund completed, `STEP_FAILED` | run `FAILED` | 1 refund, 0 emails | No; the refund stands |
| 13 | LLM call fails | `STEP_FAILED`, run `FAILED` | not resumable | 0 | No |
| 14 | Workflow code/input changed during replay | history unchanged | `NonDeterminismError`, nothing executes, run stays `RUNNING` | unchanged | No |
| 15 | Non-idempotent provider, crash after effect, no verify | `STEP_STARTED`, no outcome | `STEP_OUTCOME_UNKNOWN`, run `NEEDS_REVIEW`, **not retried** | 1 → 1 | **Prevented** by refusing to retry |
| 16 | Same, with a verify hook | same | verify finds the refund, step `RECONCILED` | 1 → 1 | **Prevented** by reconciliation |
| 17 | Plain `ctx.step` around a payment (the wrong way) | `STEP_STARTED` | step runs again | 1 → **2** | **Yes**: shown on purpose; use `ctx.effect` |

Not yet tested (later phases): crash during heartbeat renewal, stale worker after lease expiry, two workers
claiming one run, PostgreSQL unavailable or restarted, duplicate signals, duplicate job execution. These need
workers, leases, timers and signals (Phases 4, 5, 7).

## A bug the tests found

The randomized test found a real duplicate-refund bug in the first Phase 3 implementation (a reviewer's
"not executed" answer was never expired). Full account and the regression tests: see
[idempotency.md](idempotency.md#a-bug-the-property-test-found).

## Checks that the tests can fail

Mutation checks (change the code, confirm the tests catch it, restore it):

| Change | Result |
|---|---|
| Do not pass the idempotency key to the tool | headline duplicate-refund test fails |
| New random key on every attempt | 4 recovery tests fail; the property test finds a duplicate on its own |
| Retry a non-idempotent step blindly instead of `OUTCOME_UNKNOWN` | recovery tests fail |
| Do not halt after `OutcomeUnknownError` | unit tests fail |
| Skip the saved result for completed steps (Phase 2) | 8 recovery tests fail |
| Remove the event-sequence row lock (Phase 1) | concurrent-append test fails |
| Remove the expiry of a reviewer's answer | 2 regression tests fail |

## Re-run

```bash
pytest                                              # everything
pytest -m "not integration"                         # unit tests only (no database)
pytest -m recovery                                  # crash and resume, incl. killed processes
CHAOS_EXAMPLES=400 pytest -m chaos                  # extended randomized run
python scripts/demo_crash_recovery.py               # Demo 2
python scripts/demo_duplicate_refund.py             # Demo 3
```

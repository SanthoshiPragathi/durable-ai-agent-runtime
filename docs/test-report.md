# Test report (through Phase 4)

Only results that were actually observed are listed here. Re-run them yourself with the commands at the end.

## Environment of the recorded run

| | |
|---|---|
| OS | Linux (Ubuntu 24.04), container |
| Python | 3.13.18 |
| PostgreSQL | 16.15 (UTF-8) |
| Key packages | pytest 9.2.0, Hypothesis 6.170.14, SQLAlchemy 2.1.6, asyncpg 0.32.0 |
| Also confirmed by the author | Phases 1, 2 and 3 (64, 121 and 229 tests) pass on Windows, Python 3.12.6, PostgreSQL 16 in Docker (Phase 3: 229 passed in about 3-4 minutes there). Phase 4 on Windows: not yet recorded here. |

## Results

| Suite | Command | Tests | Result |
|---|---|---|---|
| Everything | `pytest` | 275 | **275 passed** (3 min 16 s) |
| Unit (no database) | `pytest -m "not integration"` | 91 | passed |
| Integration (PostgreSQL) | `pytest -m integration` | 184 | passed |
| of which crash/resume (`recovery`) | `pytest -m recovery` | 80 | passed |
| of which multi-worker (`concurrency`) | `pytest -m concurrency` | 40 | passed |
| of which randomized (`chaos`) | `pytest -m chaos` | 3 | passed |
| Property test, extended run (Phase 3) | `CHAOS_EXAMPLES=400 pytest -m chaos -k no_crash_pattern` | 400 random scenarios | **passed** (3 min 41 s) |
| Lease chaos, extended run (Phase 4) | `CHAOS_SEEDS=30 pytest tests/chaos/test_lease_chaos.py -s` | 30 random schedules | **passed** (2 min 14 s): 984 leases force-expired, 333 takeovers, 398 stale workers stood down, no duplicate refund or email |
| Lint | `ruff check .` | | clean |

(Markers overlap: recovery, concurrency and chaos tests are also integration tests.)

## What the Phase 4 tests prove

| Claim | Test |
|---|---|
| Concurrent claimers never receive the same run (10 claimers, 40 runs) | `tests/concurrency/test_claims.py` |
| A run with a live lease is not claimable; an expired one is, with a higher token | same |
| A run locked by an in-flight write is skipped, not waited for | same |
| A stale token is rejected and nothing is written; wrong owner name rejected; finished runs reject late writes | same |
| A takeover cannot slip in between a fence check and its write (row lock) | same |
| Six in-process workers run 24 runs, each exactly once | `tests/concurrency/test_worker.py` |
| The heartbeat keeps a run whose step outlasts its lease from being taken (lease 0.6 s, step 1.6 s) | same |
| A zombie is stopped by fencing alone, and separately by its own heartbeat noticing; a workflow that swallows the error still cannot continue; a worker frozen between STEP_STARTED and the step function does not run it | same |
| Three real worker processes share 15 runs, each run once | `tests/recovery/test_process_workers.py` |
| A worker killed at 5 exact points is replaced automatically: one refund, one email, tokens 1 then 2 | same |
| A worker frozen 8 s (loop and heartbeat) at 3 exact points wakes up after a takeover and is rejected: ledgers unchanged, nothing appended after `WORKFLOW_COMPLETED` | same |
| `EXECUTION_MODE=workers`: the API only records; a worker runs; resolving a review step hands the run to a worker | `tests/integration/test_api_workers.py` |
| Random lease expiry under load never duplicates a refund/email or corrupts a history | `tests/chaos/test_lease_chaos.py` |

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
PostgreSQL unavailable or restarted, duplicate signals, duplicate timers. These need timers, signals and the
Phase 7 chaos harness. (Claiming a run twice and stale workers are now covered; see above.)

## A bug the tests found

The randomized test found a real duplicate-refund bug in the first Phase 3 implementation (a reviewer's
"not executed" answer was never expired). Full account and the regression tests: see
[idempotency.md](idempotency.md#a-bug-the-property-test-found).

## Bugs the tests found (Phase 4)

The lease chaos test found a real defect in the first worker implementation: a run could finish with a
history that began with `WORKFLOW_RESUMED` and no `WORKFLOW_STARTED`. Worker A claimed the run and was taken
over before writing anything; worker B saw a RUNNING run and wrote RESUMED. The run was still correct (steps
once, one refund) but the timeline lied about how it began. The worker now decides STARTED versus RESUMED from
the history itself; a regression test fails without the fix. (An earlier failure of the same test was a flaw in
the test, not the runtime: its lease-breaking UPDATE could touch a run that had just completed. Fixed in the test.)

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
| Fence check never raises (Phase 4) | stale-token tests fail |
| Skip the `ensure_lease` check before a step function | the frozen-between-STARTED-and-run test fails |
| Claim without `SKIP LOCKED` | the locked-row test fails |
| Do not increment the fencing token on claim | claim test fails |
| Decide STARTED/RESUMED from the claim instead of the history | the regression test fails |

## Re-run

```bash
pytest                                              # everything
pytest -m "not integration"                         # unit tests only (no database)
pytest -m recovery                                  # crash and resume, incl. killed processes
CHAOS_EXAMPLES=400 pytest -m chaos                  # extended randomized run
python scripts/demo_crash_recovery.py               # Demo 2
python scripts/demo_duplicate_refund.py             # Demo 3
python scripts/demo_multi_worker.py                 # Demo 4
python scripts/demo_zombie_worker.py                # Demo 5
pytest -m concurrency                               # claims, leases, fencing, zombies
CHAOS_SEEDS=30 pytest tests/chaos/test_lease_chaos.py -s
```

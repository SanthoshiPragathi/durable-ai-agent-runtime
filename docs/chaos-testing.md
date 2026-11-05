# Failure injection and chaos tests (Phase 7)

What each test breaks, what judges the result, and what it proves. "Independent judge" means the check
does not rely on the runtime's own bookkeeping: the fake payment/email ledgers live in a separate database.

| Test | What is injected | Judge | Meaning |
|---|---|---|---|
| `tests/chaos/test_lease_chaos.py` | leases forced to expire under live workers (false failure detection) | ledgers + event-log invariants | takeovers and stale workers never duplicate an effect |
| `tests/chaos/test_property_effects.py` | random crash/retry patterns (Hypothesis) | ledgers | idempotent effects are never repeated |
| `tests/chaos/test_db_outage.py` | the runtime database refuses every statement for 50-400 ms windows during the first 8 s of each seed (then calm, so a slow machine can finish), and 8% of recorder writes commit but report failure (lost ack) | ledgers + history invariants | an outage is never recorded as a failed run; nothing is duplicated; every run completes |
| `tests/chaos/test_postgres_restart.py` | PostgreSQL really restarted (`PG_RESTART_CMD`) under 3 worker processes | ledgers + history | workers survive the restart, every run completes with one refund and one email |
| `tests/chaos/test_signal_timer_storm.py` | every signal delivered 1-4 times concurrently, some never sent, short timeouts, extra timer firers | stored signals + step results | a signal is consumed at most once; timeouts and signals never both win a wait |
| `tests/recovery/test_process_*` | real processes killed at exact points (failpoints) | ledgers + history | each crash window recovers (Phases 2-5) |

## How the runtime handles a database outage

* Errors of the infrastructure under the runtime (`SQLAlchemyError`, connection errors, timeouts, asyncpg errors)
  are **not workflow failures**. `_drive` re-raises them without writing `WORKFLOW_FAILED` (the database that
  would store the failure is the thing that is down), and the worker abandons the run. Its lease lapses and
  another worker replays the recorded history once the database is back.
* The worker loop (`claim`, `touch`, `drain` check, registration) retries after the poll interval instead of dying.
* A write whose acknowledgement was lost is safe either way: if it committed, the replay finds the event; if not,
  the step is in doubt and the effect's idempotency key (or verify hook) decides, as in Phase 3.
* The fake providers translate their own connection errors into `AmbiguousOutcomeError`, the way a real client
  library reports a dropped connection to a payment API.

## Mutation checks (a deliberate bug must make the tests fail; all were caught)

* an infrastructure error recorded as `WORKFLOW_FAILED` -> outage test fails
* the worker loop not surviving a database error -> outage test fails
* signal idempotency removed -> storm test fails

## Limits

* The restart test needs a way to restart the database (`PG_RESTART_CMD`); without it the test is skipped.
  `scripts/verify.ps1` sets it for Docker. The sandbox run used `pg_ctl restart -m fast`, a short (about 0.3 s)
  outage; a Docker restart is longer, which exercises lease expiry harder.
* Workflow code that catches the database exception itself and carries on is outside what the runtime can protect.
* Disk-full, partial-write and clock-jump faults are not injected.

## If a database test hangs (Windows / Docker Desktop)

Every connection to PostgreSQL is now bounded: `DB_CONNECT_TIMEOUT_SECONDS` (default 10) covers opening a
connection including the handshake, `DB_COMMAND_TIMEOUT_SECONDS` (default 60) covers one statement. A database
that accepts the TCP connection but never answers therefore fails fast with an error (the worker counts it as an
infrastructure error and retries on its next poll) instead of blocking forever. If a test now fails with a
connect timeout instead of hanging, the database really is not answering; check, in order:

1. `docker compose ps` shows `postgres` healthy, and `docker compose logs postgres --tail 50` has no crash loop.
2. Try `127.0.0.1` instead of `localhost` (`TEST_DATABASE_URL=postgresql+asyncpg://durable:durable@127.0.0.1:5432/durable_test`
   and `TEST_SERVICES_DATABASE_URL=...@127.0.0.1:5432/durable_services_test`). On Windows `localhost` can resolve to `::1`
   first, and Docker Desktop publishes the port on `127.0.0.1` only.
3. The chaos tests open many short-lived connections (about 100 per second in `test_db_outage.py`, because the test
   engines use `NullPool`). A slow Docker Desktop port proxy may stall under that rate.

# Interview guide: Durable AI Agent Runtime

## 30-second pitch

"AI agents call LLMs, take minutes or days, and trigger real actions like refunds. If the process crashes halfway,
a naive agent either loses its progress or repeats the refund. I built a runtime where a workflow's progress is an
append-only event log in PostgreSQL. After any crash, another worker replays the log and continues; completed
steps are not re-run, and real-world calls carry idempotency keys so a retry cannot act twice. It also has
leases and fencing for multi-worker safety, durable timers and signals, forking and diffing of runs, and chaos
tests including a real database restart."

## Numbers you can quote (all from `docs/test-report.md` and `docs/benchmarks.md`)

* 317 tests passing on Linux; 316 plus all demos and the benchmarks verified on Windows/Docker; 8 demos.
* Chaos: 30 random lease-expiry schedules (984 leases force-expired, 333 takeovers, 398 stale workers rejected,
  no duplicate refund or email); outage test with 4,222 rejected statements and 39 lost acknowledgements; real
  PostgreSQL restart with 12/12 runs completed.
* Benchmarks (2-core sandbox, so modest): ~22 runs/s with 2 workers; replaying 500 recorded steps about 70 ms;
  claim latency p50 2.6 ms; recovery after a crash about one lease length (1.5 s in the test).
* Bugs the tests found in my own code: duplicate refund after a second crash (property test), a heartbeat
  treating normal completion as a lost lease (found on Windows), STARTED-vs-RESUMED event after an early takeover.

## Questions you will be asked

**Why event sourcing instead of a state column?**
The log is the single source of truth and is append-only (a database trigger blocks UPDATE/DELETE), so history
cannot be silently rewritten, replay is possible, and debugging is "read the timeline". A state column cannot
tell you whether a step ran.

**What is deterministic replay and what breaks it?**
Re-running the workflow function against recorded results. It breaks if the code takes a different path:
`datetime.now()`, `random()`, an LLM answer, or edited code. So `ctx.now/random/llm` are recorded, and a changed
step name or input raises `NonDeterminismError` instead of reusing the wrong result.

**Do you guarantee exactly-once?**
No, and I say so. The guarantee is at-least-once execution with idempotent effects where the provider supports
keys or can be queried. Exactly-once across a crash is impossible without the other side's cooperation.

**What happens if the worker dies after the payment succeeds but before it records it?**
The step is "in doubt" on resume. For an idempotent effect it is retried with the same key and the provider
returns the original refund (demo 3). For a provider that cannot dedupe, a verify hook checks the provider; with
no hook the run is parked as NEEDS_REVIEW for a person (the runtime refuses to guess).

**Why leases AND fencing tokens?**
A lease lets another worker take over a dead worker's run. But a worker that is only frozen (GC pause, VM stall)
can wake up believing it still owns the run. The fencing token, checked under the run's row lock on every write,
makes the database reject that zombie. Leases decide who should work; fencing makes a wrong guess harmless.

**What does fencing not protect?**
External calls the zombie already started. That is why effects also carry idempotency keys: the two mechanisms
cover the two halves of the problem.

**How do two workers not claim the same run?**
One atomic `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)`. Locked rows are skipped, not waited for.
Tested with 10 concurrent claimers over 40 runs.

**How does a durable wait avoid losing a signal?**
The signal sender, the timer firer and the wait evaluation all take the run's row lock. Either the signal commits
first and the wait finds it, or the wait parks first and the signal wakes the run. Tested by racing them 30 times.

**What if PostgreSQL goes down?**
Database errors are not workflow failures: nothing is recorded as FAILED, the worker abandons the run and keeps
polling, and another worker replays after the lease lapses. A lost commit acknowledgement is safe because replay
finds the event or finds the step in doubt.

**How would you scale it?**
Add workers (shown: 1 to 2 workers ~2.3x on the benchmark machine). The ceiling is one PostgreSQL: every step is
two commits. Next steps: partition the event table, snapshot long histories ("continue as new"), read replicas for
the dashboard, and a push-based wakeup (LISTEN/NOTIFY) instead of polling.

**What are the weaknesses?**
Timers are accurate only to the poll interval; long histories replay in full; workflow code must be deterministic
and I can only detect, not prevent, violations; dashboard is read-mostly with no auth; no multi-tenant isolation.

**How is this different from Temporal?**
Same core ideas (event history, replay, workers), but a few thousand lines on plain PostgreSQL, without
Temporal's sharding, task queues, versioning tools or SDK languages. I built it to understand the mechanisms.

## Whiteboard drill (draw this)

Crash timeline for one effect step: `STEP_STARTED` -> call provider -> **crash** -> `STEP_COMPLETED`. Then show the
three recoveries: idempotent (retry with key), verify hook (ask provider), none (NEEDS_REVIEW).

## If they ask "what would you do differently?"

Property-test earlier (it found the worst bug late), model the lease/claim protocol in TLA+ or at least a state
machine table first, and ship metrics (Prometheus) alongside the dashboard.

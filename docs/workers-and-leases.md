# Workers, leases and fencing (Phase 4)

Before this phase, one process executed a run, and `resume_run` was safe only if a person knew the old
process was dead. Now several worker processes share the work, a crashed worker's runs are picked up
automatically, and a worker that was merely *slow or frozen* cannot corrupt a run it no longer owns.

## The three ideas

**1. Claiming.** A worker takes a run with one atomic SQL statement (`RunRepository.claim_next`). It picks
the oldest run that is either `PENDING` or `RUNNING` with an expired lease, using
`SELECT ... FOR UPDATE SKIP LOCKED`, and in the same statement sets `lease_owner`, pushes
`lease_expires_at` into the future and adds 1 to `fencing_token`. `SKIP LOCKED` means a worker that finds a
row locked by someone else moves on to the next row instead of waiting or taking it, so two workers can
never be handed the same run.

**2. Leases.** A lease is permission to execute a run until a deadline. The worker renews it every
`heartbeat_seconds` (default 5) while it works; the lease lasts `lease_seconds` (default 15). If the worker
dies, stops renewing and the deadline passes, any other worker may claim the run. All deadlines are
computed with the database's `now()`, so workers with skewed clocks cannot disagree about expiry.

**3. Fencing tokens.** A lease alone is not enough. A worker can be paused (a long garbage collection, a
VM freeze, a network partition) for longer than its lease, wake up believing it still owns the run, and
write. The fencing token closes this hole: every claim increments the run's token, and **every write a
worker makes is checked against the token it was given**, inside the same transaction and under the run's
row lock. A write carrying an old token raises `StaleLeaseError` and commits nothing.

```
t0   worker A claims run R           token 1, lease until t0+15
t1   A runs step "charge", then freezes (heartbeat frozen too)
t16  lease expired; worker B claims R   token 2
t17  B replays R's history, finishes the run
t30  A thaws, tries to record step_completed with token 1
     -> the check "run is at token 2, you hold 1" fails; nothing is written; A stands down
```

The database orders the two workers, not their clocks or their opinions of each other: the fence check
takes `SELECT ... FOR UPDATE` on the run row, which is also what a claim contends for. Either the takeover
committed first (and A's check fails) or A's transaction commits first (and the takeover waits, then
supersedes it). There is no interleaving in which A's write lands after B owns the run.

## What a worker does with a run

1. `claim_next` (above). Only workflows registered in this worker's code, at the same version, are claimable.
2. Under the fence, write `WORKFLOW_STARTED` (no history yet) or `WORKFLOW_RESUMED` (history exists).
   Both carry `worker_id` and `fencing_token`, so the event log shows who ran what.
3. Execute the workflow function with the replay machinery from Phase 2, using a recorder that checks the
   fence on every write. Before each step function runs it also re-checks the lease (`ensure_lease`).
4. A heartbeat task renews the lease. If a renewal is refused, or the database cannot be reached for a
   whole lease period, the worker **cancels its own execution** and stands down.
5. On completion, failure or `NEEDS_REVIEW` the lease is released in the same transaction that records the
   outcome.

If a workflow swallows the `StaleLeaseError` (a careless `except Exception`), the context is halted: every
later `ctx` call re-raises it, and every write would be rejected anyway. A test covers this.

## What fencing protects, and what it does not

| Protected | Not protected |
|---|---|
| Event-log writes, status changes and idempotency records by a worker that lost its lease | Calls a stale worker makes to the outside world. A zombie that already started a payment call cannot be recalled |
| A zombie recording a result, starting another step, or completing the run | A stale worker whose lease lapses *between* `ensure_lease` and the step function actually calling the tool |
| Two workers executing one run at the same time, as far as the history is concerned | Workflow code that does real work without `ctx.step`/`ctx.effect` |

The second column is why Phase 3 and Phase 4 are two halves of one design. Fencing keeps the *history*
correct. Idempotency keys keep the *outside world* correct: if a zombie does repeat a call, the payment
service sees the same key and returns the original result. The tests exercise both together (the zombie
tests check the payment ledger, not just the event log).

## Setup details that matter

- **Directly executed runs are never claimed.** `execute_run()` / `resume_run()` (used by the original
  runner CLI and the inline API mode) leave `lease_expires_at` NULL, and workers only claim runs with a
  non-NULL expired lease. `resume_run` refuses a run that carries a worker lease, because it would race the
  lease holder.
- **`EXECUTION_MODE`.** `inline` (default): the API process executes runs itself, as in earlier phases.
  `workers`: the API only records runs; start workers with `python -m app.engine.worker`. In workers mode,
  resolving a `NEEDS_REVIEW` step reopens the run as claimable, and a worker continues it.
- **Compose.** `docker compose up -d --build --scale worker=3` starts three workers next to the API.
- **Choosing timings.** The heartbeat must be clearly shorter than the lease (the worker refuses to start
  otherwise); one third is a common ratio. A longer lease means slower recovery from a real crash; a shorter
  one means more false takeovers when a worker is merely slow. False takeovers are *safe* (that is what
  fencing is for) but waste work.
- **Observability.** `GET /workers` lists registered workers. `GET /runs/{id}` shows `lease_owner`,
  `lease_expires_at` and `fencing_token`. The workers table is informational only: correctness never depends
  on it.

## Failure cases and what happens

| Situation | Outcome |
|---|---|
| Worker killed mid-run | Lease expires; another worker resumes from the history (tested at 5 kill points with real processes) |
| Worker frozen past its lease, then thaws | Its next write is rejected; it stands down (tested with real frozen processes at 3 points) |
| Worker alive but its lease taken (false failure detection) | Heartbeat notices and cancels; any write already in flight is rejected (chaos test) |
| Worker claims a run and is taken over before writing anything | The next worker writes `WORKFLOW_STARTED`, not `RESUMED` (found by the chaos test) |
| Database unreachable for a whole lease period | The worker cancels its own execution rather than act on a run another worker may now hold |
| Two workers poll at the same instant | `SKIP LOCKED`: each gets a different run, or one gets none |
| Run waiting in `NEEDS_REVIEW` | Never claimed; the lease was released when it was parked |

## Limitations (honest list)

- Fencing cannot recall an external call already in flight; idempotency keys (or a verify hook, or a person)
  decide whether a repeated call is harmless.
- One run at a time per worker process. Scale by starting more processes.
- No worker priorities, queues or rate limits.
- A directly executed (lease-less) run that crashes is not recovered by workers; resume it by hand.
- Cancelling a step that is mid-query drops that database connection; the transaction rolls back, but the
  logs show a connection error. This is noise, not data loss.
- Timers and signals (a run waiting days for an approval) are Phase 5.

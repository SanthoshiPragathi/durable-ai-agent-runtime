# Chaos campaign

`scripts/chaos_campaign.py` runs the refund workflow many times against **real worker processes** (`python -m app.engine.worker`) on a real PostgreSQL 16 while it injects failures, then audits the result.

## What is injected

- **Random kills:** a running worker is SIGKILLed at an arbitrary moment.
- **Failpoint deaths ("doomed" workers):** a worker exits (code 77) at an exact point, drawn from the list of failpoints (`after_lease_claimed`, `during_recovery`, `before_heartbeat`, and the recorder/effect failpoints).
- **Zombies:** a worker is frozen (`DURABLE_STALL`) past its lease, then resumes and tries to keep writing as a stale holder. Fencing tokens must reject it.
- **PostgreSQL restarts** (optional, `--pg-restart-cmd/--pg-restart-every`).

## What is verified afterwards

Every run COMPLETED; the event log of each run has no gaps, exactly one `WORKFLOW_COMPLETED`, and each of the 5 steps ran once; fencing tokens only increase; the payment provider's ledger has exactly one refund per run and the email ledger one email per run; every refund idempotency key is distinct.

## Result (seed 2026, one run on a 2-core Linux sandbox)

```
python scripts/chaos_campaign.py --runs 10000 --workers 4 --kill-every 4 --stall-seconds 8 \
  --seed 2026 --pg-restart-cmd "$PG_RESTART" --pg-restart-every 60 --timeout 3300
```

| | |
|---|---|
| result | PASS |
| runs completed / created | 10000 / 10000 |
| wall time | 693 s (14.4 runs/s) |
| worker processes started | 209 (93 normal, 50 doomed, 66 zombie) |
| random process kills | 90 |
| deaths at an exact failpoint | 49 |
| zombies frozen past their lease | 66 |
| PostgreSQL restarts | 11 |
| lease takeovers | 371 |
| stale workers fenced out | 132 |
| refunds / emails in provider ledgers | 10000 / 10000 (expected 10000 each) |
| distinct refund idempotency keys | 10000 |
| problems found | 0 |

This is one seeded campaign, not a proof. It shows no violation of the invariants above across ~370 takeovers; it does not cover multi-host clocks, network partitions between worker and database that leave a connection half-open, or disk faults. The guarantee remains **at-least-once execution with idempotent effects**, not exactly-once.

## A real bug it found

An early 200-run validation exposed a race: when several workers created the first run of a workflow at the same moment, `ensure_workflow` did read-then-insert and one of them failed with a duplicate key on `workflows_pkey`. Fixed with `INSERT ... ON CONFLICT DO NOTHING`; regression test in `tests/integration/test_repositories.py` (fails without the fix).

## Run it yourself

```
python scripts/chaos_campaign.py --runs 500 --workers 4 --seed 1 --report chaos.md
```

It truncates the development database and the demo services' ledgers; do not point it at data you care about.

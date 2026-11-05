# Benchmarks (Phase 7)

Produced by `python scripts/benchmark.py`. **These are single measurements on a small machine and mean
little in absolute terms; use them to see how the design scales and where time goes.**

Environment of the recorded run: Linux container, 2 CPU cores, PostgreSQL 16 on the same machine (no network
latency), Python 3.13. Re-run on your own setup; the Windows/Docker numbers will differ.

| Throughput (200 runs x 5 trivial steps) | runs/s | events/s |
|---|---|---|
| 1 worker process | 9.6 | 115 |
| 2 worker processes | 22.2 | 267 |
| 4 worker processes | 19.1 | 229 |

Two workers scale better than linearly here because one worker's idle polling gap overlaps the other's work.
Four workers do no better than two because the machine has two cores and every step is a database round trip;
the limit shown is the machine, not the claim mechanism (claiming takes 2.6 ms at p50, 4.5 ms at p99 with 1000
pending runs).

| History length | ms per live step | ms to resume (replay N-1 steps + 1 live step) |
|---|---|---|
| 10 | 23.9 | 42 |
| 100 | 19.2 | 44 |
| 500 | 16.4 | 70 |

Replay is cheap compared with live execution: 500 recorded steps replay in roughly 70 ms, because replay reads
the events in one query and does no writes, while each live step costs two committed transactions. The cost of
replay still grows with history length, and a very long run replays all of it on every resume. A snapshot or
"continue-as-new" mechanism would bound that; it is not implemented.

| Recovery after a worker dies mid-run | seconds |
|---|---|
| crash to run COMPLETED by a new worker (lease 1.5 s) | 1.5 |

Recovery time is dominated by the lease length: a shorter lease recovers faster and risks more false takeovers
(which fencing and idempotency keys make harmless, see `test_lease_chaos.py`).

## What is not measured

Real network latency to a remote database, many thousands of concurrent runs, large payloads, or LLM latency.
Each live step is two commits, so throughput per worker is bounded by database commit latency.

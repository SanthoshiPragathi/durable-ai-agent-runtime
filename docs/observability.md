# Observability

What you can see about a running system, how to scrape it, and what is deliberately missing.

## `/metrics` (Prometheus)

`GET /metrics` returns the Prometheus text format. Every number is computed from PostgreSQL at scrape time,
so it is the same whichever API instance answers and it survives restarts. If API authentication is on, a
**read-only** key is enough (`Authorization: Bearer <key>` or `X-API-Key`).

| Series | Type | Meaning |
|---|---|---|
| `durable_runs{status}` | gauge | runs per status (PENDING, RUNNING, WAITING, NEEDS_REVIEW, COMPLETED, FAILED) |
| `durable_queue_depth` | gauge | runs a worker could claim right now: PENDING, or RUNNING with a lapsed lease |
| `durable_oldest_pending_age_seconds` | gauge | how long the oldest PENDING run has waited |
| `durable_workers{state}` | gauge | `alive` (heartbeat within one lease period), `silent` (ACTIVE but quiet, probably dead), `stopped` |
| `durable_events_total{event_type}` | counter | event-log rows by type (the log is append-only, so this only grows) |
| `durable_workflow_resumes_total` | counter | `WORKFLOW_RESUMED` events. **Not only takeovers:** a run woken after a timer or signal is also a resume. |
| `durable_step_duration_seconds{quantile}` | gauge | STEP_STARTED to STEP_COMPLETED, p50/p90/p99, last 15 minutes, waits excluded |
| `durable_llm_calls_total`, `durable_llm_cost_usd_total`, `durable_llm_tokens_total{direction}` | counters | read from the recorded `ctx.llm` results (cost counts only calls with a known price) |
| `durable_external_operations{status}`, `durable_external_operation_attempts_total` | gauge / counter | the idempotency ledger: attempts above the number of operations are retries |
| `durable_timers{status}` | gauge | durable timers (PENDING, FIRED, CANCELLED) |

### Try it

```
curl http://localhost:8000/metrics
```

### Prometheus and Grafana (optional)

```
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d
```

Prometheus is on <http://localhost:9090>, Grafana on <http://localhost:3000> (admin / `GRAFANA_PASSWORD`, default
`admin`; change it). In Grafana add a Prometheus data source with URL `http://prometheus:9090`, then
*Dashboards > Import* and upload `monitoring/grafana-dashboard.json` (11 panels: runs by status, queue depth, oldest
pending age, workers, step p50/p99, recoveries per minute, LLM calls and cost, external-operation attempts, failed and
needs-review runs, pending timers). Both ports are published on `127.0.0.1` only.

**Honest status:** the `/metrics` endpoint is covered by automated tests that parse its output with the reference
Prometheus parser. `docker-compose.monitoring.yml`, `monitoring/prometheus.yml` and the dashboard JSON were
written and syntax-checked but **not run against a live Prometheus/Grafana** in the environment this was built in. If a
panel is empty, check the data source name first.

## What is not here, and why

- **Distributed tracing (OpenTelemetry).** There is no tracing. A step already leaves a durable, ordered record in
  the event log (who ran it, when it started and finished, the result), which answers most "what happened to this run?"
  questions without a tracing backend. If you need cross-service traces, the place to add spans is
  `WorkflowContext._execute_step` (one span per step, `run_id` and `step_name` as attributes) and `Worker.process` (one
  span per claim), with the run id as the trace correlation key.
- **Counters kept in memory.** Deliberately avoided: they reset on restart and differ between API instances.
  The cost is that each scrape runs a few aggregate queries over the event log. The first thing to replace at scale
  is the `events_total` count (a full scan) with a materialized view or a counter table.
- **Per-run latency histograms** (as opposed to the 15-minute quantiles above) and alert rules are not provided.
  Reasonable alerts to write yourself: `durable_queue_depth` high for several minutes, `durable_workers{state="alive"} == 0`,
  `durable_runs{status="NEEDS_REVIEW"} > 0`, `durable_oldest_pending_age_seconds` above your tolerance.

## Logs

Workers and the API use the standard library `logging` (level from `LOG_LEVEL`). `structlog` is listed in
`requirements.txt` but is not used; it can be removed.

"""Prometheus metrics, computed from the database on every scrape.

Nothing is kept in process memory, so the numbers are the same whichever API instance answers and
they survive restarts. The trade-off: each scrape runs a few aggregate queries, which is fine for a
scrape every 15-60 s and is the first thing to replace (with counters or a materialized view) at scale.

Series (all prefixed `durable_`):
  runs{status}                 runs per status (gauge)
  queue_depth                  runs a worker could claim right now: PENDING, or RUNNING with a lapsed lease
  oldest_pending_age_seconds   how long the oldest PENDING run has waited
  workers{state}               alive (heartbeat within one lease period), silent (ACTIVE but quiet), stopped
  events_total{event_type}     event-log rows by type (a counter: the log is append-only)
  workflow_resumes_total       WORKFLOW_RESUMED events: a worker recovered a run (a takeover after a crash
                               or lapsed lease) OR woke it after a timer/signal. Not takeovers alone.
  step_duration_seconds{quantile}  STEP_STARTED -> STEP_COMPLETED over the last 15 minutes, waits excluded
  llm_calls_total, llm_cost_usd_total, llm_tokens_total{direction}   recorded by ctx.llm
  external_operations{status}, external_operation_attempts_total     the idempotency ledger
  timers{status}               durable timers
"""

from fastapi import APIRouter, Depends, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session_factory
from app.config import get_settings
from app.database.session import SessionFactory

router = APIRouter(tags=["meta"])

STEP_WINDOW_MINUTES = 15

_STEP_DURATIONS = text(
    f"""
    WITH done AS (
        SELECT run_id, step_name, sequence_number AS cseq, created_at AS cat
        FROM workflow_events
        WHERE event_type = 'STEP_COMPLETED'
          AND created_at > now() - interval '{STEP_WINDOW_MINUTES} minutes'
    ), durations AS (
        SELECT EXTRACT(EPOCH FROM (d.cat - (
            SELECT s.created_at FROM workflow_events s
            WHERE s.run_id = d.run_id AND s.step_name = d.step_name
              AND s.event_type = 'STEP_STARTED' AND s.sequence_number < d.cseq
            ORDER BY s.sequence_number DESC LIMIT 1))) AS secs
        FROM done d
        WHERE NOT EXISTS (  -- a wait (sleep / signal) parks for as long as it takes; not a step's speed
            SELECT 1 FROM workflow_events w
            WHERE w.run_id = d.run_id AND w.step_name = d.step_name AND w.event_type = 'WORKFLOW_WAITING')
    )
    SELECT count(secs) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY secs) AS p50,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY secs) AS p90,
           percentile_cont(0.99) WITHIN GROUP (ORDER BY secs) AS p99
    FROM durations WHERE secs IS NOT NULL
    """
)

_LLM = text(
    """
    SELECT count(*) AS calls,
           COALESCE(sum((result_data->>'cost_usd')::float8), 0) AS cost,
           COALESCE(sum((result_data->>'input_tokens')::bigint), 0) AS tokens_in,
           COALESCE(sum((result_data->>'output_tokens')::bigint), 0) AS tokens_out
    FROM workflow_events
    WHERE event_type = 'STEP_COMPLETED' AND result_data->>'prompt_hash' IS NOT NULL
    """
)


def _family(lines: list[str], name: str, kind: str, help_text: str) -> None:
    lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} {kind}")


def _num(value: object) -> str:
    return repr(float(value)) if value is not None else "NaN"  # type: ignore[arg-type]


async def render_metrics(session: AsyncSession) -> str:
    out: list[str] = []

    async def rows(sql: str, **params: object) -> list:
        return list((await session.execute(text(sql), params)).all())

    _family(out, "durable_runs", "gauge", "Runs by status.")
    by_status = {
        status: n for status, n in await rows("SELECT status, count(*) FROM workflow_runs GROUP BY 1")
    }
    for status in ("PENDING", "RUNNING", "WAITING", "NEEDS_REVIEW", "COMPLETED", "FAILED"):
        out.append(f'durable_runs{{status="{status}"}} {by_status.pop(status, 0)}')
    for status, n in sorted(by_status.items()):
        out.append(f'durable_runs{{status="{status}"}} {n}')

    (depth,) = (
        await rows(
            "SELECT count(*) FROM workflow_runs WHERE status = 'PENDING' "
            "OR (status = 'RUNNING' AND lease_expires_at IS NOT NULL AND lease_expires_at < now())"
        )
    )[0]
    _family(out, "durable_queue_depth", "gauge", "Runs a worker could claim right now.")
    out.append(f"durable_queue_depth {depth}")

    (age,) = (
        await rows(
            "SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0) "
            "FROM workflow_runs WHERE status = 'PENDING'"
        )
    )[0]
    _family(out, "durable_oldest_pending_age_seconds", "gauge", "Age of the oldest PENDING run.")
    out.append(f"durable_oldest_pending_age_seconds {_num(age)}")

    window = get_settings().lease_duration_seconds
    states = {"alive": 0, "silent": 0, "stopped": 0}
    for status, fresh, n in await rows(
        "SELECT status, last_heartbeat_at > now() - make_interval(secs => :w), count(*) "
        "FROM workers GROUP BY 1, 2",
        w=window,
    ):
        states["stopped" if status == "STOPPED" else "alive" if fresh else "silent"] += n
    _family(
        out, "durable_workers", "gauge", "Workers by liveness (alive = heartbeat within one lease period)."
    )
    for state, n in states.items():
        out.append(f'durable_workers{{state="{state}"}} {n}')

    events = await rows("SELECT event_type, count(*) FROM workflow_events GROUP BY 1 ORDER BY 1")
    _family(out, "durable_events_total", "counter", "Event-log rows by type (append-only).")
    for event_type, n in events:
        out.append(f'durable_events_total{{event_type="{event_type}"}} {n}')
    _family(
        out,
        "durable_workflow_resumes_total",
        "counter",
        "WORKFLOW_RESUMED events: crash/lease recoveries plus wake-ups after a timer or signal.",
    )
    out.append(f"durable_workflow_resumes_total {dict(events).get('WORKFLOW_RESUMED', 0)}")

    n, p50, p90, p99 = (await session.execute(_STEP_DURATIONS)).one()
    _family(
        out,
        "durable_step_duration_seconds",
        "gauge",
        f"STEP_STARTED to STEP_COMPLETED, last {STEP_WINDOW_MINUTES} min, waits excluded.",
    )
    for q, v in (("0.5", p50), ("0.9", p90), ("0.99", p99)):
        out.append(f'durable_step_duration_seconds{{quantile="{q}"}} {_num(v)}')
    _family(out, "durable_step_duration_samples", "gauge", "Steps in the window behind the quantiles.")
    out.append(f"durable_step_duration_samples {n}")

    calls, cost, tokens_in, tokens_out = (await session.execute(_LLM)).one()
    _family(out, "durable_llm_calls_total", "counter", "Recorded ctx.llm calls.")
    out.append(f"durable_llm_calls_total {calls}")
    _family(
        out, "durable_llm_cost_usd_total", "counter", "Recorded LLM cost (calls with unknown cost add 0)."
    )
    out.append(f"durable_llm_cost_usd_total {_num(cost)}")
    _family(out, "durable_llm_tokens_total", "counter", "Recorded LLM tokens.")
    out.append(f'durable_llm_tokens_total{{direction="input"}} {tokens_in}')
    out.append(f'durable_llm_tokens_total{{direction="output"}} {tokens_out}')

    ops = await rows(
        "SELECT status, count(*), COALESCE(sum(attempts), 0) FROM idempotency_records GROUP BY 1"
    )
    _family(out, "durable_external_operations", "gauge", "External operations by status.")
    for status, n_ops, _ in sorted(ops):
        out.append(f'durable_external_operations{{status="{status}"}} {n_ops}')
    _family(
        out,
        "durable_external_operation_attempts_total",
        "counter",
        "Attempts across all external operations.",
    )
    out.append(f"durable_external_operation_attempts_total {sum(a for _, _, a in ops)}")

    _family(out, "durable_timers", "gauge", "Durable timers by status.")
    for status, n_timers in await rows("SELECT status, count(*) FROM workflow_timers GROUP BY 1 ORDER BY 1"):
        out.append(f'durable_timers{{status="{status}"}} {n_timers}')

    return "\n".join(out) + "\n"


@router.get("/metrics", summary="Prometheus metrics", include_in_schema=True)
async def metrics(session_factory: SessionFactory = Depends(get_session_factory)) -> Response:
    async with session_factory() as session:
        body = await render_metrics(session)
    return Response(body, media_type="text/plain; version=0.0.4; charset=utf-8")

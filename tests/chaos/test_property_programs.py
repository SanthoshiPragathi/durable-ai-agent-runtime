"""Property test: random workflow PROGRAMS (plain steps, ctx.now, ctx.random, ctx.llm, idempotent effects)
are crashed at random protocol points; the finished run must always agree with its own event log.

Hypothesis generates the program (1-7 operations in any order) and a crash plan: each entry kills the
"worker" once at one of the recorder's failpoints (before/after STEP_STARTED, before/after
STEP_COMPLETED) on any step, or at the workflow start/end. The in-process crash is a BaseException
(SimulatedCrash), the stand-in for SIGKILL that the rest of the chaos suite uses; the real-process kills
are in tests/recovery.

Invariants, judged by the event log and an independent effect ledger, not by the runtime's own opinion:
  * the run completes (nothing is lost) and never fails with a replay divergence;
  * every step has exactly one STEP_COMPLETED, in program order, with gapless sequence numbers;
  * every value the workflow RETURNED equals the value recorded in the log. now(), random() and the LLM
    are non-deterministic, so a value drawn by a crashed attempt and never recorded must be replaced, never
    mixed with the recorded one;
  * an idempotent effect happened exactly once per step in the ledger, however many times it was retried.
"""

import asyncio
import itertools
import os
from typing import Any
from unittest import mock

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.config import to_sync_url
from app.database.repositories import EventRepository, RunRepository
from app.database.session import create_session_factory
from app.engine import runtime
from app.integrations.llm import MockLLMProvider
from app.sdk import workflow
from app.sdk.workflow import registry
from tests.fixtures.crash import SimulatedCrash

pytestmark = [pytest.mark.chaos, pytest.mark.integration]

KINDS = ["step", "now", "random", "llm", "effect"]
STEP_POINTS = ["before_step_started", "after_step_started", "before_step_completed", "after_step_completed"]
WORKFLOW_POINTS = ["after_workflow_started", "before_workflow_completed"]

program_st = st.lists(st.sampled_from(KINDS), min_size=1, max_size=7)
crashes_st = st.lists(
    st.one_of(
        st.tuples(st.integers(0, 6), st.sampled_from(STEP_POINTS)),
        st.tuples(st.just(-1), st.sampled_from(WORKFLOW_POINTS)),
    ),
    max_size=6,
)


def reset(db_url: str) -> None:
    engine = create_engine(to_sync_url(db_url), isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(
            text(
                "TRUNCATE workflow_events, idempotency_records, workflow_runs, workflows "
                "RESTART IDENTITY CASCADE"
            )
        )
    engine.dispose()


def step_names(program: list[str]) -> list[str]:
    """The name each operation is recorded under (now/random are named automatically, in call order)."""
    seen = {"now": 0, "random": 0}
    names = []
    for i, kind in enumerate(program):
        if kind in seen:
            seen[kind] += 1
            names.append(f"__{kind}_{seen[kind]}")
        else:
            names.append(f"s{i}")
    return names


async def scenario(program: list[str], crashes: list[tuple[int, str]], db_url: str) -> dict[str, Any]:
    engine = create_async_engine(db_url, poolclass=NullPool)
    sf = create_session_factory(engine)
    saved = registry.snapshot()
    registry.clear()
    names = step_names(program)
    pending: set[tuple[str, str | None]] = set()
    for index, point in crashes:
        if index == -1:
            pending.add((point, None))
        elif index < len(program):
            pending.add((point, names[index]))

    def failpoint(point: str, key: str | None = None) -> None:
        if (point, key) in pending:
            pending.discard((point, key))
            raise SimulatedCrash(f"{point} {key}")

    counter = itertools.count(1)  # makes every real execution of a plain step produce a new value
    llm_answers = itertools.count(1)
    ledger: dict[str, int] = {}  # idempotency key -> effect number: the independent judge
    effect_calls: list[str] = []

    def flaky_read(i: int) -> dict[str, int]:
        return {"op": i, "execution": next(counter)}

    async def ledger_effect(i: int, idempotency_key: str) -> dict[str, int]:
        effect_calls.append(idempotency_key)
        if idempotency_key not in ledger:
            ledger[idempotency_key] = len(ledger) + 1
        return {"op": i, "effect_no": ledger[idempotency_key]}

    llm = MockLLMProvider(lambda prompt, model: f"answer-{next(llm_answers)}")

    @workflow(name="prop_programs")
    async def prop_programs(ctx):
        out: list[Any] = []
        for i, kind in enumerate(program):
            if kind == "step":
                out.append(await ctx.step(f"s{i}", flaky_read, i))
            elif kind == "now":
                out.append((await ctx.now()).isoformat())
            elif kind == "random":
                out.append(await ctx.random())
            elif kind == "llm":
                out.append(await ctx.llm(f"s{i}", f"question {i}"))
            else:
                out.append(await ctx.effect(f"s{i}", ledger_effect, i, idempotent=True))
        return out

    try:
        with (
            mock.patch("app.engine.execution_context.failpoint", failpoint),
            mock.patch("app.engine.runtime.failpoint", failpoint),
        ):
            run_id = (await runtime.create_run(sf, "prop_programs", {})).run_id
            status, attempts, limit = "PENDING", 0, len(crashes) + 4
            while status not in ("COMPLETED", "FAILED") and attempts < limit:
                try:
                    if attempts == 0:
                        run = await runtime.execute_run(sf, run_id, llm_provider=llm)
                    else:
                        run = await runtime.resume_run(sf, run_id, llm_provider=llm)
                    status = run.status
                except SimulatedCrash:
                    status = "RUNNING"
                attempts += 1
        async with sf() as session:
            run = await RunRepository(session).get_run(run_id)
            events = await EventRepository(session).list_events(run_id)
        return {
            "run": run,
            "events": events,
            "names": names,
            "ledger": ledger,
            "effect_calls": effect_calls,
            "attempts": attempts,
            "limit": limit,
        }
    finally:
        registry.restore(saved)
        await engine.dispose()


@settings(
    max_examples=int(os.environ.get("CHAOS_EXAMPLES", "60")),
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(program=program_st, crashes=crashes_st)
def test_a_crashed_program_always_agrees_with_its_own_log(
    migrated_database: str, program: list[str], crashes: list[tuple[int, str]]
) -> None:
    reset(migrated_database)
    result = asyncio.run(scenario(program, crashes, migrated_database))
    run, events, names = result["run"], result["events"], result["names"]

    assert run.status == "COMPLETED", f"ended {run.status}: {run.error_message}"
    assert result["attempts"] < result["limit"]

    # Every step completed exactly once, in program order; sequence numbers have no gaps.
    completed = [e for e in events if e.event_type == "STEP_COMPLETED"]
    assert [e.step_name for e in completed] == names
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1))
    assert [e.event_type for e in events].count("WORKFLOW_COMPLETED") == 1

    # What the workflow returned is exactly what the log recorded, never a value from a lost attempt.
    returned = run.result_data
    assert len(returned) == len(program)
    for kind, got, event in zip(program, returned, completed, strict=True):
        recorded = event.result_data
        assert got == (recorded["output"] if kind == "llm" else recorded), (kind, got, recorded)

    # An idempotent effect happened once per step in the ledger, however often it was retried.
    effect_ops = [i for i, kind in enumerate(program) if kind == "effect"]
    assert sorted(result["ledger"].values()) == list(range(1, len(effect_ops) + 1))
    assert all(f"{run.run_id}:s{i}" in result["ledger"] for i in effect_ops)
    assert set(result["effect_calls"]) <= set(result["ledger"])


def test_the_program_property_has_teeth_an_unrecorded_value_would_be_caught() -> None:
    """Negative control: with a crash after a plain step ran but before it was recorded, the step
    really does execute twice (its second value is the one kept). The property compares against the log,
    so mixing the lost first value into the result would fail it."""
    assert step_names(["now", "step", "random", "now"]) == ["__now_1", "s1", "__random_1", "__now_2"]

"""Crash, timeout and resume with REAL fake services. Every assertion about "what happened in the
world" comes from the services' own ledgers (a separate database), never from the runtime's log."""

import uuid
from typing import Any

import pytest

from app.database.repositories import EventRepository, IdempotencyRepository, RunRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.errors import (
    InvalidResolutionError,
    InvalidRunStateError,
    StepSerializationError,
)
from app.integrations.tools import EmailService, IdempotentPaymentService, NonIdempotentPaymentService
from app.integrations.tools.fake_ledger import (
    AMBIGUOUS_AFTER_COMMIT,
    AMBIGUOUS_BEFORE_COMMIT,
    DECLINE,
    FAIL,
)
from app.sdk import workflow
from tests.fixtures.crash import CrashPlan, SimulatedCrash

pytestmark = [
    pytest.mark.integration,
    pytest.mark.recovery,
    pytest.mark.usefixtures("isolated_registry"),
]


# ---- helpers ------------------------------------------------------------------------------


async def run_state(sf: SessionFactory, run_id: uuid.UUID):
    async with sf() as session:
        run = await RunRepository(session).get_run(run_id)
        events = await EventRepository(session).list_events(run_id)
        ops = {o.step_name: o for o in await IdempotencyRepository(session).list_for_run(run_id)}
    return run, events, ops


def event_types(events: list) -> list[str]:
    return [e.event_type for e in events]


async def start(sf: SessionFactory, name: str, **input_data: Any) -> uuid.UUID:
    return (await runtime.create_run(sf, name, input_data)).run_id


def register_idempotent_refund(
    payments: IdempotentPaymentService, emails: EmailService, crash: CrashPlan, name: str = "refund"
) -> None:
    @workflow(name=name)
    async def refund(ctx, order_id: str):
        r = await ctx.effect(
            "process_refund",
            crash.wrap("process_refund", payments.refund),
            order_id,
            2500,
            idempotent=True,
        )
        await ctx.effect(
            "send_email",
            crash.wrap("send_email", emails.send),
            "a@example.com",
            "Refunded",
            f"refund {r['refund_id']}",
            idempotent=True,
        )
        return {"refund_id": r["refund_id"]}


def register_legacy_refund(
    payments: NonIdempotentPaymentService, crash: CrashPlan, *, with_verify: bool, name: str = "legacy"
) -> list[str]:
    """A refund through a provider that cannot dedupe; the email step records that it ran."""
    emailed: list[str] = []

    @workflow(name=name)
    async def legacy(ctx, order_id: str):
        reference = ctx.idempotency_key("process_refund")

        async def verify(order_id: str, amount: int, reference: str):
            return await payments.find_by_reference(reference)

        r = await ctx.effect(
            "process_refund",
            crash.wrap("process_refund", payments.refund),
            order_id,
            2500,
            reference,
            idempotent=False,
            verify=verify if with_verify else None,
        )
        await ctx.step("send_email", lambda: emailed.append(r["refund_id"]) or "sent")
        return {"refund_id": r["refund_id"]}

    return emailed


@pytest.fixture
def payments(services_session_factory: SessionFactory) -> IdempotentPaymentService:
    return IdempotentPaymentService(services_session_factory)


@pytest.fixture
def legacy_payments(services_session_factory: SessionFactory) -> NonIdempotentPaymentService:
    return NonIdempotentPaymentService(services_session_factory)


@pytest.fixture
def mail(services_session_factory: SessionFactory) -> EmailService:
    return EmailService(services_session_factory)


# ---- THE scenario: duplicate refund prevention ----------------------------------------------


async def test_crash_after_the_refund_succeeds_still_leaves_exactly_one_refund(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    """Demo 3, in-process. Worker calls the payment service, the refund succeeds, the worker dies
    before STEP_COMPLETED. The new worker retries with the same key; the ledger keeps one refund."""
    crash = CrashPlan()
    crash.after.add("process_refund")
    register_idempotent_refund(payments, mail, crash)
    run_id = await start(session_factory, "refund", order_id="ORD-1")

    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)

    # The dangerous moment: the money moved, the runtime has no record of it.
    assert await payments.count_refunds() == 1
    run, events, ops = await run_state(session_factory, run_id)
    assert run.status == "RUNNING" and events[-1].event_type == "STEP_STARTED"
    assert ops["process_refund"].status == "IN_FLIGHT"

    final = await runtime.resume_run(session_factory, run_id)

    assert final.status == "COMPLETED"
    ledger = await payments.list_refunds()
    assert len(ledger) == 1  # NOT two
    assert ledger[0]["idempotency_key"] == f"{run_id}:process_refund"
    assert final.result_data == {"refund_id": ledger[0]["refund_id"]}  # the ORIGINAL refund came back
    assert len(await mail.list_emails()) == 1

    _, events, ops = await run_state(session_factory, run_id)
    assert ops["process_refund"].status == "SUCCEEDED" and ops["process_refund"].attempts == 2
    assert ops["process_refund"].result_data["refund_id"] == ledger[0]["refund_id"]
    assert event_types(events).count("WORKFLOW_COMPLETED") == 1


async def test_crash_before_the_refund_is_attempted_also_leaves_exactly_one(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    crash = CrashPlan()
    crash.before.add("process_refund")
    register_idempotent_refund(payments, mail, crash)
    run_id = await start(session_factory, "refund", order_id="ORD-1")

    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)
    assert await payments.count_refunds() == 0

    assert (await runtime.resume_run(session_factory, run_id)).status == "COMPLETED"
    assert await payments.count_refunds() == 1


async def test_crash_after_the_email_was_sent_does_not_send_it_twice(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    crash = CrashPlan()
    crash.after.add("send_email")
    register_idempotent_refund(payments, mail, crash)
    run_id = await start(session_factory, "refund", order_id="ORD-1")

    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)
    assert (len(await mail.list_emails()), await payments.count_refunds()) == (1, 1)

    assert (await runtime.resume_run(session_factory, run_id)).status == "COMPLETED"
    assert (len(await mail.list_emails()), await payments.count_refunds()) == (1, 1)


async def test_crashing_over_and_over_never_duplicates(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    crash = CrashPlan()
    crash.after.update({"process_refund", "send_email"})
    crash.before.add("send_email")
    register_idempotent_refund(payments, mail, crash)
    run_id = await start(session_factory, "refund", order_id="ORD-1")

    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)
    for _ in range(2):
        with pytest.raises(SimulatedCrash):
            await runtime.resume_run(session_factory, run_id)
    final = await runtime.resume_run(session_factory, run_id)

    assert final.status == "COMPLETED"
    assert (await payments.count_refunds(), len(await mail.list_emails())) == (1, 1)


# ---- ambiguous timeouts and failures during a normal run --------------------------------------


async def test_a_lost_reply_is_retried_with_the_same_key_and_refunds_once(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    """The refund committed but the caller only saw a timeout; the retry returns the original."""
    register_idempotent_refund(payments, mail, CrashPlan())
    payments.faults.inject(AMBIGUOUS_AFTER_COMMIT)
    run_id = await start(session_factory, "refund", order_id="ORD-1")

    final = await runtime.execute_run(session_factory, run_id)

    assert final.status == "COMPLETED" and await payments.count_refunds() == 1
    _, _, ops = await run_state(session_factory, run_id)
    assert ops["process_refund"].status == "SUCCEEDED" and ops["process_refund"].attempts == 2


async def test_a_request_lost_before_processing_is_retried_and_refunds_once(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    register_idempotent_refund(payments, mail, CrashPlan())
    payments.faults.inject(AMBIGUOUS_BEFORE_COMMIT, times=2)
    run_id = await start(session_factory, "refund", order_id="ORD-1")

    final = await runtime.execute_run(session_factory, run_id)

    assert final.status == "COMPLETED" and await payments.count_refunds() == 1
    _, _, ops = await run_state(session_factory, run_id)
    assert ops["process_refund"].attempts == 3


async def test_a_declined_payment_fails_the_run_and_moves_no_money(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    register_idempotent_refund(payments, mail, CrashPlan())
    payments.faults.inject(DECLINE)
    run_id = await start(session_factory, "refund", order_id="ORD-1")

    final = await runtime.execute_run(session_factory, run_id)

    assert final.status == "FAILED" and "declined" in final.error_message
    assert (await payments.count_refunds(), len(await mail.list_emails())) == (0, 0)
    _, events, ops = await run_state(session_factory, run_id)
    assert ops["process_refund"].status == "FAILED"
    assert event_types(events)[-2:] == ["STEP_FAILED", "WORKFLOW_FAILED"]


async def test_email_failure_after_the_refund_fails_the_run_but_the_refund_stands(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    register_idempotent_refund(payments, mail, CrashPlan())
    mail.faults.inject(FAIL)
    run_id = await start(session_factory, "refund", order_id="ORD-1")

    final = await runtime.execute_run(session_factory, run_id)

    assert final.status == "FAILED" and "mailbox unavailable" in final.error_message
    assert (await payments.count_refunds(), len(await mail.list_emails())) == (1, 0)
    _, events, _ = await run_state(session_factory, run_id)
    completed = [e.step_name for e in events if e.event_type == "STEP_COMPLETED"]
    assert completed == ["process_refund"]  # the history agrees with the ledger


async def test_a_lost_email_reply_is_retried_and_sends_once(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    register_idempotent_refund(payments, mail, CrashPlan())
    mail.faults.inject(AMBIGUOUS_AFTER_COMMIT)
    run_id = await start(session_factory, "refund", order_id="ORD-1")

    assert (await runtime.execute_run(session_factory, run_id)).status == "COMPLETED"
    assert len(await mail.list_emails()) == 1


async def test_ambiguity_that_never_clears_fails_the_step_after_bounded_retries(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    register_idempotent_refund(payments, mail, CrashPlan())
    payments.faults.inject(AMBIGUOUS_BEFORE_COMMIT, times=10)
    run_id = await start(session_factory, "refund", order_id="ORD-1")

    final = await runtime.execute_run(session_factory, run_id)

    assert final.status == "FAILED" and await payments.count_refunds() == 0
    _, _, ops = await run_state(session_factory, run_id)
    assert (ops["process_refund"].status, ops["process_refund"].attempts) == ("FAILED", 3)


# ---- non-idempotent provider, no way to check: OUTCOME_UNKNOWN ------------------------------


async def test_crash_after_a_non_idempotent_refund_is_not_retried_and_needs_review(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    crash = CrashPlan()
    crash.after.add("process_refund")
    emailed = register_legacy_refund(legacy_payments, crash, with_verify=False)
    run_id = await start(session_factory, "legacy", order_id="ORD-1")

    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)
    assert await legacy_payments.count_refunds() == 1

    run = await runtime.resume_run(session_factory, run_id)

    assert run.status == "NEEDS_REVIEW"  # parked, not failed, not retried
    assert await legacy_payments.count_refunds() == 1  # still exactly one: it was NOT repeated
    assert emailed == []  # nothing after the unknown step ran
    run, events, ops = await run_state(session_factory, run_id)
    assert events[-1].event_type == "STEP_OUTCOME_UNKNOWN"
    assert "no verify hook" in events[-1].result_data["reason"]
    assert "WORKFLOW_FAILED" not in event_types(events) and run.completed_at is None
    assert ops["process_refund"].status == "UNKNOWN"


async def test_a_run_needing_review_cannot_be_resumed_or_executed_again(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    crash = CrashPlan()
    crash.after.add("process_refund")
    register_legacy_refund(legacy_payments, crash, with_verify=False)
    run_id = await start(session_factory, "legacy", order_id="ORD-1")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)
    await runtime.resume_run(session_factory, run_id)

    with pytest.raises(InvalidRunStateError, match="NEEDS_REVIEW"):
        await runtime.resume_run(session_factory, run_id)
    with pytest.raises(InvalidRunStateError):
        await runtime.execute_run(session_factory, run_id)
    assert await legacy_payments.count_refunds() == 1


async def test_ambiguous_timeout_on_a_non_idempotent_tool_is_never_retried(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    register_legacy_refund(legacy_payments, CrashPlan(), with_verify=False)
    legacy_payments.faults.inject(AMBIGUOUS_AFTER_COMMIT)
    run_id = await start(session_factory, "legacy", order_id="ORD-1")

    run = await runtime.execute_run(session_factory, run_id)

    assert run.status == "NEEDS_REVIEW"
    assert await legacy_payments.count_refunds() == 1  # one call only, never repeated


# ---- the review path ------------------------------------------------------------------------


async def _needs_review_run(sf: SessionFactory, legacy: NonIdempotentPaymentService, *, crash_after: bool):
    crash = CrashPlan()
    (crash.after if crash_after else crash.before).add("process_refund")
    emailed = register_legacy_refund(legacy, crash, with_verify=False)
    run_id = await start(sf, "legacy", order_id="ORD-1")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(sf, run_id)
    assert (await runtime.resume_run(sf, run_id)).status == "NEEDS_REVIEW"
    return run_id, emailed


async def test_reviewer_confirms_the_refund_happened_and_the_run_finishes(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    run_id, emailed = await _needs_review_run(session_factory, legacy_payments, crash_after=True)
    refund = (await legacy_payments.list_refunds())[0]  # the reviewer checked the provider's dashboard

    reopened = await runtime.resolve_unknown_step(
        session_factory,
        run_id,
        "process_refund",
        outcome="completed",
        result=refund,
        message="found in the provider dashboard",
        resolved_by="asha",
    )
    assert reopened.status == "RUNNING"
    final = await runtime.resume_run(session_factory, run_id)

    assert final.status == "COMPLETED"
    assert final.result_data == {"refund_id": refund["refund_id"]}
    assert await legacy_payments.count_refunds() == 1  # the refund was never repeated
    assert emailed == [refund["refund_id"]]  # and the rest of the workflow ran
    _, events, ops = await run_state(session_factory, run_id)
    resolved = next(e for e in events if e.event_type == "STEP_RESOLVED")
    assert resolved.result_data["resolved_by"] == "asha" and resolved.result_data["outcome"] == "completed"
    assert ops["process_refund"].status == "RECONCILED"
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1))


async def test_reviewer_confirms_the_refund_did_not_happen_and_it_is_run_now(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    run_id, emailed = await _needs_review_run(session_factory, legacy_payments, crash_after=False)
    assert await legacy_payments.count_refunds() == 0  # the worker died before the call

    await runtime.resolve_unknown_step(
        session_factory, run_id, "process_refund", outcome="not_executed"
    )
    final = await runtime.resume_run(session_factory, run_id)

    assert final.status == "COMPLETED" and await legacy_payments.count_refunds() == 1
    assert len(emailed) == 1


async def test_reviewer_marks_the_step_failed_and_the_run_fails(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    run_id, emailed = await _needs_review_run(session_factory, legacy_payments, crash_after=True)

    await runtime.resolve_unknown_step(
        session_factory, run_id, "process_refund", outcome="failed", message="refund was reversed"
    )
    final = await runtime.resume_run(session_factory, run_id)

    assert final.status == "FAILED" and "refund was reversed" in final.error_message
    assert emailed == [] and await legacy_payments.count_refunds() == 1  # nothing repeated


async def test_resolution_is_validated(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    run_id, _ = await _needs_review_run(session_factory, legacy_payments, crash_after=True)

    with pytest.raises(InvalidResolutionError, match="outcome must be"):
        await runtime.resolve_unknown_step(session_factory, run_id, "process_refund", outcome="shrug")
    with pytest.raises(InvalidResolutionError, match="not awaiting"):
        await runtime.resolve_unknown_step(session_factory, run_id, "send_email", outcome="not_executed")
    with pytest.raises(InvalidResolutionError, match="not awaiting"):
        await runtime.resolve_unknown_step(session_factory, run_id, "nope", outcome="not_executed")
    with pytest.raises(StepSerializationError):  # a result that is not JSON
        await runtime.resolve_unknown_step(
            session_factory, run_id, "process_refund", outcome="completed", result=object()
        )

    await runtime.resolve_unknown_step(session_factory, run_id, "process_refund", outcome="not_executed")
    with pytest.raises(InvalidRunStateError):  # a second resolution: the run is RUNNING again
        await runtime.resolve_unknown_step(
            session_factory, run_id, "process_refund", outcome="not_executed"
        )
    _, events, _ = await run_state(session_factory, run_id)
    assert event_types(events).count("STEP_RESOLVED") == 1


async def test_only_one_of_two_simultaneous_resolutions_wins(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    import asyncio

    run_id, _ = await _needs_review_run(session_factory, legacy_payments, crash_after=True)
    outcomes = await asyncio.gather(
        *(
            runtime.resolve_unknown_step(
                session_factory, run_id, "process_refund", outcome="not_executed", resolved_by=f"r{i}"
            )
            for i in range(4)
        ),
        return_exceptions=True,
    )
    assert sum(1 for o in outcomes if not isinstance(o, Exception)) == 1
    _, events, _ = await run_state(session_factory, run_id)
    assert event_types(events).count("STEP_RESOLVED") == 1


# ---- non-idempotent provider WITH a verify hook: reconciliation -----------------------------


async def test_verify_finds_the_refund_after_a_crash_and_nothing_is_repeated(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    crash = CrashPlan()
    crash.after.add("process_refund")
    emailed = register_legacy_refund(legacy_payments, crash, with_verify=True)
    run_id = await start(session_factory, "legacy", order_id="ORD-1")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)

    final = await runtime.resume_run(session_factory, run_id)

    assert final.status == "COMPLETED"
    assert await legacy_payments.count_refunds() == 1
    assert final.result_data["refund_id"] == (await legacy_payments.list_refunds())[0]["refund_id"]
    assert len(emailed) == 1
    _, _, ops = await run_state(session_factory, run_id)
    assert ops["process_refund"].status == "RECONCILED"


async def test_verify_proves_nothing_happened_so_the_call_is_made_once(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    crash = CrashPlan()
    crash.before.add("process_refund")
    register_legacy_refund(legacy_payments, crash, with_verify=True)
    run_id = await start(session_factory, "legacy", order_id="ORD-1")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)

    final = await runtime.resume_run(session_factory, run_id)

    assert final.status == "COMPLETED" and await legacy_payments.count_refunds() == 1


async def test_lost_reply_on_a_non_idempotent_tool_is_resolved_by_verify(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    register_legacy_refund(legacy_payments, CrashPlan(), with_verify=True)
    legacy_payments.faults.inject(AMBIGUOUS_AFTER_COMMIT)
    run_id = await start(session_factory, "legacy", order_id="ORD-1")

    final = await runtime.execute_run(session_factory, run_id)

    assert final.status == "COMPLETED" and await legacy_payments.count_refunds() == 1


async def test_lost_request_on_a_non_idempotent_tool_is_retried_only_after_verify_says_so(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    register_legacy_refund(legacy_payments, CrashPlan(), with_verify=True)
    legacy_payments.faults.inject(AMBIGUOUS_BEFORE_COMMIT)
    run_id = await start(session_factory, "legacy", order_id="ORD-1")

    final = await runtime.execute_run(session_factory, run_id)

    assert final.status == "COMPLETED" and await legacy_payments.count_refunds() == 1


async def test_a_broken_verify_hook_parks_the_run_instead_of_guessing(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    @workflow(name="bad_verify")
    async def bad_verify(ctx):
        def verify(*_args):
            raise ConnectionError("provider API is down")

        return await ctx.effect(
            "process_refund",
            legacy_payments.refund,
            "ORD-1",
            2500,
            idempotent=False,
            verify=verify,
        )

    legacy_payments.faults.inject(AMBIGUOUS_AFTER_COMMIT)
    run_id = (await runtime.create_run(session_factory, "bad_verify", {})).run_id

    run = await runtime.execute_run(session_factory, run_id)

    assert run.status == "NEEDS_REVIEW" and await legacy_payments.count_refunds() == 1
    _, events, _ = await run_state(session_factory, run_id)
    assert "verification failed" in events[-1].result_data["reason"]


# ---- the wrong way, shown honestly ----------------------------------------------------------


async def test_wrapping_a_payment_in_a_plain_step_can_still_double_refund(
    session_factory: SessionFactory, legacy_payments: NonIdempotentPaymentService
) -> None:
    """Why ctx.effect exists. ctx.step assumes repeat-safe work; a payment is not, so after a crash
    the step runs again and the ledger shows two refunds. Use ctx.effect for effects."""
    crash = CrashPlan()
    crash.after.add("process_refund")

    @workflow(name="naive")
    async def naive(ctx):
        tool = crash.wrap("process_refund", legacy_payments.refund)
        return await ctx.step("process_refund", tool, "ORD-1", 2500)

    run_id = await start(session_factory, "naive")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)
    await runtime.resume_run(session_factory, run_id)

    assert await legacy_payments.count_refunds() == 2  # the duplicate

"""The refund example workflow end to end, checked against the fake payment and email ledgers."""

import uuid
from collections.abc import Iterator

import pytest

from app.database.repositories import EventRepository, IdempotencyRepository, RunRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.integrations.llm import MockLLMProvider
from app.integrations.tools import (
    EmailService,
    IdempotentPaymentService,
    set_email_service,
    set_payment_service,
)
from app.integrations.tools.fake_ledger import AMBIGUOUS_AFTER_COMMIT, DECLINE
from tests.fixtures.crash import CrashPlan, SimulatedCrash

import app.examples.refund_workflow  # noqa: F401  isort:skip  (registers "refund_workflow")

pytestmark = [pytest.mark.integration, pytest.mark.recovery]


@pytest.fixture
def payments(services_session_factory: SessionFactory) -> Iterator[IdempotentPaymentService]:
    service = IdempotentPaymentService(services_session_factory)
    set_payment_service(service)
    yield service
    set_payment_service(None)


@pytest.fixture
def mail(services_session_factory: SessionFactory) -> Iterator[EmailService]:
    service = EmailService(services_session_factory)
    set_email_service(service)
    yield service
    set_email_service(None)


async def go(sf: SessionFactory, order_id: str, llm: MockLLMProvider | None = None):
    run = await runtime.create_run(sf, "refund_workflow", {"order_id": order_id})
    return run.run_id, await runtime.execute_run(sf, run.run_id, llm_provider=llm or MockLLMProvider())


async def history(sf: SessionFactory, run_id: uuid.UUID) -> list:
    async with sf() as session:
        return await EventRepository(session).list_events(run_id)


def completed_steps(events: list) -> list[str]:
    return [e.step_name for e in events if e.event_type == "STEP_COMPLETED"]


async def test_a_justified_refund_is_processed_and_confirmed(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    llm = MockLLMProvider("APPROVE")
    run_id, run = await go(session_factory, "ORD-1001", llm)

    ledger = await payments.list_refunds()
    assert run.status == "COMPLETED"
    assert run.result_data == {
        "order_id": "ORD-1001",
        "status": "REFUNDED",
        "refund_id": ledger[0]["refund_id"],
        "amount": 4999,
    }
    assert [(r["order_id"], r["amount"]) for r in ledger] == [("ORD-1001", 4999)]
    sent = await mail.list_emails()
    assert [(e["to"], e["subject"]) for e in sent] == [("asha@example.com", "Your refund has been processed")]
    assert len(llm.calls) == 1 and "cracked left ear cup" in llm.calls[0]["prompt"]

    events = await history(session_factory, run_id)
    assert completed_steps(events) == [
        "fetch_order",
        "check_eligibility",
        "classify_complaint",
        "process_refund",
        "send_confirmation",
    ]
    llm_record = next(e.result_data for e in events if e.step_name == "classify_complaint" and e.result_data)
    assert llm_record["output"] == "APPROVE" and llm_record["input_tokens"] > 0
    assert llm_record["cost_usd"] is not None


async def test_an_order_outside_the_refund_window_is_rejected_without_calling_the_llm(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    llm = MockLLMProvider("APPROVE")
    _, run = await go(session_factory, "ORD-2002", llm)

    assert run.status == "COMPLETED"
    assert run.result_data == {
        "order_id": "ORD-2002",
        "status": "REJECTED",
        "reason": "outside the 30-day refund window",
    }
    assert llm.calls == []
    assert (await payments.count_refunds(), len(await mail.list_emails())) == (0, 0)


async def test_an_amount_over_the_automatic_limit_is_rejected(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    _, run = await go(session_factory, "ORD-3003")
    assert run.result_data["status"] == "REJECTED" and "limit" in run.result_data["reason"]
    assert await payments.count_refunds() == 0


async def test_an_llm_rejection_means_no_money_moves(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    _, run = await go(session_factory, "ORD-1001", MockLLMProvider("REJECT"))
    assert run.result_data == {
        "order_id": "ORD-1001",
        "status": "REJECTED",
        "reason": "complaint not justified",
    }
    assert (await payments.count_refunds(), len(await mail.list_emails())) == (0, 0)


async def test_llm_failure_fails_the_run_and_nothing_is_refunded(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    run_id, run = await go(
        session_factory, "ORD-1001", MockLLMProvider(fail_with=TimeoutError("model timed out"))
    )
    assert run.status == "FAILED" and "model timed out" in run.error_message
    assert await payments.count_refunds() == 0
    events = await history(session_factory, run_id)
    assert [e.event_type for e in events][-2:] == ["STEP_FAILED", "WORKFLOW_FAILED"]


async def test_unknown_order_fails_the_run(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    _, run = await go(session_factory, "ORD-9999")
    assert run.status == "FAILED" and "not found" in run.error_message


async def test_payment_failure_fails_the_run_and_sends_no_email(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    payments.faults.inject(DECLINE)
    _, run = await go(session_factory, "ORD-1001")
    assert run.status == "FAILED" and "declined" in run.error_message
    assert (await payments.count_refunds(), len(await mail.list_emails())) == (0, 0)


async def test_a_worker_crash_before_the_refund_resumes_to_one_refund(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    crash = CrashPlan()
    crash.before.add("process_refund")
    payments.refund = crash.wrap("process_refund", payments.refund)  # type: ignore[method-assign]
    run = await runtime.create_run(session_factory, "refund_workflow", {"order_id": "ORD-1001"})

    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run.run_id, llm_provider=MockLLMProvider())
    assert await payments.count_refunds() == 0

    final = await runtime.resume_run(session_factory, run.run_id, llm_provider=MockLLMProvider())
    assert final.status == "COMPLETED" and await payments.count_refunds() == 1


async def test_a_worker_crash_after_the_refund_resumes_to_one_refund_and_one_email(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    llm = MockLLMProvider("APPROVE")
    crash = CrashPlan()
    crash.after.add("process_refund")
    payments.refund = crash.wrap("process_refund", payments.refund)  # type: ignore[method-assign]
    run = await runtime.create_run(session_factory, "refund_workflow", {"order_id": "ORD-1001"})

    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run.run_id, llm_provider=llm)
    assert await payments.count_refunds() == 1  # the money already moved

    final = await runtime.resume_run(session_factory, run.run_id, llm_provider=llm)

    assert final.status == "COMPLETED"
    assert (await payments.count_refunds(), len(await mail.list_emails())) == (1, 1)
    assert len(llm.calls) == 1  # the model was not asked a second time

    # Runtime history, payment ledger and email ledger all tell the same story.
    events = await history(session_factory, run.run_id)
    assert completed_steps(events).count("process_refund") == 1
    assert completed_steps(events).count("send_confirmation") == 1
    ledger = (await payments.list_refunds())[0]
    assert final.result_data["refund_id"] == ledger["refund_id"]
    async with session_factory() as session:
        ops = {o.step_name: o for o in await IdempotencyRepository(session).list_for_run(run.run_id)}
    assert ops["process_refund"].result_data["refund_id"] == ledger["refund_id"]
    assert ops["process_refund"].idempotency_key == ledger["idempotency_key"]


async def test_a_lost_reply_from_the_payment_provider_does_not_double_refund(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    payments.faults.inject(AMBIGUOUS_AFTER_COMMIT)
    _, run = await go(session_factory, "ORD-1001")
    assert run.status == "COMPLETED" and await payments.count_refunds() == 1


async def test_two_runs_for_the_same_order_are_two_independent_operations(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    """Idempotency keys are per run and step, so they stop a RETRY from repeating an effect, not a
    second, separate run. Preventing duplicate refunds per order is a business rule for the
    workflow author (e.g. check for an existing refund first); documented in docs/idempotency.md."""
    await go(session_factory, "ORD-1001")
    await go(session_factory, "ORD-1001")
    assert await payments.count_refunds("ORD-1001") == 2


async def test_run_status_lookup_helpers_agree_with_the_ledger(
    session_factory: SessionFactory, payments: IdempotentPaymentService, mail: EmailService
) -> None:
    run_id, _ = await go(session_factory, "ORD-1001")
    async with session_factory() as session:
        run = await RunRepository(session).get_run(run_id)
    ledger = await payments.list_refunds()
    assert run.status == "COMPLETED" and run.result_data["refund_id"] == ledger[0]["refund_id"]

"""The fake services are the ground truth for later tests, so they get their own tests first."""

import asyncio

import pytest

from app.database.session import SessionFactory
from app.engine.errors import AmbiguousOutcomeError
from app.integrations.tools import (
    EmailService,
    IdempotentPaymentService,
    NonIdempotentPaymentService,
)
from app.integrations.tools.errors import (
    EmailDeliveryError,
    IdempotencyKeyReuseError,
    PaymentDeclinedError,
)
from app.integrations.tools.fake_ledger import (
    AMBIGUOUS_AFTER_COMMIT,
    AMBIGUOUS_BEFORE_COMMIT,
    DECLINE,
    FAIL,
)

pytestmark = pytest.mark.integration


# ---- idempotent payment provider ------------------------------------------------------------


async def test_same_key_returns_the_original_refund_and_creates_no_second_one(
    services_session_factory: SessionFactory,
) -> None:
    payments = IdempotentPaymentService(services_session_factory)

    first = await payments.refund("ORD-1", 2500, idempotency_key="run101:process_refund")
    second = await payments.refund("ORD-1", 2500, idempotency_key="run101:process_refund")

    assert second == first  # the original result, including the same refund_id
    assert await payments.count_refunds() == 1


async def test_different_keys_create_different_refunds(services_session_factory: SessionFactory) -> None:
    payments = IdempotentPaymentService(services_session_factory)
    a = await payments.refund("ORD-1", 100, idempotency_key="k1")
    b = await payments.refund("ORD-1", 100, idempotency_key="k2")
    assert a["refund_id"] != b["refund_id"] and await payments.count_refunds() == 2


async def test_reusing_a_key_with_different_parameters_is_rejected(
    services_session_factory: SessionFactory,
) -> None:
    payments = IdempotentPaymentService(services_session_factory)
    await payments.refund("ORD-1", 2000, idempotency_key="k")
    with pytest.raises(IdempotencyKeyReuseError):
        await payments.refund("ORD-1", 3000, idempotency_key="k")
    assert await payments.count_refunds() == 1


async def test_idempotent_provider_requires_a_key(services_session_factory: SessionFactory) -> None:
    with pytest.raises(ValueError):
        await IdempotentPaymentService(services_session_factory).refund("ORD-1", 1, idempotency_key="")


async def test_concurrent_requests_with_one_key_create_a_single_refund(
    services_session_factory: SessionFactory,
) -> None:
    payments = IdempotentPaymentService(services_session_factory)
    results = await asyncio.gather(
        *(payments.refund("ORD-1", 2500, idempotency_key="race") for _ in range(12))
    )
    assert len({r["refund_id"] for r in results}) == 1
    assert await payments.count_refunds() == 1


# ---- non-idempotent payment provider --------------------------------------------------------


async def test_non_idempotent_provider_refunds_every_time_even_with_a_key(
    services_session_factory: SessionFactory,
) -> None:
    payments = NonIdempotentPaymentService(services_session_factory)
    await payments.refund("ORD-1", 2500, idempotency_key="same")
    await payments.refund("ORD-1", 2500, idempotency_key="same")
    assert await payments.count_refunds() == 2  # the duplicate that idempotency would have prevented


async def test_non_idempotent_provider_can_be_asked_about_a_reference(
    services_session_factory: SessionFactory,
) -> None:
    payments = NonIdempotentPaymentService(services_session_factory)
    assert await payments.find_by_reference("run1:refund") is None
    refund = await payments.refund("ORD-1", 2500, "run1:refund")
    assert (await payments.find_by_reference("run1:refund"))["refund_id"] == refund["refund_id"]


# ---- fault injection ------------------------------------------------------------------------


async def test_decline_has_no_effect(services_session_factory: SessionFactory) -> None:
    payments = IdempotentPaymentService(services_session_factory)
    payments.faults.inject(DECLINE)
    with pytest.raises(PaymentDeclinedError):
        await payments.refund("ORD-1", 100, idempotency_key="k")
    assert await payments.count_refunds() == 0


async def test_ambiguous_before_commit_has_no_effect(services_session_factory: SessionFactory) -> None:
    payments = IdempotentPaymentService(services_session_factory)
    payments.faults.inject(AMBIGUOUS_BEFORE_COMMIT)
    with pytest.raises(AmbiguousOutcomeError):
        await payments.refund("ORD-1", 100, idempotency_key="k")
    assert await payments.count_refunds() == 0


async def test_ambiguous_after_commit_HAS_an_effect_the_caller_cannot_see(
    services_session_factory: SessionFactory,
) -> None:
    """The classic: the refund went through, but the caller only sees a timeout."""
    payments = IdempotentPaymentService(services_session_factory)
    payments.faults.inject(AMBIGUOUS_AFTER_COMMIT)
    with pytest.raises(AmbiguousOutcomeError):
        await payments.refund("ORD-1", 100, idempotency_key="k")
    assert await payments.count_refunds() == 1


async def test_faults_apply_once_then_calls_succeed(services_session_factory: SessionFactory) -> None:
    payments = IdempotentPaymentService(services_session_factory)
    payments.faults.inject(DECLINE, times=2)
    for _ in range(2):
        with pytest.raises(PaymentDeclinedError):
            await payments.refund("ORD-1", 100, idempotency_key="k")
    assert (await payments.refund("ORD-1", 100, idempotency_key="k"))["status"] == "SUCCEEDED"


# ---- email ----------------------------------------------------------------------------------


async def test_email_with_a_key_is_sent_once(services_session_factory: SessionFactory) -> None:
    mail = EmailService(services_session_factory)
    a = await mail.send("a@example.com", "Hi", "body", idempotency_key="k")
    b = await mail.send("a@example.com", "Hi", "body", idempotency_key="k")
    assert a == b and len(await mail.list_emails()) == 1


async def test_email_without_a_key_is_sent_every_time(services_session_factory: SessionFactory) -> None:
    mail = EmailService(services_session_factory)
    await mail.send("a@example.com", "Hi", "body")
    await mail.send("a@example.com", "Hi", "body")
    assert len(await mail.list_emails()) == 2


async def test_email_key_reuse_with_different_content_is_rejected(
    services_session_factory: SessionFactory,
) -> None:
    mail = EmailService(services_session_factory)
    await mail.send("a@example.com", "Hi", "body", idempotency_key="k")
    with pytest.raises(IdempotencyKeyReuseError):
        await mail.send("a@example.com", "Hi", "DIFFERENT", idempotency_key="k")


async def test_email_faults(services_session_factory: SessionFactory) -> None:
    mail = EmailService(services_session_factory)
    mail.faults.inject(FAIL)
    with pytest.raises(EmailDeliveryError):
        await mail.send("a@example.com", "Hi", "body")
    assert await mail.list_emails() == []
    mail.faults.inject(AMBIGUOUS_AFTER_COMMIT)
    with pytest.raises(AmbiguousOutcomeError):
        await mail.send("a@example.com", "Hi", "body", idempotency_key="k")
    assert len(await mail.list_emails()) == 1


# ---- the ledger really is independent and durable -------------------------------------------


async def test_ledger_survives_a_new_service_instance(services_session_factory: SessionFactory) -> None:
    """A 'restarted' service (new object, same database) still knows its past refunds."""
    await IdempotentPaymentService(services_session_factory).refund("ORD-1", 100, idempotency_key="k")
    restarted = IdempotentPaymentService(services_session_factory)
    assert await restarted.count_refunds() == 1
    assert (await restarted.refund("ORD-1", 100, idempotency_key="k"))["status"] == "SUCCEEDED"
    assert await restarted.count_refunds() == 1


async def test_ledger_lives_in_a_separate_database_from_the_runtime(
    services_session_factory: SessionFactory, session_factory: SessionFactory
) -> None:
    from sqlalchemy import text

    async with session_factory() as runtime_session:
        tables = {
            r[0]
            for r in await runtime_session.execute(
                text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            )
        }
    assert "workflow_events" in tables
    assert "payment_refunds" not in tables and "emails" not in tables

"""Access to the process-wide fake services used by the example workflows.

Workflows call payments() / emails() at run time (not at import time), so tests can swap in their own
instances with set_payment_service() / set_email_service().
"""

from app.integrations.tools.email_service import EmailService
from app.integrations.tools.fake_ledger import get_services_session_factory
from app.integrations.tools.payment_service import (
    IdempotentPaymentService,
    NonIdempotentPaymentService,
)

PaymentService = IdempotentPaymentService | NonIdempotentPaymentService

_payments: PaymentService | None = None
_emails: EmailService | None = None


def payments() -> PaymentService:
    global _payments
    if _payments is None:
        _payments = IdempotentPaymentService(get_services_session_factory())
    return _payments


def emails() -> EmailService:
    global _emails
    if _emails is None:
        _emails = EmailService(get_services_session_factory())
    return _emails


def set_payment_service(service: PaymentService | None) -> None:
    global _payments
    _payments = service


def set_email_service(service: EmailService | None) -> None:
    global _emails
    _emails = service


__all__ = [
    "EmailService",
    "IdempotentPaymentService",
    "NonIdempotentPaymentService",
    "emails",
    "payments",
    "set_email_service",
    "set_payment_service",
]

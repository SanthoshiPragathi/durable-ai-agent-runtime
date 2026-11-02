"""Read-only views of the fake services' own ledgers (independent of the runtime's event log)."""

from fastapi import APIRouter, Depends

from app.api.deps import get_services_session_factory_dep
from app.api.schemas import EmailOut, RefundOut
from app.database.session import SessionFactory
from app.integrations.tools.email_service import EmailService
from app.integrations.tools.payment_service import IdempotentPaymentService

router = APIRouter(prefix="/fake-services", tags=["fake-services"])


@router.get("/payments/refunds", response_model=list[RefundOut], summary="Payment provider's refund ledger")
async def list_refunds(
    order_id: str | None = None,
    session_factory: SessionFactory = Depends(get_services_session_factory_dep),
) -> list[RefundOut]:
    rows = await IdempotentPaymentService(session_factory).list_refunds(order_id)
    return [RefundOut(**r) for r in rows]


@router.get("/emails", response_model=list[EmailOut], summary="Email provider's sent-mail ledger")
async def list_emails(
    to: str | None = None,
    session_factory: SessionFactory = Depends(get_services_session_factory_dep),
) -> list[EmailOut]:
    return [EmailOut(**e) for e in await EmailService(session_factory).list_emails(to)]

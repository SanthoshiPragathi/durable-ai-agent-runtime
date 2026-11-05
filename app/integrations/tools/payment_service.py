"""Fake payment providers.

IdempotentPaymentService behaves like a well-built provider (e.g. Stripe's Idempotency-Key header):
the same key returns the ORIGINAL result and never creates a second refund.

NonIdempotentPaymentService behaves like a legacy provider: every call creates a refund, whatever
key it is sent. It can still store a caller-chosen `reference` and look it up, which is what makes
reconciliation ("did my request go through?") possible.

Both write to their own ledger (fake_ledger.py), independent of the runtime's event log.
"""

import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database.session import SessionFactory
from app.engine.errors import AmbiguousOutcomeError
from app.integrations.tools.errors import (
    IdempotencyKeyReuseError,
    PaymentDeclinedError,
    connection_loss_is_ambiguous,
)
from app.integrations.tools.fake_ledger import (
    AMBIGUOUS_AFTER_COMMIT,
    AMBIGUOUS_BEFORE_COMMIT,
    DECLINE,
    FaultInjector,
    RefundRecord,
    ensure_services_schema,
)


def refund_to_dict(row: RefundRecord) -> dict[str, Any]:
    return {
        "refund_id": row.refund_id,
        "order_id": row.order_id,
        "amount": row.amount,
        "status": row.status,
        "idempotency_key": row.idempotency_key,
        "reference": row.reference,
    }


class _PaymentBase:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._sf = session_factory
        self.faults = FaultInjector()

    async def _begin(self) -> str | None:
        """Prepare the ledger and apply a "before the effect" fault. Returns any later fault."""
        await ensure_services_schema(self._sf)
        fault = self.faults.next()
        if fault == DECLINE:
            raise PaymentDeclinedError("card declined by the issuer")
        if fault == AMBIGUOUS_BEFORE_COMMIT:
            raise AmbiguousOutcomeError("timed out before the provider processed the request")
        return fault

    @staticmethod
    def _after(fault: str | None) -> None:
        if fault == AMBIGUOUS_AFTER_COMMIT:
            raise AmbiguousOutcomeError("connection dropped after the request was sent")

    # inspection (what an operator or a verify hook would use)

    async def list_refunds(self, order_id: str | None = None) -> list[dict[str, Any]]:
        await ensure_services_schema(self._sf)
        stmt = select(RefundRecord).order_by(RefundRecord.id)
        if order_id is not None:
            stmt = stmt.where(RefundRecord.order_id == order_id)
        async with self._sf() as session:
            return [refund_to_dict(r) for r in (await session.execute(stmt)).scalars()]

    async def count_refunds(self, order_id: str | None = None) -> int:
        await ensure_services_schema(self._sf)
        stmt = select(func.count()).select_from(RefundRecord)
        if order_id is not None:
            stmt = stmt.where(RefundRecord.order_id == order_id)
        async with self._sf() as session:
            return (await session.execute(stmt)).scalar_one()

    async def find_by_reference(self, reference: str) -> dict[str, Any] | None:
        await ensure_services_schema(self._sf)
        async with self._sf() as session:
            row = (
                await session.execute(
                    select(RefundRecord).where(RefundRecord.reference == reference).limit(1)
                )
            ).scalar_one_or_none()
        return refund_to_dict(row) if row else None


class IdempotentPaymentService(_PaymentBase):
    @connection_loss_is_ambiguous
    async def refund(
        self, order_id: str, amount: int, *, idempotency_key: str, reference: str | None = None
    ) -> dict[str, Any]:
        if not idempotency_key:
            raise ValueError("this provider requires an idempotency key")
        later_fault = await self._begin()

        async with self._sf() as session, session.begin():
            # ON CONFLICT makes "create unless this key already exists" one atomic statement, so
            # concurrent requests with the same key cannot both create a refund.
            await session.execute(
                pg_insert(RefundRecord)
                .values(
                    refund_id=f"rf_{uuid.uuid4().hex[:16]}",
                    order_id=order_id,
                    amount=amount,
                    idempotency_key=idempotency_key,
                    reference=reference,
                    status="SUCCEEDED",
                )
                .on_conflict_do_nothing(index_elements=["idempotency_key"])
            )
            row = (
                await session.execute(
                    select(RefundRecord).where(RefundRecord.idempotency_key == idempotency_key)
                )
            ).scalar_one()
            if (row.order_id, row.amount) != (order_id, amount):
                raise IdempotencyKeyReuseError(
                    f"idempotency key {idempotency_key!r} was already used with different parameters"
                )
            result = refund_to_dict(row)

        self._after(later_fault)
        return result

    async def find_by_idempotency_key(self, key: str) -> dict[str, Any] | None:
        await ensure_services_schema(self._sf)
        async with self._sf() as session:
            row = (
                await session.execute(select(RefundRecord).where(RefundRecord.idempotency_key == key))
            ).scalar_one_or_none()
        return refund_to_dict(row) if row else None


class NonIdempotentPaymentService(_PaymentBase):
    @connection_loss_is_ambiguous
    async def refund(
        self,
        order_id: str,
        amount: int,
        reference: str | None = None,
        *,
        idempotency_key: str | None = None,  # accepted and IGNORED: this provider cannot dedupe
    ) -> dict[str, Any]:
        later_fault = await self._begin()
        async with self._sf() as session, session.begin():
            row = RefundRecord(
                refund_id=f"rf_{uuid.uuid4().hex[:16]}",
                order_id=order_id,
                amount=amount,
                reference=reference,
                status="SUCCEEDED",
            )
            session.add(row)
            await session.flush()
            result = refund_to_dict(row)
        self._after(later_fault)
        return result

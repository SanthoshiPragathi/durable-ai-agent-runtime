"""Fake email provider with its own ledger. With an idempotency key it sends at most once per key;
without one it sends every time."""

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database.session import SessionFactory
from app.engine.errors import AmbiguousOutcomeError
from app.integrations.tools.errors import (
    EmailDeliveryError,
    IdempotencyKeyReuseError,
    connection_loss_is_ambiguous,
)
from app.integrations.tools.fake_ledger import (
    AMBIGUOUS_AFTER_COMMIT,
    AMBIGUOUS_BEFORE_COMMIT,
    FAIL,
    EmailRecord,
    FaultInjector,
    ensure_services_schema,
)


def email_to_dict(row: EmailRecord) -> dict[str, Any]:
    return {
        "email_id": row.email_id,
        "to": row.to_address,
        "subject": row.subject,
        "idempotency_key": row.idempotency_key,
    }


class EmailService:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._sf = session_factory
        self.faults = FaultInjector()

    @connection_loss_is_ambiguous
    async def send(
        self, to: str, subject: str, body: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        await ensure_services_schema(self._sf)
        fault = self.faults.next()
        if fault == FAIL:
            raise EmailDeliveryError("mailbox unavailable")
        if fault == AMBIGUOUS_BEFORE_COMMIT:
            raise AmbiguousOutcomeError("timed out before the mail server accepted the message")

        values = {
            "email_id": f"em_{uuid.uuid4().hex[:16]}",
            "to_address": to,
            "subject": subject,
            "body": body,
            "idempotency_key": idempotency_key,
        }
        async with self._sf() as session, session.begin():
            if idempotency_key is None:
                row = EmailRecord(**values)
                session.add(row)
                await session.flush()
            else:
                await session.execute(
                    pg_insert(EmailRecord)
                    .values(**values)
                    .on_conflict_do_nothing(index_elements=["idempotency_key"])
                )
                row = (
                    await session.execute(
                        select(EmailRecord).where(EmailRecord.idempotency_key == idempotency_key)
                    )
                ).scalar_one()
                if (row.to_address, row.subject, row.body) != (to, subject, body):
                    raise IdempotencyKeyReuseError(
                        f"idempotency key {idempotency_key!r} was already used with a different email"
                    )
            result = email_to_dict(row)

        if fault == AMBIGUOUS_AFTER_COMMIT:
            raise AmbiguousOutcomeError("connection dropped after the message was accepted")
        return result

    async def list_emails(self, to: str | None = None) -> list[dict[str, Any]]:
        await ensure_services_schema(self._sf)
        stmt = select(EmailRecord).order_by(EmailRecord.id)
        if to is not None:
            stmt = stmt.where(EmailRecord.to_address == to)
        async with self._sf() as session:
            return [email_to_dict(r) for r in (await session.execute(stmt)).scalars()]

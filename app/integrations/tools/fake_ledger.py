"""The fake services' OWN durable store, in a separate database from the runtime.

It plays the role of "the real world": the payment provider's books. Tests compare it with the
runtime's event history, and because it lives in its own database it survives worker crashes and is
not touched by the runtime's transactions. Tables are created with create_all (it is not part of the
runtime schema, so it has no Alembic migrations).
"""

import weakref
from collections import deque
from datetime import datetime
from functools import lru_cache
from typing import Any

from sqlalchemy import BigInteger, DateTime, Identity, String, func
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.config import get_settings
from app.database.session import SessionFactory, create_session_factory


class ServicesBase(DeclarativeBase):
    pass


class RefundRecord(ServicesBase):
    __tablename__ = "payment_refunds"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    refund_id: Mapped[str] = mapped_column(String(40), unique=True, nullable=False)
    order_id: Mapped[str] = mapped_column(String(100), nullable=False)
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # Set only by the idempotent service. NULLs do not collide, so the non-idempotent service
    # (which leaves it empty) can create any number of rows.
    idempotency_key: Mapped[str | None] = mapped_column(String(400), unique=True)
    # A caller-chosen label that a service may store WITHOUT deduplicating on it. It lets a
    # non-idempotent service still be asked "did my request go through?".
    reference: Mapped[str | None] = mapped_column(String(400), index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="SUCCEEDED")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class EmailRecord(ServicesBase):
    __tablename__ = "emails"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    email_id: Mapped[str] = mapped_column(String(40), unique=True, nullable=False)
    to_address: Mapped[str] = mapped_column(String(320), nullable=False)
    subject: Mapped[str] = mapped_column(String(500), nullable=False)
    body: Mapped[str] = mapped_column(String, nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(400), unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


# ---- faults ---------------------------------------------------------------------------------

DECLINE = "decline"  # definitive refusal; no effect
FAIL = "fail"  # definitive failure (email); no effect
AMBIGUOUS_BEFORE_COMMIT = "ambiguous_before_commit"  # request lost: no effect, caller cannot know
AMBIGUOUS_AFTER_COMMIT = "ambiguous_after_commit"  # effect DONE, but the reply was lost


class FaultInjector:
    """Queue of faults the next service calls will hit, to script failures in tests."""

    def __init__(self) -> None:
        self._queue: deque[str] = deque()

    def inject(self, fault: str, times: int = 1) -> None:
        self._queue.extend([fault] * times)

    def next(self) -> str | None:
        return self._queue.popleft() if self._queue else None

    def clear(self) -> None:
        self._queue.clear()


# ---- engine / session -----------------------------------------------------------------------

_initialized: "weakref.WeakSet[AsyncEngine]" = weakref.WeakSet()


def create_services_engine(url: str | None = None, **kwargs: Any) -> AsyncEngine:
    return create_async_engine(url or get_settings().services_database_url, pool_pre_ping=True, **kwargs)


async def init_services_db(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(ServicesBase.metadata.create_all)
    _initialized.add(engine)


async def ensure_services_schema(session_factory: SessionFactory) -> None:
    """Create the ledger tables once per engine (cheap no-op afterwards)."""
    engine: AsyncEngine = session_factory.kw["bind"]
    if engine not in _initialized:
        await init_services_db(engine)


@lru_cache
def get_services_session_factory() -> async_sessionmaker[AsyncSession]:
    return create_session_factory(create_services_engine())

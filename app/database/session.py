"""Async engine and session factory.

The default engine is created lazily so importing the app never requires a running database.
"""

from functools import lru_cache
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings

SessionFactory = async_sessionmaker[AsyncSession]


def create_engine(url: str | None = None, **kwargs: Any) -> AsyncEngine:
    return create_async_engine(url or get_settings().database_url, pool_pre_ping=True, **kwargs)


def create_session_factory(engine: AsyncEngine) -> SessionFactory:
    # expire_on_commit=False: objects stay readable after the short transaction that loaded them.
    return async_sessionmaker(engine, expire_on_commit=False)


@lru_cache
def get_default_session_factory() -> SessionFactory:
    return create_session_factory(create_engine())

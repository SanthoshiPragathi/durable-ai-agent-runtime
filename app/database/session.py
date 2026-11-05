"""Async engine and session factory.

The default engine is created lazily so importing the app never requires a running database.
"""

import math
from functools import lru_cache
from typing import Any

from sqlalchemy import Engine
from sqlalchemy import create_engine as sa_create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings

SessionFactory = async_sessionmaker[AsyncSession]


def async_connect_args() -> dict[str, Any]:
    """asyncpg limits: `timeout` bounds opening a connection (handshake included), `command_timeout`
    bounds each statement. Without them a database that accepts the socket but never answers blocks
    the caller until the OS gives up."""
    settings = get_settings()
    args: dict[str, Any] = {"timeout": settings.db_connect_timeout_seconds}
    if settings.db_command_timeout_seconds is not None:
        args["command_timeout"] = settings.db_command_timeout_seconds
    return args


def sync_connect_args() -> dict[str, Any]:
    """The same bound for the synchronous psycopg engines (Alembic, test fixtures). libpq's
    connect_timeout is whole seconds, and 0 would mean "wait forever", so round up to at least 1."""
    return {"connect_timeout": max(1, math.ceil(get_settings().db_connect_timeout_seconds))}


def create_sync_engine(url: str, **kwargs: Any) -> Engine:
    """A synchronous engine whose connection attempts are bounded."""
    connect_args = {**sync_connect_args(), **kwargs.pop("connect_args", {})}
    return sa_create_engine(url, connect_args=connect_args, **kwargs)


def create_engine(url: str | None = None, **kwargs: Any) -> AsyncEngine:
    connect_args = {**async_connect_args(), **kwargs.pop("connect_args", {})}
    return create_async_engine(
        url or get_settings().database_url, pool_pre_ping=True, connect_args=connect_args, **kwargs
    )


def create_session_factory(engine: AsyncEngine) -> SessionFactory:
    # expire_on_commit=False: objects stay readable after the short transaction that loaded them.
    return async_sessionmaker(engine, expire_on_commit=False)


@lru_cache
def get_default_session_factory() -> SessionFactory:
    return create_session_factory(create_engine())

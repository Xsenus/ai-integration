"""Helper utilities for short-lived Postgres transactions."""

from __future__ import annotations

from typing import Awaitable, Callable, Optional, TypeVar

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.db.postgres import get_postgres_engine

T = TypeVar("T")


def get_primary_engine() -> Optional[AsyncEngine]:
    """Returns the main application engine (Postgres).

    The helper mirrors the interface that is used in operational scripts.
    If the DSN is not configured the function simply returns ``None`` so
    callers can handle the situation gracefully.
    """

    return get_postgres_engine()


async def run_on_engine(
    engine: AsyncEngine,
    action: Callable[[AsyncConnection], Awaitable[T]],
) -> T:
    """Executes ``action`` within a single connection of ``engine``.

    The helper mirrors the ergonomics that analysts use in ad-hoc scripts
    (see the task description) and ensures that the connection is properly
    disposed of once the coroutine completes.
    """

    async with engine.begin() as conn:
        return await action(conn)


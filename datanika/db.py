"""Centralized database engine and session factories.

All code should use these engines instead of creating their own.
- Async engine/sessions: Reflex UI state handlers
- Sync engine/sessions: Celery tasks, API middleware, Starlette routes
"""

from collections.abc import AsyncGenerator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from datanika.config import settings

_pool_kwargs = {
    "pool_size": settings.db_pool_size,
    "max_overflow": settings.db_max_overflow,
    "pool_timeout": settings.db_pool_timeout,
    "pool_recycle": settings.db_pool_recycle,
    "pool_pre_ping": True,
}

# Async engine — used by Reflex UI via get_session()
engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    **_pool_kwargs,
)
async_session_factory = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False,
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        yield session


# Sync engine — used by Celery tasks, API middleware, Starlette routes
sync_engine = create_engine(
    settings.database_url_sync,
    echo=settings.debug,
    **_pool_kwargs,
)


def get_sync_session() -> Session:
    """Return a sync Session bound to the shared sync engine."""
    return Session(sync_engine)

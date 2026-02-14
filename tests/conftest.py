import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from etlfabric.models.base import Base


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="session")
def engine():
    """SQLite in-memory engine for fast model tests."""
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def db_session(engine):
    """Provide a transactional session that rolls back after each test."""
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()

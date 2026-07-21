"""Shared harness for exercising ``@api_endpoint`` routes over real HTTP (core#416).

Routes decorated with ``@api_endpoint`` cannot be called directly — the
decorator owns auth and exposes no ``__wrapped__`` — so the only way to test a
route's own behaviour (status codes, error mapping, response shape) is through
a client with the middleware patched. That harness existed, but **file-local**
in ``test_transformation_compile.py``, which is why most ``api_v1`` routes have
no endpoint tests at all: the cost of writing the first one was copying it.

Lifted here unchanged in behaviour, minus the compile-specific bits (dbt dir,
Fernet key) which stay with the tests that need them. What's generic is the
auth layer, and that is all this provides.
"""

import contextlib
from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.rate_limit_service import RateLimitResult


@pytest.fixture
def fake_api_key():
    """An authenticated key with full access (``scopes=None`` = unscoped)."""
    key = MagicMock()
    key.id = 1
    key.user_id = 1
    key.name = "Test Key"
    key.scopes = None
    return key


@pytest.fixture
def rate_limit_ok():
    return RateLimitResult(
        allowed=True,
        current_count=1,
        limit=60,
        remaining=59,
        retry_after=0,
        reset_at=9999999999,
    )


@pytest.fixture
def client():
    return TestClient(Starlette(routes=api_v1_routes))


def api_headers(token: str = "etf_test") -> dict:
    return {"Authorization": f"Bearer {token}"}


@contextlib.contextmanager
def patch_api_auth(
    fake_api_key,
    rate_limit_ok,
    db_session,
    org_id: int = 1,
    *,
    authenticated: bool = True,
):
    """Patch ``api_middleware`` so a request reaches the handler.

    ``authenticated=False`` makes the key lookup fail the way the real service
    does on an unknown/expired/out-of-scope key, so a 401 can be asserted
    without hand-rolling the middleware's behaviour.
    """
    fake_api_key.org_id = org_id

    @contextlib.contextmanager
    def fake_session():
        yield db_session

    with (
        patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
        patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
        patch("datanika.services.api_middleware._get_session", fake_session),
    ):
        mock_svc.authenticate_api_key.return_value = fake_api_key if authenticated else None
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = rate_limit_ok
        yield mock_svc

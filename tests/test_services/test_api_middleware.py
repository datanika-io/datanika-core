"""Tests for API middleware — auth + rate limiting integration."""

from unittest.mock import MagicMock, patch

import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.routing import Route

from datanika.services.api_middleware import api_endpoint
from datanika.services.rate_limit_service import RateLimitResult


# Sample endpoints decorated with api_endpoint
@api_endpoint(required_scope="pipeline:read")
async def sample_handler(request, api_key, session):
    return JSONResponse({"ok": True, "org_id": api_key.org_id})


@api_endpoint()
async def sample_handler_no_scope(request, api_key, session):
    return JSONResponse({"ok": True})


@pytest.fixture
def fake_api_key():
    key = MagicMock()
    key.id = 1
    key.org_id = 10
    key.name = "Test Key"
    return key


@pytest.fixture
def rate_limit_ok():
    return RateLimitResult(
        allowed=True, current_count=1, limit=60,
        remaining=59, retry_after=0, reset_at=9999999999,
    )


@pytest.fixture
def rate_limit_exceeded():
    return RateLimitResult(
        allowed=False, current_count=61, limit=60,
        remaining=0, retry_after=45, reset_at=9999999999,
    )


class TestApiEndpointAuth:
    def test_missing_auth_header(self):
        app = Starlette(routes=[Route("/test", sample_handler)])
        client = TestClient(app)
        resp = client.get("/test")
        assert resp.status_code == 401
        assert "Missing" in resp.json()["error"]

    def test_invalid_auth_scheme(self):
        app = Starlette(routes=[Route("/test", sample_handler)])
        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Basic abc"})
        assert resp.status_code == 401

    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_invalid_key(self, mock_session, mock_svc):
        mock_svc.authenticate_api_key.return_value = None
        mock_session.return_value.__enter__ = lambda s: MagicMock()
        mock_session.return_value.__exit__ = lambda s, *a: None

        app = Starlette(routes=[Route("/test", sample_handler)])
        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Bearer etf_badkey"})
        assert resp.status_code == 401
        assert "Invalid" in resp.json()["error"]


class TestApiEndpointRateLimit:
    @patch("datanika.services.api_middleware._rate_limit_svc")
    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_rate_limited(self, mock_session, mock_svc, mock_rl, fake_api_key, rate_limit_exceeded):
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        mock_svc.authenticate_api_key.return_value = fake_api_key
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = rate_limit_exceeded

        app = Starlette(routes=[Route("/test", sample_handler)])
        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Bearer etf_validkey"})
        assert resp.status_code == 429
        assert "Retry-After" in resp.headers
        assert resp.headers["Retry-After"] == "45"

    @patch("datanika.services.api_middleware._rate_limit_svc")
    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_success_with_rate_headers(
        self, mock_session, mock_svc, mock_rl, fake_api_key, rate_limit_ok
    ):
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        mock_svc.authenticate_api_key.return_value = fake_api_key
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = rate_limit_ok

        app = Starlette(routes=[Route("/test", sample_handler)])
        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Bearer etf_validkey"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        assert resp.headers["X-RateLimit-Limit"] == "60"
        assert resp.headers["X-RateLimit-Remaining"] == "59"

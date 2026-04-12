"""Tests for Tier 4 Agent Compatibility: cancel, wait, idempotency (#60)."""

from __future__ import annotations

import contextlib
import json
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SASession
from sqlalchemy.pool import StaticPool
from starlette.applications import Starlette
from starlette.testclient import TestClient

import datanika.models.invitation  # noqa: F401
import datanika.models.notification_channel  # noqa: F401
import datanika.models.sso_config  # noqa: F401
from datanika.models.base import Base
from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.rate_limit_service import RateLimitResult


@pytest.fixture
def engine():
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def db_session(engine):
    session = SASession(engine)
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def fake_api_key():
    key = MagicMock()
    key.id = 1
    key.org_id = 10
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


def _headers(idem_key=None):
    h = {"Authorization": "Bearer etf_test"}
    if idem_key:
        h["Idempotency-Key"] = idem_key
    return h


@contextlib.contextmanager
def _patch_auth(fake_api_key, rate_limit_ok, db_session):
    @contextlib.contextmanager
    def fake_session():
        yield db_session

    with (
        patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
        patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
        patch("datanika.services.api_middleware._get_session", fake_session),
    ):
        mock_svc.authenticate_api_key.return_value = fake_api_key
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = rate_limit_ok
        yield


def _create_run(session, org_id, status=RunStatus.PENDING):
    """Helper to seed a run directly in the DB."""
    run = Run(
        org_id=org_id,
        target_type=NodeType.UPLOAD,
        target_id=1,
        status=status,
    )
    session.add(run)
    session.flush()
    return run


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------


class TestCancelRun:
    def test_cancel_pending_run(self, client, fake_api_key, rate_limit_ok, db_session):
        with _patch_auth(fake_api_key, rate_limit_ok, db_session):
            run = _create_run(db_session, fake_api_key.org_id, RunStatus.PENDING)
            db_session.commit()
            resp = client.post(f"/api/v1/runs/{run.id}/cancel", headers=_headers())
            assert resp.status_code == 200
            assert resp.json()["status"] == "cancelled"

    def test_cancel_running_run(self, client, fake_api_key, rate_limit_ok, db_session):
        with _patch_auth(fake_api_key, rate_limit_ok, db_session):
            run = _create_run(db_session, fake_api_key.org_id, RunStatus.RUNNING)
            db_session.commit()
            resp = client.post(f"/api/v1/runs/{run.id}/cancel", headers=_headers())
            assert resp.status_code == 200
            assert resp.json()["status"] == "cancelled"

    def test_cancel_completed_run_returns_409(
        self, client, fake_api_key, rate_limit_ok, db_session
    ):
        with _patch_auth(fake_api_key, rate_limit_ok, db_session):
            run = _create_run(db_session, fake_api_key.org_id, RunStatus.SUCCESS)
            db_session.commit()
            resp = client.post(f"/api/v1/runs/{run.id}/cancel", headers=_headers())
            assert resp.status_code == 409
            assert resp.json()["error"]["code"] == "not_cancellable"

    def test_cancel_wrong_org_returns_404(self, client, fake_api_key, rate_limit_ok, db_session):
        with _patch_auth(fake_api_key, rate_limit_ok, db_session):
            run = _create_run(db_session, 999, RunStatus.PENDING)
            db_session.commit()
            resp = client.post(f"/api/v1/runs/{run.id}/cancel", headers=_headers())
            assert resp.status_code == 404

    def test_cancel_nonexistent_returns_404(self, client, fake_api_key, rate_limit_ok, db_session):
        with _patch_auth(fake_api_key, rate_limit_ok, db_session):
            resp = client.post("/api/v1/runs/99999/cancel", headers=_headers())
            assert resp.status_code == 404

    def test_cancel_requires_auth(self, client):
        resp = client.post("/api/v1/runs/1/cancel")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Idempotency-Key
# ---------------------------------------------------------------------------


class TestIdempotencyKey:
    def test_without_key_creates_normally(self, client, fake_api_key, rate_limit_ok, db_session):
        with _patch_auth(fake_api_key, rate_limit_ok, db_session):
            resp = client.post(
                "/api/v1/transformations",
                json={
                    "name": "test_model",
                    "sql_body": "SELECT 1",
                },
                headers=_headers(),
            )
            assert resp.status_code == 201

    def test_duplicate_key_returns_cached_response(
        self, client, fake_api_key, rate_limit_ok, db_session
    ):
        with (
            _patch_auth(fake_api_key, rate_limit_ok, db_session),
            patch("datanika.services.idempotency._redis") as mock_redis_fn,
        ):
            mock_r = MagicMock()
            mock_redis_fn.return_value = mock_r
            # First call: no cache
            mock_r.get.return_value = None
            resp1 = client.post(
                "/api/v1/transformations",
                json={
                    "name": "idem_model",
                    "sql_body": "SELECT 1",
                },
                headers=_headers(idem_key="key-123"),
            )
            assert resp1.status_code == 201
            # Verify response was cached
            assert mock_r.set.called

            # Second call: return cached
            cached_body = resp1.json()
            mock_r.get.return_value = json.dumps({"status": 201, "body": cached_body})
            resp2 = client.post(
                "/api/v1/transformations",
                json={
                    "name": "idem_model_duplicate",
                    "sql_body": "SELECT 2",
                },
                headers=_headers(idem_key="key-123"),
            )
            assert resp2.status_code == 201
            assert resp2.json() == cached_body

    def test_different_key_creates_new(self, client, fake_api_key, rate_limit_ok, db_session):
        with (
            _patch_auth(fake_api_key, rate_limit_ok, db_session),
            patch("datanika.services.idempotency._redis") as mock_redis_fn,
        ):
            mock_r = MagicMock()
            mock_redis_fn.return_value = mock_r
            mock_r.get.return_value = None

            resp1 = client.post(
                "/api/v1/transformations",
                json={
                    "name": "model_a",
                    "sql_body": "SELECT 1",
                },
                headers=_headers(idem_key="key-aaa"),
            )
            resp2 = client.post(
                "/api/v1/transformations",
                json={
                    "name": "model_b",
                    "sql_body": "SELECT 2",
                },
                headers=_headers(idem_key="key-bbb"),
            )
            assert resp1.status_code == 201
            assert resp2.status_code == 201
            assert resp1.json()["name"] != resp2.json()["name"]

    def test_get_requests_ignore_idempotency_key(
        self, client, fake_api_key, rate_limit_ok, db_session
    ):
        with (
            _patch_auth(fake_api_key, rate_limit_ok, db_session),
            patch("datanika.services.idempotency._redis") as mock_redis_fn,
        ):
            mock_r = MagicMock()
            mock_redis_fn.return_value = mock_r
            resp = client.get(
                "/api/v1/transformations",
                headers=_headers(idem_key="should-be-ignored"),
            )
            assert resp.status_code == 200
            mock_r.get.assert_not_called()

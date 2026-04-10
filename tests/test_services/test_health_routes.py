"""Tests for health check endpoints."""

from unittest.mock import patch, MagicMock

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services.health_routes import health_routes


@pytest.fixture
def client():
    app = Starlette(routes=health_routes)
    return TestClient(app)


class TestHealthz:
    def test_liveness_returns_200(self, client):
        resp = client.get("/healthz")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestReadyz:
    @patch("datanika.services.health_routes._engine")
    def test_readiness_all_ok(self, mock_engine, client):
        # Mock database check
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = lambda s: mock_conn
        mock_engine.connect.return_value.__exit__ = lambda s, *a: None

        # Mock redis
        with patch("redis.from_url") as mock_redis:
            mock_r = MagicMock()
            mock_redis.return_value = mock_r
            mock_r.ping.return_value = True

            resp = client.get("/readyz")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "ok"
            assert data["checks"]["database"] == "ok"
            assert data["checks"]["redis"] == "ok"

    @patch("datanika.services.health_routes._engine")
    def test_readiness_db_down(self, mock_engine, client):
        mock_engine.connect.side_effect = Exception("DB down")

        with patch("redis.from_url") as mock_redis:
            mock_r = MagicMock()
            mock_redis.return_value = mock_r
            mock_r.ping.return_value = True

            resp = client.get("/readyz")
            assert resp.status_code == 503
            data = resp.json()
            assert data["status"] == "degraded"
            assert data["checks"]["database"] == "error"
            assert data["checks"]["redis"] == "ok"

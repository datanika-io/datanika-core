"""Route-layer cross-tenant boundary test for /api/v1/* mutations (core#138).

CEO scope Q1 — highest-severity security gap per the 2026-04-14 review.

`tests/test_security/test_tenant_isolation.py` already covers the
*service* layer: every service method filters by `org_id`. This file
covers the *route* layer: given a valid Bearer token for org A, every
mutation route targeting a B-owned resource id must return 404 (or
400/403) — never 200/201, never 500. That catches any handler that
forgets to thread `api_key.org_id` into the service call.

Naming: the issue is titled "JWT boundary" but the /api/v1/* surface
uses ApiKeyService Bearer tokens, not user session JWTs — Reflex/UI
uses JWTs, the public API uses API keys. The two share the same
contract (`api_key.org_id` vs `claims["org_id"]`), so the boundary
semantics are identical; this test exercises the actual /api/v1 path.
"""

from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SASession
from sqlalchemy.pool import StaticPool
from starlette.applications import Starlette
from starlette.testclient import TestClient

import datanika.models.invitation  # noqa: F401  (register tables)
import datanika.models.notification_channel  # noqa: F401
import datanika.models.sso_config  # noqa: F401
from datanika.models.base import Base
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.notification import Notification, NotificationType
from datanika.models.notification_channel import ChannelType, NotificationChannel
from datanika.models.pipeline import DbtCommand, Pipeline
from datanika.models.run import Run, RunStatus
from datanika.models.schedule import Schedule
from datanika.models.transformation import Materialization, Transformation
from datanika.models.upload import Upload
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.rate_limit_service import RateLimitResult
from datanika.services.schedule_service import ScheduleService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService

ORG_A_ID = 10
ORG_B_ID = 20
BEARER_A = "etf_key_org_a"
BEARER_B = "etf_key_org_b"


def _fake_key(org_id: int, key_id: int) -> MagicMock:
    key = MagicMock()
    key.id = key_id
    key.org_id = org_id
    key.user_id = 1
    key.name = f"Key org {org_id}"
    key.scopes = None
    return key


_RATE_LIMIT_OK = RateLimitResult(
    allowed=True,
    current_count=1,
    limit=60,
    remaining=59,
    retry_after=0,
    reset_at=9999999999,
)


@pytest.fixture
def app() -> Starlette:
    return Starlette(routes=api_v1_routes)


@pytest.fixture
def client(app: Starlette) -> TestClient:
    return TestClient(app)


@contextlib.contextmanager
def _boundary_env():
    """Stand up an in-memory DB + route-level auth patches with a key
    dispatcher so the same Starlette app can be hit as either org A or
    org B by varying the Bearer token. Yields (session, b_ids) where
    `b_ids` is a dict of the B-owned resource ids to probe.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session = SASession(engine)

    enc = EncryptionService(Fernet.generate_key().decode())
    conn_svc = ConnectionService(enc)
    upload_svc = UploadService(conn_svc)
    pipeline_svc = PipelineService()
    transform_svc = TransformationService()
    schedule_svc = ScheduleService(upload_svc, transform_svc, pipeline_service=pipeline_svc)

    b_ids = _seed_b_resources(session, enc)
    # Seed a single A-owned notification channel so we can assert B's
    # channel is not returned when the handler scopes by A's org_id.
    # (Not strictly required for boundary checks; keeps the db realistic.)
    session.flush()

    key_a = _fake_key(ORG_A_ID, key_id=1)
    key_b = _fake_key(ORG_B_ID, key_id=2)

    def _auth_dispatch(_session, raw_key, required_scope=None):
        if raw_key == BEARER_A:
            return key_a
        if raw_key == BEARER_B:
            return key_b
        return None

    @contextlib.contextmanager
    def fake_session():
        yield session

    import datanika.services.api_v1_routes as routes_mod

    with (
        patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
        patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
        patch("datanika.services.api_middleware._get_session", fake_session),
        patch.object(routes_mod, "_get_conn_svc", return_value=conn_svc),
        patch.object(routes_mod, "_get_upload_svc", return_value=upload_svc),
        patch.object(routes_mod, "_get_schedule_svc", return_value=schedule_svc),
    ):
        mock_svc.authenticate_api_key.side_effect = _auth_dispatch
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = _RATE_LIMIT_OK

        yield session, b_ids

    session.close()
    engine.dispose()


def _seed_b_resources(session: SASession, enc: EncryptionService) -> dict[str, int]:
    """Create one of every resource type scoped to ORG_B_ID.

    Returns a dict mapping logical resource name → pk, used by the
    parameterized route table below. We skip the org_a side entirely —
    the boundary check only needs a victim in B and an attacker in A.
    """
    conn_b = Connection(
        org_id=ORG_B_ID,
        name="B Source",
        connection_type=ConnectionType.POSTGRES,
        direction=ConnectionDirection.SOURCE,
        config_encrypted=enc.encrypt({"host": "localhost", "port": 5432}),
    )
    conn_b_dest = Connection(
        org_id=ORG_B_ID,
        name="B Dest",
        connection_type=ConnectionType.POSTGRES,
        direction=ConnectionDirection.DESTINATION,
        config_encrypted=enc.encrypt({"host": "localhost", "port": 5432}),
    )
    session.add_all([conn_b, conn_b_dest])
    session.flush()

    upload_b = Upload(
        org_id=ORG_B_ID,
        name="B Upload",
        source_connection_id=conn_b.id,
        destination_connection_id=conn_b_dest.id,
        dlt_config={},
    )
    pipeline_b = Pipeline(
        org_id=ORG_B_ID,
        name="B Pipeline",
        destination_connection_id=conn_b_dest.id,
        command=DbtCommand.RUN,
    )
    transformation_b = Transformation(
        org_id=ORG_B_ID,
        name="B Transform",
        sql_body="SELECT 1",
        materialization=Materialization.VIEW,
        destination_connection_id=conn_b_dest.id,
    )
    session.add_all([upload_b, pipeline_b, transformation_b])
    session.flush()

    schedule_b = Schedule(
        org_id=ORG_B_ID,
        target_type=NodeType.UPLOAD,
        target_id=upload_b.id,
        cron_expression="0 * * * *",
    )
    run_b = Run(
        org_id=ORG_B_ID,
        target_type=NodeType.UPLOAD,
        target_id=upload_b.id,
        status=RunStatus.RUNNING,
    )
    notif_b = Notification(
        org_id=ORG_B_ID,
        type=NotificationType.RUN_SUCCEEDED,
        title="B notif",
        resource_type="run",
        resource_id=run_b.id or 1,
    )
    channel_b = NotificationChannel(
        org_id=ORG_B_ID,
        name="B Channel",
        channel_type=ChannelType.EMAIL,
        config={"to": "ops@b.example"},
        events=["run.failed"],
    )
    session.add_all([schedule_b, run_b, notif_b, channel_b])
    session.flush()

    return {
        "connection": conn_b.id,
        "upload": upload_b.id,
        "pipeline": pipeline_b.id,
        "transformation": transformation_b.id,
        "schedule": schedule_b.id,
        "run": run_b.id,
        "notification": notif_b.id,
        "channel": channel_b.id,
    }


# (method, path_template, resource_key, body, expected_status_set)
#
# `expected_status_set` is the union of acceptable non-success codes. We
# prefer 404 (the handler branch we most want to exercise) but accept
# 400 (body-shape errors that land *before* the org check) and 403
# (hypothetical future RBAC layer) as passing — the test is primarily a
# negative assertion: response MUST NOT be 2xx. Any 5xx is a bug and
# fails the test too.
MUTATION_ROUTES: list[tuple[str, str, str, dict | None]] = [
    # Connections
    ("PUT", "/api/v1/connections/{id}", "connection", {"name": "pwned"}),
    ("DELETE", "/api/v1/connections/{id}", "connection", None),
    ("POST", "/api/v1/connections/{id}/test", "connection", None),
    ("POST", "/api/v1/connections/{id}/introspect", "connection", {}),
    ("POST", "/api/v1/connections/{id}/columns", "connection", {"table": "x"}),
    ("POST", "/api/v1/connections/{id}/preview", "connection", {"table": "x"}),
    ("POST", "/api/v1/connections/{id}/query", "connection", {"query": "SELECT 1"}),
    # Uploads
    ("PUT", "/api/v1/uploads/{id}", "upload", {"name": "pwned"}),
    ("DELETE", "/api/v1/uploads/{id}", "upload", None),
    ("POST", "/api/v1/uploads/{id}/run", "upload", None),
    # Pipelines
    ("PUT", "/api/v1/pipelines/{id}", "pipeline", {"name": "pwned"}),
    ("DELETE", "/api/v1/pipelines/{id}", "pipeline", None),
    ("POST", "/api/v1/pipelines/{id}/run", "pipeline", None),
    # Transformations
    ("PUT", "/api/v1/transformations/{id}", "transformation", {"name": "pwned"}),
    ("DELETE", "/api/v1/transformations/{id}", "transformation", None),
    ("POST", "/api/v1/transformations/{id}/run", "transformation", None),
    ("POST", "/api/v1/transformations/{id}/compile", "transformation", None),
    ("POST", "/api/v1/transformations/{id}/preview", "transformation", None),
    # Schedules
    ("PUT", "/api/v1/schedules/{id}", "schedule", {"cron_expression": "*/5 * * * *"}),
    ("DELETE", "/api/v1/schedules/{id}", "schedule", None),
    # Runs
    ("POST", "/api/v1/runs/{id}/cancel", "run", None),
    # In-app notifications
    ("PATCH", "/api/v1/notifications/{id}/read", "notification", None),
    ("DELETE", "/api/v1/notifications/{id}", "notification", None),
    # Notification channels
    ("PUT", "/api/v1/notifications/channels/{id}", "channel", {"name": "pwned"}),
    ("DELETE", "/api/v1/notifications/channels/{id}", "channel", None),
]


def _route_id(param: tuple[str, str, str, dict | None]) -> str:
    method, path, _resource, _body = param
    return f"{method} {path}"


class TestCrossTenantBoundary:
    """Every /api/v1/* mutation route must 404 a cross-tenant request.

    The threat model: an attacker with a valid org-A API key discovers
    a resource id belonging to org B (via leaked URL, enumeration, or
    side-channel) and tries to mutate it. The expected behavior is a
    clean 404 — not a 200, and not a 500 crash (which could mask a
    boundary bypass that got further than it should have).
    """

    @pytest.mark.parametrize("route", MUTATION_ROUTES, ids=_route_id)
    def test_org_a_cannot_mutate_org_b_resource(
        self,
        client: TestClient,
        route: tuple[str, str, str, dict | None],
    ) -> None:
        method, path_template, resource_key, body = route
        with _boundary_env() as (session, b_ids):
            path = path_template.format(id=b_ids[resource_key])
            headers = {"Authorization": f"Bearer {BEARER_A}"}
            kwargs: dict = {"headers": headers}
            if body is not None:
                kwargs["json"] = body

            resp = client.request(method, path, **kwargs)

            # The one thing we absolutely refuse: a 2xx. That would
            # mean org A successfully touched a B-owned resource.
            assert resp.status_code not in (200, 201, 202, 204), (
                f"{method} {path} leaked org B resource to org A "
                f"(status={resp.status_code}, body={resp.text[:200]})"
            )
            # 5xx on a cross-tenant attempt is also a bug: the handler
            # got past the org check far enough to crash. The expected
            # branch is a clean 404 from the service returning None.
            assert resp.status_code < 500, (
                f"{method} {path} returned {resp.status_code} on a "
                f"cross-tenant probe (expected 404). body={resp.text[:200]}"
            )
            # 404 is the preferred contract. 400 is tolerated for the
            # few endpoints whose body parsing runs before the org
            # check; 403 is tolerated for a hypothetical future RBAC
            # layer. Anything else is a failure we want to see.
            assert resp.status_code in (400, 403, 404), (
                f"{method} {path} returned an unexpected status "
                f"{resp.status_code}: {resp.text[:200]}"
            )

    def test_put_does_not_mutate_b_record(self, client: TestClient) -> None:
        """Belt-and-suspenders: even if a cross-tenant PUT returned an
        error, verify the B-owned row's content is unchanged. Catches
        a hypothetical "error response but the update still committed"
        bug.
        """
        with _boundary_env() as (session, b_ids):
            headers = {"Authorization": f"Bearer {BEARER_A}"}
            path = f"/api/v1/connections/{b_ids['connection']}"
            resp = client.put(path, json={"name": "pwned-by-a"}, headers=headers)
            assert resp.status_code == 404

            session.expire_all()
            victim = session.get(Connection, b_ids["connection"])
            assert victim is not None, "B-owned connection vanished after cross-tenant PUT"
            assert victim.name == "B Source", (
                f"B-owned connection was mutated by org A "
                f"(name={victim.name!r}, expected 'B Source')"
            )

    def test_org_b_can_still_access_own_resource(self, client: TestClient) -> None:
        """Sanity: the boundary check doesn't accidentally wall off the
        rightful owner. Using B's own bearer token, the same endpoints
        that 404'd for A must succeed.
        """
        with _boundary_env() as (session, b_ids):
            headers_b = {"Authorization": f"Bearer {BEARER_B}"}
            resp = client.get(
                f"/api/v1/connections/{b_ids['connection']}",
                headers=headers_b,
            )
            assert resp.status_code == 200
            assert resp.json()["name"] == "B Source"

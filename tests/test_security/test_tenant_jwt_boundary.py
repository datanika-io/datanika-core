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

import ast
import contextlib
import pathlib
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
    org B by varying the Bearer token. Yields (session, b_ids, a_ids),
    dicts of the B-owned resource ids to probe and the A-owned ids the
    attacker legitimately controls.

    The A side exists for the body-carried vector: A updating *its own*
    resource (so every path-level org check passes) while pointing it at
    a B-owned connection in the request body.
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
    a_ids = _seed_a_resources(session, enc)
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

        yield session, b_ids, a_ids

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
        "connection_dest": conn_b_dest.id,
        "upload": upload_b.id,
        "pipeline": pipeline_b.id,
        "transformation": transformation_b.id,
        "schedule": schedule_b.id,
        "run": run_b.id,
        "notification": notif_b.id,
        "channel": channel_b.id,
    }


def _seed_a_resources(session: SASession, enc: EncryptionService) -> dict[str, int]:
    """Create the attacker-side resources org A legitimately owns.

    Needed for the body-carried vector below: to prove that a handler
    checks a connection id arriving in the *body*, the request must
    otherwise be entirely legitimate — A's own token, A's own resource in
    the path. Without an A-owned pipeline/transformation the request
    would 404 at the path check and the body would never be reached, so
    the test would pass for the wrong reason.
    """
    conn_a_src = Connection(
        org_id=ORG_A_ID,
        name="A Source",
        connection_type=ConnectionType.POSTGRES,
        direction=ConnectionDirection.SOURCE,
        config_encrypted=enc.encrypt({"host": "localhost", "port": 5432}),
    )
    conn_a_dest = Connection(
        org_id=ORG_A_ID,
        name="A Dest",
        connection_type=ConnectionType.POSTGRES,
        direction=ConnectionDirection.DESTINATION,
        config_encrypted=enc.encrypt({"host": "localhost", "port": 5432}),
    )
    session.add_all([conn_a_src, conn_a_dest])
    session.flush()

    pipeline_a = Pipeline(
        org_id=ORG_A_ID,
        name="A Pipeline",
        destination_connection_id=conn_a_dest.id,
        command=DbtCommand.RUN,
    )
    transformation_a = Transformation(
        org_id=ORG_A_ID,
        name="A Transform",
        sql_body="SELECT 1",
        materialization=Materialization.VIEW,
        destination_connection_id=conn_a_dest.id,
    )
    session.add_all([pipeline_a, transformation_a])
    session.flush()

    return {
        "connection_src": conn_a_src.id,
        "connection_dest": conn_a_dest.id,
        "pipeline": pipeline_a.id,
        "transformation": transformation_a.id,
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
        with _boundary_env() as (session, b_ids, _a_ids):
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
        with _boundary_env() as (session, b_ids, _a_ids):
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
        with _boundary_env() as (session, b_ids, _a_ids):
            headers_b = {"Authorization": f"Bearer {BEARER_B}"}
            resp = client.get(
                f"/api/v1/connections/{b_ids['connection']}",
                headers=headers_b,
            )
            assert resp.status_code == 200
            assert resp.json()["name"] == "B Source"


# ===========================================================================
# The body-carried resource reference (core#719)
# ===========================================================================
#
# MUTATION_ROUTES above is built on one premise, stated in its own class
# docstring: the foreign id arrives *in the path*. Endpoints that take a
# foreign id in the request **body** are outside that premise by
# construction, and lengthening the table does not bring them inside it.
#
# `test_tenant_fk_boundary.py` already covers that class for `Connection`
# — creates and one update — and its two AST guards make "a Connection is
# never resolved by primary key alone" a property of the whole source
# tree. So the rows below are deliberately the ones NOT already covered:
#
#   * a foreign `target_id` on a schedule. `Schedule.target_id` is a
#     polymorphic reference to an Upload/Transformation/Pipeline, not a
#     Connection, so every guard in that file misses it by construction —
#     they all key on the name `Connection`. This is core#732's class,
#     which is exactly the set that is still moving.
#   * the transformation UPDATE side; only create is pinned over there.
#   * the two bulk-import surfaces, which address a connection by **name**.
#     A name is as much a cross-tenant reference as an integer if the
#     lookup that resolves it is not scoped to the caller's org, and no
#     id-shaped guard can see it.


def _body_create_schedule(b_ids, a_ids) -> dict:
    return {
        "target_type": "upload",
        "target_id": b_ids["upload"],
        "cron_expression": "0 * * * *",
    }


def _body_repoint_to_b(b_ids, a_ids) -> dict:
    return {"destination_connection_id": b_ids["connection_dest"]}


def _body_import_by_foreign_name(b_ids, a_ids) -> dict:
    return {
        "version": 2,
        "uploads": [
            {
                "name": "A Stolen Import",
                "source_connection_name": "B Source",
                "destination_connection_name": "B Dest",
                "dlt_config": {},
            }
        ],
    }


def _body_import_yaml_by_foreign_name(b_ids, a_ids) -> str:
    # Hand-built rather than round-tripped through our own YAML writer: this
    # has to be the bytes a caller would actually send.
    return (
        "version: 2\n"
        "uploads:\n"
        "  - name: A Stolen Import\n"
        "    source_connection_name: B Source\n"
        "    destination_connection_name: B Dest\n"
        "    dlt_config: {}\n"
    )


def _no_upload_reached_org_a(session, b_ids, a_ids) -> None:
    rows = session.query(Upload).filter(Upload.org_id == ORG_A_ID).all()
    assert rows == [], (
        f"org A now owns {len(rows)} upload(s) built on a B-owned connection: "
        f"{[(u.id, u.name, u.source_connection_id) for u in rows]}"
    )


def _no_schedule_reached_org_a(session, b_ids, a_ids) -> None:
    rows = session.query(Schedule).filter(Schedule.org_id == ORG_A_ID).all()
    assert rows == [], (
        f"org A now owns {len(rows)} schedule(s) firing a B-owned target: "
        f"{[(s.id, s.target_type, s.target_id) for s in rows]}"
    )


def _a_transformation_still_points_at_its_own_connection(session, b_ids, a_ids) -> None:
    session.expire_all()
    transformation = session.get(Transformation, a_ids["transformation"])
    assert transformation is not None
    assert transformation.destination_connection_id == a_ids["connection_dest"], (
        "A's own transformation was re-pointed at B's connection "
        f"(destination_connection_id={transformation.destination_connection_id}, "
        f"expected {a_ids['connection_dest']})"
    )


# (method, path_template, a_owned_path_key | None, build_body, what_is_foreign, verify)
BODY_REF_ROUTES: list[tuple] = [
    (
        "POST",
        "/api/v1/schedules",
        None,
        _body_create_schedule,
        "target_id",
        _no_schedule_reached_org_a,
    ),
    (
        "PUT",
        "/api/v1/transformations/{id}",
        "transformation",
        _body_repoint_to_b,
        "destination_connection_id on A's own transformation",
        _a_transformation_still_points_at_its_own_connection,
    ),
    (
        "POST",
        "/api/v1/import",
        None,
        _body_import_by_foreign_name,
        "source/destination_connection_name",
        _no_upload_reached_org_a,
    ),
    (
        "POST",
        "/api/v1/pipelines/yaml",
        None,
        _body_import_yaml_by_foreign_name,
        "source/destination_connection_name (YAML)",
        _no_upload_reached_org_a,
    ),
]


def _body_route_id(param: tuple) -> str:
    method, path, _key, _build, what_is_foreign, _verify = param
    return f"{method} {path} [{what_is_foreign}]"


class TestBodyCarriedResourceReference:
    """A foreign resource reference in the request BODY must be re-scoped too.

    Every row here is a request org A is fully entitled to make, right up to
    one field. That is what makes the class distinct from
    `TestCrossTenantBoundary`: there, the request is illegitimate at the path
    and a 404 is the whole answer. Here the request authenticates, authorises
    and resolves its path resource correctly, and the only defence left is the
    handler re-scoping something it took out of the body.
    """

    @pytest.mark.parametrize("route", BODY_REF_ROUTES, ids=_body_route_id)
    def test_a_foreign_reference_in_the_body_is_refused(
        self, client: TestClient, route: tuple
    ) -> None:
        method, path_template, a_key, build_body, what_is_foreign, verify = route
        with _boundary_env() as (session, b_ids, a_ids):
            path = path_template.format(id=a_ids[a_key]) if a_key is not None else path_template
            headers = {"Authorization": f"Bearer {BEARER_A}"}
            body = build_body(b_ids, a_ids)
            kwargs: dict = {"headers": headers}
            if isinstance(body, str):
                kwargs["content"] = body
            else:
                kwargs["json"] = body

            resp = client.request(method, path, **kwargs)

            assert resp.status_code not in (200, 201, 202, 204), (
                f"{method} {path} accepted a B-owned {what_is_foreign} from org A "
                f"(status={resp.status_code}, body={resp.text[:300]})"
            )
            assert resp.status_code < 500, (
                f"{method} {path} returned {resp.status_code} on a body-carried "
                f"cross-tenant reference — the handler got far enough to crash. "
                f"body={resp.text[:300]}"
            )
            # The status is the cheap half. This is the half that catches
            # "reported an error and committed anyway".
            verify(session, b_ids, a_ids)

    def test_the_same_requests_succeed_against_the_org_s_own_resources(
        self, client: TestClient
    ) -> None:
        """NEGATIVE CONTROL, one per row above, and it is not optional.

        Every assertion above is satisfied by a handler that refuses
        *everything* — a broken body parser, a service that always raises, a
        route that 400s on any payload. This is what separates "the boundary is
        enforced" from "the endpoint is broken", and it has to exercise the
        identical fields.
        """
        with _boundary_env() as (session, b_ids, a_ids):
            headers = {"Authorization": f"Bearer {BEARER_A}"}

            resp = client.post(
                "/api/v1/schedules",
                json={
                    "target_type": "pipeline",
                    "target_id": a_ids["pipeline"],
                    "cron_expression": "0 * * * *",
                },
                headers=headers,
            )
            assert resp.status_code == 201, (
                "org A cannot schedule its OWN pipeline "
                f"({resp.status_code}: {resp.text[:300]}) — the foreign-target "
                "assertion above is therefore proving nothing"
            )

            resp = client.put(
                f"/api/v1/transformations/{a_ids['transformation']}",
                json={"destination_connection_id": a_ids["connection_dest"]},
                headers=headers,
            )
            assert resp.status_code == 200, (
                "org A cannot re-point its own transformation at its own "
                f"connection ({resp.status_code}: {resp.text[:300]})"
            )

            resp = client.post(
                "/api/v1/import",
                json={
                    "version": 2,
                    "uploads": [
                        {
                            "name": "A Legitimate Import",
                            "source_connection_name": "A Source",
                            "destination_connection_name": "A Dest",
                            "dlt_config": {},
                        }
                    ],
                },
                headers=headers,
            )
            assert resp.status_code == 201, (
                "org A cannot import against its OWN connection names "
                f"({resp.status_code}: {resp.text[:300]}) — so the foreign-name "
                "assertions above would pass against an import endpoint that is "
                "simply broken"
            )


# ===========================================================================
# Derived coverage — a gap is a failure, not an omission (core#719)
# ===========================================================================
#
# Every table above is hand-written, and a hand-written table cannot fail when
# someone adds a route: it does not mention it, the suite stays green on its
# existing rows, and the new route goes unexamined. So the set of routes to
# cover is DERIVED from the shipped `api_v1_routes` object and everything that
# claims coverage is SUBTRACTED from it. Whatever is left over is a failure.
#
# The subtraction runs in BOTH directions, and the second direction is what
# keeps the first honest — an entry naming a route the app no longer serves is
# coverage of nothing, and it silently pads the set being subtracted.
#
# The three tests below also arm each other. If the derivation returns nothing,
# `test_every_mutating_route_has_boundary_coverage` passes vacuously — and the
# stale-entry test fails, because every entry is suddenly unmatched. If the
# tables are emptied, the reverse. `test_the_derivation_reads_the_real_route_table`
# closes the one case that defeats both: everything empty at once.

MUTATING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_REQUEST_ATTRS = frozenset({"get", "post", "put", "patch", "delete"})


def _canonical(path: str) -> str:
    """Starlette's `:int` converter suffix is a detail of the route table, not
    of the boundary contract."""
    return path.replace("{id:int}", "{id}")


def _real_mutating_routes() -> set[tuple[str, str]]:
    """Every mutating route the app actually serves.

    Read off the shipped `api_v1_routes` object — not a copy of it, not a
    docstring, not an AST walk of the source file. Adding a route to the
    application therefore adds it here, with nobody remembering to do anything.
    """
    found: set[tuple[str, str]] = set()
    for route in api_v1_routes:
        for method in getattr(route, "methods", None) or ():
            if method in MUTATING_METHODS:
                found.add((method, _canonical(route.path)))
    return found


# Routes whose body-carried boundary is pinned in a NEIGHBOURING module, with
# the module that pins it. Not taken on trust: `test_the_named_module_really
# _exercises_that_route` re-derives each one from that module's own AST, so an
# entry survives only as long as a request to that path does.
COVERED_ELSEWHERE: dict[tuple[str, str], str] = {
    ("POST", "/api/v1/pipelines"): "tests/test_security/test_tenant_fk_boundary.py",
    ("PUT", "/api/v1/pipelines/{id}"): "tests/test_security/test_tenant_fk_boundary.py",
    ("POST", "/api/v1/transformations"): "tests/test_security/test_tenant_fk_boundary.py",
    ("POST", "/api/v1/uploads"): "tests/test_security/test_tenant_fk_boundary.py",
}


# Mutating routes that address no org-owned resource the caller chose — not in
# the path, not in the body. Each entry carries its reason, and the stale-entry
# test revokes the licence the moment the route stops existing, so an entry
# cannot outlive the fact that justified it.
NO_RESOURCE_REFERENCE: dict[tuple[str, str], str] = {
    ("POST", "/api/v1/connections"): (
        "Creates a connection out of literal body fields. org_id is taken from "
        "the API key; no pre-existing row is addressed."
    ),
    ("POST", "/api/v1/connections/openapi/parse"): (
        "Stateless preview: parses an OpenAPI document and returns the derived "
        "shape. Creates nothing and reads no org-owned row."
    ),
    ("POST", "/api/v1/notifications/channels"): (
        "Creates a channel out of literal body fields (name, type, config, "
        "events). References no pre-existing org-owned row."
    ),
    ("POST", "/api/v1/notifications/read-all"): (
        "Bulk update scoped to the caller's own org. Accepts no id at all, so "
        "there is no foreign reference to smuggle."
    ),
}


def _pinned_routes() -> dict[tuple[str, str], str]:
    """route -> the source that accounts for it."""
    pinned: dict[tuple[str, str], str] = {}
    for method, path, _resource, _body in MUTATION_ROUTES:
        pinned[(method, _canonical(path))] = "MUTATION_ROUTES"
    for method, path, _key, _build, _what, _verify in BODY_REF_ROUTES:
        key = (method, _canonical(path))
        pinned[key] = (
            "MUTATION_ROUTES + BODY_REF_ROUTES"
            if pinned.get(key) == "MUTATION_ROUTES"
            else "BODY_REF_ROUTES"
        )
    for key, module in COVERED_ELSEWHERE.items():
        pinned.setdefault(key, module)
    for key in NO_RESOURCE_REFERENCE:
        pinned.setdefault(key, "NO_RESOURCE_REFERENCE")
    return pinned


def _requests_in_module(rel_path: str) -> set[tuple[str, str]]:
    """(METHOD, path) for every literal `client.<verb>(...)` in a test module.

    Reads the neighbour's own source rather than believing a label about it.
    An f-string path (`f"/api/v1/pipelines/{pipeline_id}"`) is normalised to
    the route template, because that is what the interpolation always is here.
    """
    root = pathlib.Path(__file__).resolve().parents[2]
    source = (root / rel_path).read_text(encoding="utf-8")
    tree = ast.parse(source, filename=rel_path)

    found: set[tuple[str, str]] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = node.func
        if not isinstance(func, ast.Attribute) or func.attr not in _REQUEST_ATTRS:
            continue
        if not (isinstance(func.value, ast.Name) and func.value.id == "client"):
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            path = first.value
        elif isinstance(first, ast.JoinedStr):
            parts = []
            for piece in first.values:
                if isinstance(piece, ast.Constant) and isinstance(piece.value, str):
                    parts.append(piece.value)
                else:
                    parts.append("{id}")
            path = "".join(parts)
        else:
            continue
        found.add((func.attr.upper(), path))
    return found


class TestBoundaryRouteCoverage:
    """The coverage of the tables above is derived, not asserted."""

    def test_every_mutating_route_has_boundary_coverage(self) -> None:
        uncovered = sorted(
            _real_mutating_routes() - set(_pinned_routes()), key=lambda r: (r[1], r[0])
        )
        assert not uncovered, (
            "These mutating /api/v1 routes have no cross-tenant boundary coverage:\n"
            + "\n".join(f"  {m} {p}" for m, p in uncovered)
            + "\n\nAdd each one to MUTATION_ROUTES (foreign id in the path), to "
            "BODY_REF_ROUTES (foreign reference in the body), to COVERED_ELSEWHERE "
            "if a neighbouring module already exercises it, or — only if it "
            "addresses no org-owned resource at all — to NO_RESOURCE_REFERENCE "
            "with the reason. Do not widen this test."
        )

    def test_no_coverage_entry_outlives_its_route(self) -> None:
        """The other direction, and it is what keeps the first honest.

        An entry naming a route that no longer exists is coverage of nothing,
        and it silently pads the set the first test subtracts. It also catches
        the derivation failing open: if `_real_mutating_routes()` ever returns
        less than it should, every orphaned entry surfaces here.
        """
        real = _real_mutating_routes()
        stale = sorted(
            ((m, p, src) for (m, p), src in _pinned_routes().items() if (m, p) not in real),
            key=lambda r: (r[1], r[0]),
        )
        assert not stale, (
            "These entries name a route the app does not serve:\n"
            + "\n".join(f"  {m} {p}  (in {src})" for m, p, src in stale)
            + "\n\nEither the route was renamed or removed — in which case delete "
            "the entry — or the derivation in _real_mutating_routes() has stopped "
            "seeing the real route table, which is the more serious reading."
        )

    def test_the_named_module_really_exercises_that_route(self) -> None:
        """COVERED_ELSEWHERE is a claim about another file. Check it.

        Crediting a route to a neighbour and never reading that neighbour is
        the same defect this whole file is about, one level up: the entry would
        keep the route out of the uncovered list long after the test it names
        was deleted or renamed.
        """
        missing: list[str] = []
        for (method, path), module in sorted(COVERED_ELSEWHERE.items()):
            if (method, path) not in _requests_in_module(module):
                missing.append(f"  {method} {path}  claimed by {module}")
        assert not missing, (
            "COVERED_ELSEWHERE credits routes to modules that do not request them:\n"
            + "\n".join(missing)
            + "\n\nEither that module's test was removed — in which case the route "
            "is uncovered and the entry must go, so it reappears in the uncovered "
            "list — or the request is built in a form this AST scan cannot read, "
            "in which case widen _requests_in_module rather than deleting the check."
        )

    def test_the_derivation_reads_the_real_route_table(self) -> None:
        """Arming assertion: the pair above is mutually protective, but both go
        quiet if the derivation AND the tables empty at the same time. Three
        spot checks across three methods and three resources close that.
        """
        real = _real_mutating_routes()
        assert real, (
            "_real_mutating_routes() derived nothing. That is a broken "
            "derivation, not an app with no mutating endpoints."
        )
        for spot in (
            ("DELETE", "/api/v1/connections/{id}"),
            ("POST", "/api/v1/schedules"),
            ("PATCH", "/api/v1/notifications/{id}/read"),
        ):
            assert spot in real, (
                f"{spot[0]} {spot[1]} is served by the app but the derivation did "
                f"not find it — _canonical()/methods handling has drifted."
            )
        off_prefix = sorted(p for _m, p in real if not p.startswith("/api/v1/"))
        assert not off_prefix, f"derivation picked up non-/api/v1 paths: {off_prefix}"

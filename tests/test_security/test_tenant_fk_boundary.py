"""Cross-tenant boundary for foreign keys arriving in a *request body*.

Companion to `test_tenant_jwt_boundary.py`, which pins routes where org B's id
arrives in the **path**. That file states its own threat model — "an attacker
with a valid org-A API key discovers a resource id belonging to org B ... and
tries to mutate it" — so a *create* whose body carries B's foreign key sits
outside its `MUTATION_ROUTES` table **by construction, not by oversight**.
Adding rows there cannot reach this class, which is why this file exists
instead of more rows in that one.

Both halves of the trust decision are pinned here, deliberately:

  1. creation must refuse a foreign key that is not the caller's, and
  2. the consumer must refuse to resolve one that is *already stored*.

Either half alone leaves the other as the next incident. A create-side-only fix
still trusts every row written before it shipped; a consumer-side-only fix lets
the database keep accumulating cross-org references for some future reader to
resolve. core#679 was closed on one side and the takeover survived on the other.

The `own connection still works` cases are not filler — they are the negative
control. A "reject everything" implementation satisfies every rejection
assertion in this file and would ship a total outage of pipeline creation.
"""

from __future__ import annotations

import ast
import pathlib

import pytest
from cryptography.fernet import Fernet
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.pipeline import DbtCommand, Pipeline
from datanika.models.run import RunStatus
from datanika.models.upload import Upload
from datanika.models.user import Organization
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from tests.test_security.test_tenant_jwt_boundary import (
    BEARER_A,
    ORG_A_ID,
    _boundary_env,
)

# `app`/`client` are redefined rather than imported: importing a fixture and
# then naming it as a test parameter is an F811 redefinition on every test.
# They are two lines each, and the part worth sharing — `_boundary_env` — is
# imported above so the two files cannot drift on what the environment is.


@pytest.fixture
def app() -> Starlette:
    return Starlette(routes=api_v1_routes)


@pytest.fixture
def client(app: Starlette) -> TestClient:
    return TestClient(app)


@pytest.fixture
def env():
    with _boundary_env() as (session, b_ids):
        yield session, b_ids


def _auth_a() -> dict[str, str]:
    return {"Authorization": f"Bearer {BEARER_A}"}


def _own_connection(session) -> int:
    """An A-owned destination connection.

    `_seed_b_resources` deliberately seeds only the B side, so the negative
    controls have to bring their own A-owned row.
    """
    conn = Connection(
        org_id=ORG_A_ID,
        name="A Dest",
        connection_type=ConnectionType.POSTGRES,
        direction=ConnectionDirection.DESTINATION,
        config_encrypted=EncryptionService(Fernet.generate_key().decode()).encrypt(
            {"host": "localhost", "port": 5432}
        ),
    )
    session.add(conn)
    session.flush()
    return conn.id


class TestCreateRejectsForeignConnection:
    """Org A submits org B's connection id in a create/update body."""

    def test_pipeline_create_rejects_org_b_connection(self, client, env) -> None:
        _session, b_ids = env
        resp = client.post(
            "/api/v1/pipelines",
            headers=_auth_a(),
            json={
                "name": "a-pipeline-pointing-at-b",
                "destination_connection_id": b_ids["connection"],
            },
        )
        assert resp.status_code != 201, (
            f"org A created a pipeline bound to org B's connection "
            f"{b_ids['connection']}; body={resp.text}"
        )

    def test_pipeline_update_rejects_org_b_connection(self, client, env) -> None:
        session, b_ids = env
        own = _own_connection(session)
        created = client.post(
            "/api/v1/pipelines",
            headers=_auth_a(),
            json={"name": "a-pipeline-legit", "destination_connection_id": own},
        )
        assert created.status_code == 201, created.text
        pipeline_id = created.json()["id"]

        resp = client.put(
            f"/api/v1/pipelines/{pipeline_id}",
            headers=_auth_a(),
            json={"destination_connection_id": b_ids["connection"]},
        )
        assert resp.status_code not in (200, 201), (
            f"org A repointed its own pipeline at org B's connection "
            f"{b_ids['connection']} via update; body={resp.text}"
        )
        stored = session.get(Pipeline, pipeline_id)
        session.refresh(stored)
        assert stored.destination_connection_id == own, (
            "update persisted a cross-org destination even though it reported failure"
        )

    def test_transformation_create_rejects_org_b_connection(self, client, env) -> None:
        _session, b_ids = env
        resp = client.post(
            "/api/v1/transformations",
            headers=_auth_a(),
            json={
                "name": "a_model_pointing_at_b",
                "sql_body": "SELECT 1 AS x",
                "destination_connection_id": b_ids["connection"],
            },
        )
        assert resp.status_code != 201, (
            f"org A created a transformation bound to org B's connection "
            f"{b_ids['connection']}; body={resp.text}"
        )

    def test_upload_create_rejects_org_b_connection(self, client, env) -> None:
        """CONTROL — uploads already validated before this fix.

        Kept because it is what attributes a red run to the defect rather than
        to a harness that never reached a handler.
        """
        _session, b_ids = env
        resp = client.post(
            "/api/v1/uploads",
            headers=_auth_a(),
            json={
                "name": "a-upload-pointing-at-b",
                "source_connection_id": b_ids["connection"],
                "destination_connection_id": b_ids["connection"],
            },
        )
        assert resp.status_code != 201, (
            f"org A created an upload bound to org B's connection; body={resp.text}"
        )


class TestCreateStillAcceptsOwnConnection:
    """NEGATIVE CONTROL — the org's own connection must keep working.

    Without these, "reject every foreign key" and "reject every foreign key
    plus every legitimate one" are indistinguishable.
    """

    def test_pipeline_create_accepts_own_connection(self, client, env) -> None:
        session, _b_ids = env
        own = _own_connection(session)
        resp = client.post(
            "/api/v1/pipelines",
            headers=_auth_a(),
            json={"name": "a-pipeline-own", "destination_connection_id": own},
        )
        assert resp.status_code == 201, (
            f"the fix broke ordinary pipeline creation against the org's own "
            f"connection {own}; body={resp.text}"
        )

    def test_transformation_create_accepts_own_connection(self, client, env) -> None:
        session, _b_ids = env
        own = _own_connection(session)
        resp = client.post(
            "/api/v1/transformations",
            headers=_auth_a(),
            json={
                "name": "a_model_own",
                "sql_body": "SELECT 1 AS x",
                "destination_connection_id": own,
            },
        )
        assert resp.status_code == 201, (
            f"the fix broke transformation creation against the org's own "
            f"connection {own}; body={resp.text}"
        )

    def test_transformation_create_accepts_no_connection(self, client, env) -> None:
        """`destination_connection_id` is optional on transformations."""
        _session, _b_ids = env
        resp = client.post(
            "/api/v1/transformations",
            headers=_auth_a(),
            json={"name": "a_model_no_conn", "sql_body": "SELECT 1 AS x"},
        )
        assert resp.status_code == 201, (
            f"the fix broke transformation creation with no destination "
            f"connection at all; body={resp.text}"
        )


class TestConsumerRefusesStoredForeignConnection:
    """The second half: a cross-org row that is *already in the database*.

    Creation is closed above, so these seed the bad row directly — which is
    exactly the state a database is in for rows written before the create-side
    fix shipped. If this class were written through the API it would silently
    skip, and the consumer would be guarded by a test that never runs.
    """

    def test_run_pipeline_does_not_decrypt_foreign_connection(self, db_session) -> None:
        from unittest.mock import patch

        from datanika.tasks.pipeline_tasks import run_pipeline

        enc = EncryptionService(Fernet.generate_key().decode())
        org_a = Organization(name="A", slug="fk-a")
        org_b = Organization(name="B", slug="fk-b")
        db_session.add_all([org_a, org_b])
        db_session.flush()

        victim = Connection(
            org_id=org_b.id,
            name="B Warehouse",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted=enc.encrypt(
                {"host": "b-secret-host", "port": 5432, "password": "b-secret-password"}
            ),
        )
        db_session.add(victim)
        db_session.flush()

        pipeline = Pipeline(
            org_id=org_a.id,
            name="attacker_pipeline",
            destination_connection_id=victim.id,
            command=DbtCommand.RUN,
            models=[],
        )
        db_session.add(pipeline)
        db_session.flush()

        run = ExecutionService().create_run(db_session, org_a.id, NodeType.PIPELINE, pipeline.id)

        with patch("datanika.tasks.pipeline_tasks.DbtProjectService") as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 0,
                "logs": "",
                "raw_result": [],
            }
            run_pipeline(
                run_id=run.id,
                org_id=org_a.id,
                session=db_session,
                encryption=enc,
            )

        db_session.refresh(run)
        assert run.status == RunStatus.FAILED, (
            "a pipeline pointing at another org's connection ran to completion"
        )
        instance.generate_profiles_yml.assert_not_called()

    def test_run_upload_does_not_decrypt_foreign_connections(self, db_session) -> None:
        from datanika.services.connection_service import get_org_connection

        enc = EncryptionService(Fernet.generate_key().decode())
        org_a = Organization(name="A2", slug="fk-a2")
        org_b = Organization(name="B2", slug="fk-b2")
        db_session.add_all([org_a, org_b])
        db_session.flush()

        victim = Connection(
            org_id=org_b.id,
            name="B Source",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.SOURCE,
            config_encrypted=enc.encrypt({"host": "b-host", "password": "b-pass"}),
        )
        db_session.add(victim)
        db_session.flush()

        upload = Upload(
            org_id=org_a.id,
            name="attacker_upload",
            source_connection_id=victim.id,
            destination_connection_id=victim.id,
            dlt_config={},
        )
        db_session.add(upload)
        db_session.flush()

        # Both FKs on the upload path must resolve org-scoped — the source one
        # as much as the destination. Fixing only the destination would leave
        # the source decrypting another org's credentials.
        assert get_org_connection(db_session, upload.org_id, upload.source_connection_id) is None
        assert (
            get_org_connection(db_session, upload.org_id, upload.destination_connection_id) is None
        )


class TestSoftDeletedConnectionFailsClosed:
    """A deliberate behaviour change that rides along with the fix, pinned here.

    `ConnectionService.delete_connection` soft-deletes with no in-use check, so
    a pipeline can outlive its destination connection. The bare
    `session.get(Connection, id)` this fix removes did **not** filter
    `deleted_at`, so such a run used to proceed and decrypt the credentials of a
    connection the user believes they deleted. The org-scoped accessor filters
    it, so the run now fails instead.

    That is the better behaviour, but it is a change beyond the security
    property, and an untested behaviour change is indistinguishable from an
    accident when a run starts failing in production. Hence this test.
    """

    def test_run_pipeline_fails_on_soft_deleted_own_connection(self, db_session) -> None:
        from datetime import UTC, datetime
        from unittest.mock import patch

        from datanika.tasks.pipeline_tasks import run_pipeline

        enc = EncryptionService(Fernet.generate_key().decode())
        org = Organization(name="C", slug="fk-c")
        db_session.add(org)
        db_session.flush()

        conn = Connection(
            org_id=org.id,
            name="Deleted Dest",
            connection_type=ConnectionType.POSTGRES,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted=enc.encrypt({"host": "h", "port": 5432}),
            deleted_at=datetime.now(UTC),
        )
        db_session.add(conn)
        db_session.flush()

        pipeline = Pipeline(
            org_id=org.id,
            name="orphaned_pipeline",
            destination_connection_id=conn.id,
            command=DbtCommand.RUN,
            models=[],
        )
        db_session.add(pipeline)
        db_session.flush()

        run = ExecutionService().create_run(db_session, org.id, NodeType.PIPELINE, pipeline.id)

        with patch("datanika.tasks.pipeline_tasks.DbtProjectService") as mock_dbt_cls:
            instance = mock_dbt_cls.return_value
            instance.run_command.return_value = {
                "success": True,
                "rows_affected": 0,
                "logs": "",
                "raw_result": [],
            }
            run_pipeline(run_id=run.id, org_id=org.id, session=db_session, encryption=enc)

        db_session.refresh(run)
        assert run.status == RunStatus.FAILED
        instance.generate_profiles_yml.assert_not_called()


class TestNoBarePrimaryKeyConnectionLookup:
    """The durable, class-level guard.

    A route-table drift check would not have caught this defect and adding one
    would not prevent the next: the table's shape excludes body-carried foreign
    keys however long it gets. What generalises is the invariant that a
    `Connection` is never resolved by primary key alone — every lookup goes
    through an org-scoped accessor, so a caller cannot reach another tenant's
    row even when it holds a valid id.
    """

    def test_no_session_get_connection_in_source(self) -> None:
        root = pathlib.Path(__file__).resolve().parents[2] / "datanika"
        offenders: list[str] = []
        for path in sorted(root.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or not node.args:
                    continue
                func = node.func
                if not isinstance(func, ast.Attribute) or func.attr != "get":
                    continue
                # Both spellings: a bare `Connection` and a qualified
                # `models.Connection` / `connection.Connection`. Matching only
                # the bare name would let the qualified form reintroduce the
                # defect while this test stayed green.
                first = node.args[0]
                name = None
                if isinstance(first, ast.Name):
                    name = first.id
                elif isinstance(first, ast.Attribute):
                    name = first.attr
                if name == "Connection":
                    rel = path.relative_to(root.parent).as_posix()
                    offenders.append(f"{rel}:{node.lineno}")

        assert offenders == [], (
            "Connection resolved by primary key with no org filter at "
            + ", ".join(offenders)
            + " — use datanika.services.connection_service.get_org_connection(session, "
            "org_id, conn_id), which is the single definition of 'this org owns "
            "this connection'. A bare primary-key lookup will happily return "
            "another tenant's row."
        )

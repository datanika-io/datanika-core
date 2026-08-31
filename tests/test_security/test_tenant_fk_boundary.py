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


# Query-construction forms that resolve a `Connection`. `session.get` is the one
# that shipped the defect; the other two are the spellings the rest of this
# codebase actually uses, which is why matching only `get` guards the bug rather
# than the class.
_SELECT_FUNCS = {"select"}
_QUERY_ATTRS = {"query"}


def _names_connection(node: ast.expr) -> bool:
    """True for `Connection` and for the qualified `models.Connection` form."""
    if isinstance(node, ast.Name):
        return node.id == "Connection"
    if isinstance(node, ast.Attribute):
        return node.attr == "Connection"
    return False


def _is_connection_query(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name) and func.id in _SELECT_FUNCS:
        return any(_names_connection(a) for a in node.args)
    if isinstance(func, ast.Attribute):
        if func.attr in _QUERY_ATTRS:
            return any(_names_connection(a) for a in node.args)
        if func.attr == "get" and node.args:
            return _names_connection(node.args[0])
    return False


def _constrains_org(stmt: ast.stmt) -> bool:
    """Does this statement mention org scoping at all?

    Deliberately generous — `Connection.org_id == org_id`, `filter_by(org_id=…)`
    and `where(Connection.org_id.in_(…))` all count. The guard's job is to force
    the question to be asked in the statement that builds the query, not to
    typecheck the predicate.
    """
    for node in ast.walk(stmt):
        if isinstance(node, ast.Attribute) and node.attr == "org_id":
            return True
        if isinstance(node, ast.keyword) and node.arg == "org_id":
            return True
    return False


def _enclosing(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    return parents


def _climb(node: ast.AST, parents: dict[ast.AST, ast.AST], kinds: tuple) -> ast.AST | None:
    cur = parents.get(node)
    while cur is not None:
        if isinstance(cur, kinds):
            return cur
        cur = parents.get(cur)
    return None


class TestEveryConnectionQueryIsOrgScoped:
    """The same invariant as the class above, at the width of the class.

    `TestNoBarePrimaryKeyConnectionLookup` pins `session.get(Connection, id)` —
    the exact call that shipped the defect. Measured 2026-08-31 by planting a
    synthetic module under `datanika/`: that check stays **green** on all four
    of these, while a `session.get` in the same file turns it red, so the green
    was coverage absence rather than a walker that never read the file:

        select(Connection).where(Connection.id == conn_id)      # ← the idiom
        session.query(Connection).get(conn_id)                  #   this repo
        session.scalars(select(Connection).filter_by(id=cid))   #   uses
        session.query(Connection).filter(Connection.id == cid).first()

    The first is how `get_org_connection`, `list_connections` and
    `export_backup` are all written, so the *most likely* way this defect comes
    back is in a form the narrower check cannot see. Hence: any statement that
    builds a `Connection` query must constrain `org_id` in that same statement.

    Empty allowlist on purpose. A genuinely cross-org query is a real thing to
    want one day — a support console, a platform-wide migration — and when it
    is, adding its `module.py::function` here is an edit to a security test that
    someone reviews, rather than a comment nobody reads.
    """

    CROSS_ORG_ALLOWLIST: frozenset[str] = frozenset()

    def test_connection_queries_constrain_org_id(self) -> None:
        root = pathlib.Path(__file__).resolve().parents[2] / "datanika"
        offenders: list[str] = []
        for path in sorted(root.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            parents = _enclosing(tree)
            for node in ast.walk(tree):
                if not _is_connection_query(node):
                    continue
                stmt = _climb(node, parents, (ast.stmt,))
                if stmt is None or _constrains_org(stmt):
                    continue
                func = _climb(node, parents, (ast.FunctionDef, ast.AsyncFunctionDef))
                fname = func.name if func is not None else "<module>"
                rel = path.relative_to(root.parent).as_posix()
                if f"{path.name}::{fname}" in self.CROSS_ORG_ALLOWLIST:
                    continue
                offenders.append(f"{rel}:{node.lineno} (in {fname})")

        assert offenders == [], (
            "Connection query with no org_id constraint at "
            + ", ".join(offenders)
            + " — every read of a Connection must be scoped to the caller's org "
            "in the statement that builds it, because connection ids are small "
            "sequential integers and an id that arrived in a request body is "
            "another tenant's id until proven otherwise. Use "
            "datanika.services.connection_service.get_org_connection(session, "
            "org_id, conn_id). If the query is deliberately cross-org, add "
            "'<module>.py::<function>' to CROSS_ORG_ALLOWLIST above so the "
            "exemption is reviewed rather than assumed."
        )

    def test_connection_relationship_is_not_traversed_outside_models(self) -> None:
        """The ORM route the query guard above cannot see.

        `Pipeline.destination_connection`, `Upload.source_connection` and
        `Upload.destination_connection` are declared relationships. Reading one
        lazy-loads by foreign key with **no** org filter — the identical
        cross-tenant read as the bare primary-key lookup, in an expression that
        contains no query at all. Nothing traverses them today (measured), and
        `get_org_connection(session, org_id, pipeline.destination_connection_id)`
        sits one autocomplete away from `pipeline.destination_connection`.
        """
        root = pathlib.Path(__file__).resolve().parents[2] / "datanika"
        rel_names = {"destination_connection", "source_connection"}
        offenders: list[str] = []
        for path in sorted(root.rglob("*.py")):
            rel = path.relative_to(root.parent).as_posix()
            if rel.startswith("datanika/models/"):
                continue  # where the relationships are declared
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and node.attr in rel_names:
                    offenders.append(f"{rel}:{node.lineno} (.{node.attr})")

        assert offenders == [], (
            "Connection reached through an ORM relationship at "
            + ", ".join(offenders)
            + " — a relationship load follows the foreign key with no org "
            "filter, so it resolves another tenant's row for any cross-org or "
            "soft-deleted reference already stored. Read the *_connection_id "
            "column and pass it to get_org_connection(session, org_id, id)."
        )

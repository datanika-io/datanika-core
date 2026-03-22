"""Security tests — cross-org data isolation for all entity types."""

import uuid

import pytest

from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus
from datanika.models.upload import Upload
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.upload_service import UploadService


@pytest.fixture
def encryption():
    return EncryptionService("NS7W71uT0X-FxSq_mwJfthZzc3hIatYjQHM3MhDnQX8=")


@pytest.fixture
def org_a(db_session):
    o = Organization(name="Org A", slug=f"org-a-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def org_b(db_session):
    o = Organization(name="Org B", slug=f"org-b-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def conn_a(db_session, org_a, encryption):
    c = Connection(
        org_id=org_a.id,
        name="Conn A",
        connection_type=ConnectionType.POSTGRES,
        direction=ConnectionDirection.SOURCE,
        config_encrypted=encryption.encrypt({"host": "localhost"}),
    )
    db_session.add(c)
    db_session.flush()
    return c


@pytest.fixture
def conn_b(db_session, org_b, encryption):
    c = Connection(
        org_id=org_b.id,
        name="Conn B",
        connection_type=ConnectionType.POSTGRES,
        direction=ConnectionDirection.SOURCE,
        config_encrypted=encryption.encrypt({"host": "localhost"}),
    )
    db_session.add(c)
    db_session.flush()
    return c


class TestConnectionIsolation:
    def test_list_connections_filtered_by_org(self, db_session, org_a, org_b, conn_a, conn_b,
                                              encryption):
        svc = ConnectionService(encryption)
        a_conns = svc.list_connections(db_session, org_a.id)
        b_conns = svc.list_connections(db_session, org_b.id)
        a_ids = {c.id for c in a_conns}
        b_ids = {c.id for c in b_conns}
        assert conn_a.id in a_ids
        assert conn_b.id not in a_ids
        assert conn_b.id in b_ids
        assert conn_a.id not in b_ids

    def test_get_connection_wrong_org_returns_none(self, db_session, org_a, org_b, conn_a,
                                                    encryption):
        svc = ConnectionService(encryption)
        result = svc.get_connection(db_session, org_b.id, conn_a.id)
        assert result is None


class TestUploadIsolation:
    def test_list_uploads_filtered_by_org(self, db_session, org_a, org_b, conn_a, conn_b,
                                           encryption):
        conn_svc = ConnectionService(encryption)
        svc = UploadService(conn_svc)
        # Create uploads in each org
        u_a = Upload(
            org_id=org_a.id, name="Upload A", source_connection_id=conn_a.id,
            destination_connection_id=conn_a.id, dlt_config={},
        )
        u_b = Upload(
            org_id=org_b.id, name="Upload B", source_connection_id=conn_b.id,
            destination_connection_id=conn_b.id, dlt_config={},
        )
        db_session.add_all([u_a, u_b])
        db_session.flush()

        a_list = svc.list_uploads(db_session, org_a.id)
        assert any(u.id == u_a.id for u in a_list)
        assert not any(u.id == u_b.id for u in a_list)


class TestRunIsolation:
    def test_list_runs_filtered_by_org(self, db_session, org_a, org_b):
        svc = ExecutionService()
        r_a = Run(org_id=org_a.id, target_type=NodeType.UPLOAD, target_id=1,
                   status=RunStatus.SUCCESS)
        r_b = Run(org_id=org_b.id, target_type=NodeType.UPLOAD, target_id=1,
                   status=RunStatus.SUCCESS)
        db_session.add_all([r_a, r_b])
        db_session.flush()

        a_runs = svc.list_runs(db_session, org_a.id)
        assert any(r.id == r_a.id for r in a_runs)
        assert not any(r.id == r_b.id for r in a_runs)

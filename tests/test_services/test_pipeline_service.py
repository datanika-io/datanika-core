"""TDD tests for pipeline service."""

import pytest
from cryptography.fernet import Fernet

from etlfabric.models.connection import ConnectionDirection, ConnectionType
from etlfabric.models.pipeline import Pipeline, PipelineStatus
from etlfabric.models.user import Organization
from etlfabric.services.connection_service import ConnectionService
from etlfabric.services.encryption import EncryptionService
from etlfabric.services.pipeline_service import PipelineConfigError, PipelineService


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def svc(conn_svc):
    return PipelineService(conn_svc)


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-pipe-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def other_org(db_session):
    org = Organization(name="OtherCo", slug="other-pipe-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def source_conn(conn_svc, db_session, org):
    return conn_svc.create_connection(
        db_session,
        org.id,
        "Source DB",
        ConnectionType.POSTGRES,
        ConnectionDirection.SOURCE,
        {"host": "src"},
    )


@pytest.fixture
def dest_conn(conn_svc, db_session, org):
    return conn_svc.create_connection(
        db_session,
        org.id,
        "Dest DB",
        ConnectionType.POSTGRES,
        ConnectionDirection.DESTINATION,
        {"host": "dst"},
    )


@pytest.fixture
def both_conn(conn_svc, db_session, org):
    return conn_svc.create_connection(
        db_session,
        org.id,
        "Both DB",
        ConnectionType.POSTGRES,
        ConnectionDirection.BOTH,
        {"host": "both"},
    )


class TestCreatePipeline:
    def test_basic(self, svc, db_session, org, source_conn, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "my_pipe",
            "desc",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert isinstance(pipe, Pipeline)
        assert isinstance(pipe.id, int)
        assert pipe.name == "my_pipe"
        assert pipe.status == PipelineStatus.DRAFT

    def test_with_config(self, svc, db_session, org, source_conn, dest_conn):
        config = {"write_disposition": "append"}
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            config,
        )
        assert pipe.dlt_config == config

    def test_invalid_source_connection(self, svc, db_session, org, dest_conn):
        with pytest.raises(ValueError, match="source"):
            svc.create_pipeline(
                db_session,
                org.id,
                "p",
                "d",
                99999,
                dest_conn.id,
                {},
            )

    def test_wrong_direction_source(self, svc, db_session, org, dest_conn):
        """A DESTINATION-only connection cannot be used as source."""
        with pytest.raises(ValueError, match="source"):
            svc.create_pipeline(
                db_session,
                org.id,
                "p",
                "d",
                dest_conn.id,
                dest_conn.id,
                {},
            )

    def test_wrong_direction_dest(self, svc, db_session, org, source_conn):
        """A SOURCE-only connection cannot be used as destination."""
        with pytest.raises(ValueError, match="destination"):
            svc.create_pipeline(
                db_session,
                org.id,
                "p",
                "d",
                source_conn.id,
                source_conn.id,
                {},
            )

    def test_both_direction_works_as_source(self, svc, db_session, org, both_conn, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            both_conn.id,
            dest_conn.id,
            {},
        )
        assert pipe.source_connection_id == both_conn.id

    def test_both_direction_works_as_dest(self, svc, db_session, org, source_conn, both_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            both_conn.id,
            {},
        )
        assert pipe.destination_connection_id == both_conn.id


class TestGetPipeline:
    def test_existing(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        fetched = svc.get_pipeline(db_session, org.id, created.id)
        assert fetched is not None
        assert fetched.id == created.id

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_pipeline(db_session, org.id, 99999) is None

    def test_wrong_org(self, svc, db_session, org, other_org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert svc.get_pipeline(db_session, other_org.id, created.id) is None

    def test_soft_deleted(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.delete_pipeline(db_session, org.id, created.id)
        assert svc.get_pipeline(db_session, org.id, created.id) is None


class TestListPipelines:
    def test_empty(self, svc, db_session, org):
        assert svc.list_pipelines(db_session, org.id) == []

    def test_multiple(self, svc, db_session, org, source_conn, dest_conn):
        svc.create_pipeline(
            db_session,
            org.id,
            "A",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.create_pipeline(
            db_session,
            org.id,
            "B",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert len(svc.list_pipelines(db_session, org.id)) == 2

    def test_excludes_deleted(self, svc, db_session, org, source_conn, dest_conn):
        p1 = svc.create_pipeline(
            db_session,
            org.id,
            "A",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.create_pipeline(
            db_session,
            org.id,
            "B",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        svc.delete_pipeline(db_session, org.id, p1.id)
        result = svc.list_pipelines(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "B"

    def test_filters_by_org(
        self, svc, conn_svc, db_session, org, other_org, source_conn, dest_conn
    ):
        svc.create_pipeline(
            db_session,
            org.id,
            "A",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        # Create connections for other org
        other_src = conn_svc.create_connection(
            db_session,
            other_org.id,
            "S2",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        other_dst = conn_svc.create_connection(
            db_session,
            other_org.id,
            "D2",
            ConnectionType.POSTGRES,
            ConnectionDirection.DESTINATION,
            {},
        )
        svc.create_pipeline(
            db_session,
            other_org.id,
            "B",
            "d",
            other_src.id,
            other_dst.id,
            {},
        )
        result = svc.list_pipelines(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "A"


class TestUpdatePipeline:
    def test_update_name(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "Old",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        updated = svc.update_pipeline(db_session, org.id, created.id, name="New")
        assert updated is not None
        assert updated.name == "New"

    def test_update_status(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        updated = svc.update_pipeline(
            db_session,
            org.id,
            created.id,
            status=PipelineStatus.ACTIVE,
        )
        assert updated.status == PipelineStatus.ACTIVE

    def test_nonexistent(self, svc, db_session, org):
        assert svc.update_pipeline(db_session, org.id, 99999, name="X") is None

    def test_config_re_validates(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        with pytest.raises(PipelineConfigError):
            svc.update_pipeline(
                db_session,
                org.id,
                created.id,
                dlt_config={"write_disposition": "invalid"},
            )


class TestDeletePipeline:
    def test_sets_deleted_at(self, svc, db_session, org, source_conn, dest_conn):
        created = svc.create_pipeline(
            db_session,
            org.id,
            "p",
            "d",
            source_conn.id,
            dest_conn.id,
            {},
        )
        assert svc.delete_pipeline(db_session, org.id, created.id) is True
        db_session.refresh(created)
        assert created.deleted_at is not None

    def test_nonexistent(self, svc, db_session, org):
        assert svc.delete_pipeline(db_session, org.id, 99999) is False


class TestValidatePipelineConfig:
    def test_append_valid(self):
        PipelineService.validate_pipeline_config({"write_disposition": "append"})

    def test_replace_valid(self):
        PipelineService.validate_pipeline_config({"write_disposition": "replace"})

    def test_merge_with_pk_valid(self):
        PipelineService.validate_pipeline_config(
            {"write_disposition": "merge", "primary_key": "id"}
        )

    def test_invalid_disposition(self):
        with pytest.raises(PipelineConfigError, match="write_disposition"):
            PipelineService.validate_pipeline_config({"write_disposition": "invalid"})

    def test_merge_without_pk(self):
        with pytest.raises(PipelineConfigError, match="primary_key"):
            PipelineService.validate_pipeline_config({"write_disposition": "merge"})

    def test_empty_config_valid(self):
        PipelineService.validate_pipeline_config({})

    def test_non_dict_raises(self):
        with pytest.raises(PipelineConfigError):
            PipelineService.validate_pipeline_config("not a dict")

"""TDD tests for dbt pipeline service — CRUD + selector builder."""

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionDirection, ConnectionType
from datanika.models.pipeline import DbtCommand, Pipeline, PipelineStatus
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.pipeline_service import PipelineConfigError, PipelineService


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def svc():
    return PipelineService()


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
def dest_conn(conn_svc, db_session, org):
    return conn_svc.create_connection(
        db_session,
        org.id,
        "Dest DB",
        ConnectionType.POSTGRES,
        ConnectionDirection.DESTINATION,
        {"host": "localhost"},
    )


class TestCreatePipeline:
    def test_creates_draft_pipeline(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "My Pipeline",
            "desc",
            dest_conn.id,
            DbtCommand.RUN,
        )
        assert isinstance(pipe, Pipeline)
        assert isinstance(pipe.id, int)
        assert pipe.name == "My Pipeline"
        assert pipe.description == "desc"
        assert pipe.destination_connection_id == dest_conn.id
        assert pipe.command == DbtCommand.RUN
        assert pipe.status == PipelineStatus.DRAFT
        assert pipe.full_refresh is False
        assert pipe.models == []
        assert pipe.custom_selector is None
        assert pipe.org_id == org.id

    def test_creates_with_models(self, svc, db_session, org, dest_conn):
        models = [
            {"name": "orders", "upstream": True, "downstream": False},
            {"name": "customers", "upstream": False, "downstream": True},
        ]
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "Pipe",
            None,
            dest_conn.id,
            DbtCommand.BUILD,
            models=models,
        )
        assert pipe.models == models
        assert pipe.command == DbtCommand.BUILD

    def test_creates_with_full_refresh(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "FR",
            None,
            dest_conn.id,
            DbtCommand.RUN,
            full_refresh=True,
        )
        assert pipe.full_refresh is True

    def test_creates_with_custom_selector(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "CS",
            None,
            dest_conn.id,
            DbtCommand.RUN,
            custom_selector="tag:nightly",
        )
        assert pipe.custom_selector == "tag:nightly"

    def test_empty_name_rejected(self, svc, db_session, org, dest_conn):
        with pytest.raises(PipelineConfigError, match="name"):
            svc.create_pipeline(
                db_session,
                org.id,
                "",
                None,
                dest_conn.id,
                DbtCommand.RUN,
            )

    def test_whitespace_name_rejected(self, svc, db_session, org, dest_conn):
        with pytest.raises(PipelineConfigError, match="name"):
            svc.create_pipeline(
                db_session,
                org.id,
                "   ",
                None,
                dest_conn.id,
                DbtCommand.RUN,
            )

    def test_invalid_models_rejected(self, svc, db_session, org, dest_conn):
        with pytest.raises(PipelineConfigError, match="name"):
            svc.create_pipeline(
                db_session,
                org.id,
                "Bad",
                None,
                dest_conn.id,
                DbtCommand.RUN,
                models=[{"bad": "format"}],
            )


class TestGetPipeline:
    def test_existing(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "P",
            None,
            dest_conn.id,
            DbtCommand.RUN,
        )
        found = svc.get_pipeline(db_session, org.id, pipe.id)
        assert found is not None
        assert found.id == pipe.id

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_pipeline(db_session, org.id, 99999) is None

    def test_wrong_org(self, svc, db_session, org, other_org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "P",
            None,
            dest_conn.id,
            DbtCommand.RUN,
        )
        assert svc.get_pipeline(db_session, other_org.id, pipe.id) is None

    def test_soft_deleted_excluded(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "P",
            None,
            dest_conn.id,
            DbtCommand.RUN,
        )
        svc.delete_pipeline(db_session, org.id, pipe.id)
        assert svc.get_pipeline(db_session, org.id, pipe.id) is None


class TestListPipelines:
    def test_empty(self, svc, db_session, org):
        assert svc.list_pipelines(db_session, org.id) == []

    def test_multiple(self, svc, db_session, org, dest_conn):
        svc.create_pipeline(db_session, org.id, "A", None, dest_conn.id, DbtCommand.RUN)
        svc.create_pipeline(db_session, org.id, "B", None, dest_conn.id, DbtCommand.BUILD)
        assert len(svc.list_pipelines(db_session, org.id)) == 2

    def test_excludes_deleted(self, svc, db_session, org, dest_conn):
        p1 = svc.create_pipeline(db_session, org.id, "A", None, dest_conn.id, DbtCommand.RUN)
        svc.create_pipeline(db_session, org.id, "B", None, dest_conn.id, DbtCommand.BUILD)
        svc.delete_pipeline(db_session, org.id, p1.id)
        result = svc.list_pipelines(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "B"

    def test_filters_by_org(self, svc, db_session, org, other_org, dest_conn):
        svc.create_pipeline(db_session, org.id, "A", None, dest_conn.id, DbtCommand.RUN)
        assert svc.list_pipelines(db_session, other_org.id) == []


class TestUpdatePipeline:
    def test_updates_name(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "Old",
            None,
            dest_conn.id,
            DbtCommand.RUN,
        )
        updated = svc.update_pipeline(db_session, org.id, pipe.id, name="New")
        assert updated is not None
        assert updated.name == "New"

    def test_updates_command(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "P",
            None,
            dest_conn.id,
            DbtCommand.RUN,
        )
        updated = svc.update_pipeline(db_session, org.id, pipe.id, command=DbtCommand.BUILD)
        assert updated.command == DbtCommand.BUILD

    def test_updates_models(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "P",
            None,
            dest_conn.id,
            DbtCommand.RUN,
        )
        models = [{"name": "orders", "upstream": False, "downstream": False}]
        updated = svc.update_pipeline(db_session, org.id, pipe.id, models=models)
        assert updated.models == models

    def test_updates_full_refresh(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "P",
            None,
            dest_conn.id,
            DbtCommand.RUN,
        )
        updated = svc.update_pipeline(db_session, org.id, pipe.id, full_refresh=True)
        assert updated.full_refresh is True

    def test_updates_status(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "P",
            None,
            dest_conn.id,
            DbtCommand.RUN,
        )
        updated = svc.update_pipeline(
            db_session,
            org.id,
            pipe.id,
            status=PipelineStatus.ACTIVE,
        )
        assert updated.status == PipelineStatus.ACTIVE

    def test_nonexistent(self, svc, db_session, org):
        assert svc.update_pipeline(db_session, org.id, 99999, name="X") is None

    def test_invalid_models_rejected(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "P",
            None,
            dest_conn.id,
            DbtCommand.RUN,
        )
        with pytest.raises(PipelineConfigError, match="models"):
            svc.update_pipeline(db_session, org.id, pipe.id, models="not a list")


class TestDeletePipeline:
    def test_soft_deletes(self, svc, db_session, org, dest_conn):
        pipe = svc.create_pipeline(
            db_session,
            org.id,
            "P",
            None,
            dest_conn.id,
            DbtCommand.RUN,
        )
        assert svc.delete_pipeline(db_session, org.id, pipe.id) is True
        db_session.refresh(pipe)
        assert pipe.deleted_at is not None

    def test_nonexistent(self, svc, db_session, org):
        assert svc.delete_pipeline(db_session, org.id, 99999) is False


class TestBuildSelector:
    def test_single_model(self):
        result = PipelineService.build_selector(
            [{"name": "orders", "upstream": False, "downstream": False}],
            None,
        )
        assert result == "orders"

    def test_model_with_upstream(self):
        result = PipelineService.build_selector(
            [{"name": "orders", "upstream": True, "downstream": False}],
            None,
        )
        assert result == "+orders"

    def test_model_with_downstream(self):
        result = PipelineService.build_selector(
            [{"name": "orders", "upstream": False, "downstream": True}],
            None,
        )
        assert result == "orders+"

    def test_model_with_both(self):
        result = PipelineService.build_selector(
            [{"name": "orders", "upstream": True, "downstream": True}],
            None,
        )
        assert result == "+orders+"

    def test_multiple_models(self):
        result = PipelineService.build_selector(
            [
                {"name": "orders", "upstream": True, "downstream": False},
                {"name": "customers", "upstream": False, "downstream": True},
            ],
            None,
        )
        assert result == "+orders customers+"

    def test_custom_selector_overrides(self):
        result = PipelineService.build_selector(
            [{"name": "orders", "upstream": True, "downstream": False}],
            "tag:nightly",
        )
        assert result == "tag:nightly"

    def test_empty_models_no_custom(self):
        result = PipelineService.build_selector([], None)
        assert result is None

    def test_custom_selector_whitespace_treated_as_empty(self):
        result = PipelineService.build_selector([], "   ")
        assert result is None


class TestValidateModels:
    def test_valid_models(self):
        PipelineService.validate_models(
            [
                {"name": "orders", "upstream": True, "downstream": False},
            ]
        )

    def test_not_a_list_rejected(self):
        with pytest.raises(PipelineConfigError, match="models must be a list"):
            PipelineService.validate_models("not a list")

    def test_missing_name_rejected(self):
        with pytest.raises(PipelineConfigError, match="name"):
            PipelineService.validate_models([{"upstream": True, "downstream": False}])

    def test_empty_name_rejected(self):
        with pytest.raises(PipelineConfigError, match="name"):
            PipelineService.validate_models(
                [
                    {"name": "", "upstream": True, "downstream": False},
                ]
            )

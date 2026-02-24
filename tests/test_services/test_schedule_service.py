"""TDD tests for schedule management service."""

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.schedule import Schedule
from datanika.models.transformation import Materialization
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.schedule_service import ScheduleConfigError, ScheduleService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def upload_svc(conn_svc):
    return UploadService(conn_svc)


@pytest.fixture
def transform_svc():
    return TransformationService()


@pytest.fixture
def svc(upload_svc, transform_svc):
    return ScheduleService(upload_svc, transform_svc)


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-sched-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def other_org(db_session):
    org = Organization(name="OtherCo", slug="other-sched-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def upload(upload_svc, conn_svc, db_session, org):
    src = conn_svc.create_connection(
        db_session, org.id, "Src", ConnectionType.POSTGRES, ConnectionDirection.SOURCE, {"h": "x"}
    )
    dst = conn_svc.create_connection(
        db_session,
        org.id,
        "Dst",
        ConnectionType.POSTGRES,
        ConnectionDirection.DESTINATION,
        {"h": "y"},
    )
    return upload_svc.create_upload(db_session, org.id, "pipe", "desc", src.id, dst.id, {})


@pytest.fixture
def transformation(transform_svc, db_session, org):
    return transform_svc.create_transformation(
        db_session, org.id, "model", "SELECT 1", Materialization.VIEW
    )


class TestCreateSchedule:
    def test_upload_schedule(self, svc, db_session, org, upload):
        s = svc.create_schedule(db_session, org.id, NodeType.UPLOAD, upload.id, "0 * * * *")
        assert isinstance(s, Schedule)
        assert isinstance(s.id, int)
        assert s.target_type == NodeType.UPLOAD
        assert s.target_id == upload.id
        assert s.cron_expression == "0 * * * *"
        assert s.timezone == "UTC"
        assert s.is_active is True

    def test_transformation_schedule(self, svc, db_session, org, transformation):
        s = svc.create_schedule(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id, "30 2 * * 1"
        )
        assert s.target_type == NodeType.TRANSFORMATION
        assert s.target_id == transformation.id

    def test_invalid_cron(self, svc, db_session, org, upload):
        with pytest.raises(ScheduleConfigError, match="cron"):
            svc.create_schedule(db_session, org.id, NodeType.UPLOAD, upload.id, "bad")

    def test_nonexistent_upload(self, svc, db_session, org):
        with pytest.raises(ScheduleConfigError, match="target"):
            svc.create_schedule(db_session, org.id, NodeType.UPLOAD, 99999, "0 * * * *")

    def test_nonexistent_transformation(self, svc, db_session, org):
        with pytest.raises(ScheduleConfigError, match="target"):
            svc.create_schedule(db_session, org.id, NodeType.TRANSFORMATION, 99999, "0 * * * *")


class TestGetSchedule:
    def test_existing(self, svc, db_session, org, upload):
        created = svc.create_schedule(
            db_session, org.id, NodeType.UPLOAD, upload.id, "0 * * * *"
        )
        fetched = svc.get_schedule(db_session, org.id, created.id)
        assert fetched is not None
        assert fetched.id == created.id

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_schedule(db_session, org.id, 99999) is None

    def test_wrong_org(self, svc, db_session, org, other_org, upload):
        created = svc.create_schedule(
            db_session, org.id, NodeType.UPLOAD, upload.id, "0 * * * *"
        )
        assert svc.get_schedule(db_session, other_org.id, created.id) is None

    def test_soft_deleted(self, svc, db_session, org, upload):
        created = svc.create_schedule(
            db_session, org.id, NodeType.UPLOAD, upload.id, "0 * * * *"
        )
        svc.delete_schedule(db_session, org.id, created.id)
        assert svc.get_schedule(db_session, org.id, created.id) is None


class TestListSchedules:
    def test_empty(self, svc, db_session, org):
        result = svc.list_schedules(db_session, org.id)
        assert result == []

    def test_multiple(self, svc, db_session, org, upload, transformation):
        svc.create_schedule(db_session, org.id, NodeType.UPLOAD, upload.id, "0 * * * *")
        svc.create_schedule(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id, "30 2 * * 1"
        )
        result = svc.list_schedules(db_session, org.id)
        assert len(result) == 2

    def test_excludes_deleted(self, svc, db_session, org, upload, transformation):
        created = svc.create_schedule(
            db_session, org.id, NodeType.UPLOAD, upload.id, "0 * * * *"
        )
        svc.create_schedule(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id, "30 2 * * 1"
        )
        svc.delete_schedule(db_session, org.id, created.id)
        result = svc.list_schedules(db_session, org.id)
        assert len(result) == 1

    def test_filters_by_org(self, svc, db_session, org, other_org, upload):
        svc.create_schedule(db_session, org.id, NodeType.UPLOAD, upload.id, "0 * * * *")
        # Create upload in other org for a schedule there
        result = svc.list_schedules(db_session, other_org.id)
        assert result == []


class TestUpdateSchedule:
    def test_update_cron(self, svc, db_session, org, upload):
        created = svc.create_schedule(
            db_session, org.id, NodeType.UPLOAD, upload.id, "0 * * * *"
        )
        updated = svc.update_schedule(db_session, org.id, created.id, cron_expression="30 2 * * *")
        assert updated is not None
        assert updated.cron_expression == "30 2 * * *"

    def test_update_timezone(self, svc, db_session, org, upload):
        created = svc.create_schedule(
            db_session, org.id, NodeType.UPLOAD, upload.id, "0 * * * *"
        )
        updated = svc.update_schedule(db_session, org.id, created.id, timezone="US/Eastern")
        assert updated.timezone == "US/Eastern"

    def test_nonexistent(self, svc, db_session, org):
        assert svc.update_schedule(db_session, org.id, 99999, timezone="UTC") is None

    def test_invalid_cron_rejected(self, svc, db_session, org, upload):
        created = svc.create_schedule(
            db_session, org.id, NodeType.UPLOAD, upload.id, "0 * * * *"
        )
        with pytest.raises(ScheduleConfigError, match="cron"):
            svc.update_schedule(db_session, org.id, created.id, cron_expression="bad")


class TestDeleteSchedule:
    def test_sets_deleted_at(self, svc, db_session, org, upload):
        created = svc.create_schedule(
            db_session, org.id, NodeType.UPLOAD, upload.id, "0 * * * *"
        )
        result = svc.delete_schedule(db_session, org.id, created.id)
        assert result is True
        db_session.refresh(created)
        assert created.deleted_at is not None

    def test_nonexistent(self, svc, db_session, org):
        result = svc.delete_schedule(db_session, org.id, 99999)
        assert result is False


class TestToggleActive:
    def test_active_to_inactive(self, svc, db_session, org, upload):
        created = svc.create_schedule(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            "0 * * * *",
            is_active=True,
        )
        toggled = svc.toggle_active(db_session, org.id, created.id)
        assert toggled is not None
        assert toggled.is_active is False

    def test_inactive_to_active(self, svc, db_session, org, upload):
        created = svc.create_schedule(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            "0 * * * *",
            is_active=False,
        )
        toggled = svc.toggle_active(db_session, org.id, created.id)
        assert toggled is not None
        assert toggled.is_active is True

    def test_nonexistent(self, svc, db_session, org):
        assert svc.toggle_active(db_session, org.id, 99999) is None


class TestValidateCron:
    def test_valid_5_field(self):
        ScheduleService.validate_cron_expression("0 * * * *")

    def test_empty_rejected(self):
        with pytest.raises(ScheduleConfigError, match="cron"):
            ScheduleService.validate_cron_expression("")

    def test_3_fields_rejected(self):
        with pytest.raises(ScheduleConfigError, match="cron"):
            ScheduleService.validate_cron_expression("0 * *")

    def test_6_fields_rejected(self):
        with pytest.raises(ScheduleConfigError, match="cron"):
            ScheduleService.validate_cron_expression("0 * * * * *")

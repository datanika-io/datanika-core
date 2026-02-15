"""TDD tests for AuditService — action logging and querying."""

import pytest

from etlfabric.models.audit_log import AuditAction, AuditLog
from etlfabric.models.user import Organization, User
from etlfabric.services.audit_service import AuditService


@pytest.fixture
def svc():
    return AuditService()


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-audit-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def user(db_session):
    user = User(
        email="audit-user@example.com",
        password_hash="hashed",
        full_name="Audit User",
    )
    db_session.add(user)
    db_session.flush()
    return user


class TestLogAction:
    def test_basic(self, svc, db_session, org, user):
        log = svc.log_action(
            db_session,
            org_id=org.id,
            user_id=user.id,
            action=AuditAction.CREATE,
            resource_type="pipeline",
            resource_id=42,
        )
        assert isinstance(log, AuditLog)
        assert isinstance(log.id, int)
        assert log.org_id == org.id
        assert log.user_id == user.id
        assert log.action == AuditAction.CREATE
        assert log.resource_type == "pipeline"
        assert log.resource_id == 42

    def test_with_values_and_ip(self, svc, db_session, org, user):
        log = svc.log_action(
            db_session,
            org_id=org.id,
            user_id=user.id,
            action=AuditAction.UPDATE,
            resource_type="connection",
            resource_id=5,
            old_values={"name": "old"},
            new_values={"name": "new"},
            ip_address="192.168.1.1",
        )
        assert log.old_values == {"name": "old"}
        assert log.new_values == {"name": "new"}
        assert log.ip_address == "192.168.1.1"

    def test_null_user_for_system_actions(self, svc, db_session, org):
        log = svc.log_action(
            db_session,
            org_id=org.id,
            user_id=None,
            action=AuditAction.RUN,
            resource_type="schedule",
            resource_id=1,
        )
        assert log.user_id is None

    def test_all_action_types(self, svc, db_session, org, user):
        for action in AuditAction:
            log = svc.log_action(
                db_session,
                org_id=org.id,
                user_id=user.id,
                action=action,
                resource_type="test",
            )
            assert log.action == action


class TestListLogs:
    def test_empty(self, svc, db_session, org):
        assert svc.list_logs(db_session, org.id) == []

    def test_multiple(self, svc, db_session, org, user):
        svc.log_action(db_session, org.id, user.id, AuditAction.CREATE, "pipeline")
        svc.log_action(db_session, org.id, user.id, AuditAction.DELETE, "connection")
        result = svc.list_logs(db_session, org.id)
        assert len(result) == 2

    def test_filters_by_org(self, svc, db_session, org, user):
        other_org = Organization(name="Other", slug="other-audit")
        db_session.add(other_org)
        db_session.flush()
        svc.log_action(db_session, org.id, user.id, AuditAction.CREATE, "pipeline")
        svc.log_action(db_session, other_org.id, user.id, AuditAction.DELETE, "pipeline")
        result = svc.list_logs(db_session, org.id)
        assert len(result) == 1

    def test_filter_by_action(self, svc, db_session, org, user):
        svc.log_action(db_session, org.id, user.id, AuditAction.CREATE, "pipeline")
        svc.log_action(db_session, org.id, user.id, AuditAction.DELETE, "pipeline")
        result = svc.list_logs(db_session, org.id, action=AuditAction.CREATE)
        assert len(result) == 1
        assert result[0].action == AuditAction.CREATE

    def test_filter_by_resource_type(self, svc, db_session, org, user):
        svc.log_action(db_session, org.id, user.id, AuditAction.CREATE, "pipeline")
        svc.log_action(db_session, org.id, user.id, AuditAction.CREATE, "connection")
        result = svc.list_logs(db_session, org.id, resource_type="pipeline")
        assert len(result) == 1
        assert result[0].resource_type == "pipeline"

    def test_filter_by_user(self, svc, db_session, org, user):
        other_user = User(
            email="other-audit@example.com",
            password_hash="h",
            full_name="Other",
        )
        db_session.add(other_user)
        db_session.flush()
        svc.log_action(db_session, org.id, user.id, AuditAction.CREATE, "pipeline")
        svc.log_action(db_session, org.id, other_user.id, AuditAction.CREATE, "pipeline")
        result = svc.list_logs(db_session, org.id, user_id=user.id)
        assert len(result) == 1

    def test_limit(self, svc, db_session, org, user):
        for i in range(5):
            svc.log_action(db_session, org.id, user.id, AuditAction.CREATE, f"res_{i}")
        result = svc.list_logs(db_session, org.id, limit=3)
        assert len(result) == 3

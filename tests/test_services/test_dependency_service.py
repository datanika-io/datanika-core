"""TDD tests for dependency management service."""

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionDirection, ConnectionType
from datanika.models.dependency import Dependency, NodeType
from datanika.models.transformation import Materialization
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.dependency_service import DependencyConfigError, DependencyService
from datanika.services.encryption import EncryptionService
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
    return DependencyService(upload_svc, transform_svc)


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-dep-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def other_org(db_session):
    org = Organization(name="OtherCo", slug="other-dep-svc")
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
def upload2(upload_svc, conn_svc, db_session, org):
    src = conn_svc.create_connection(
        db_session,
        org.id,
        "Src2",
        ConnectionType.POSTGRES,
        ConnectionDirection.SOURCE,
        {"h": "x"},
    )
    dst = conn_svc.create_connection(
        db_session,
        org.id,
        "Dst2",
        ConnectionType.POSTGRES,
        ConnectionDirection.DESTINATION,
        {"h": "y"},
    )
    return upload_svc.create_upload(db_session, org.id, "pipe2", "desc", src.id, dst.id, {})


@pytest.fixture
def transformation(transform_svc, db_session, org):
    return transform_svc.create_transformation(
        db_session, org.id, "model_a", "SELECT 1", Materialization.VIEW
    )


@pytest.fixture
def transformation2(transform_svc, db_session, org):
    return transform_svc.create_transformation(
        db_session, org.id, "model_b", "SELECT 2", Materialization.TABLE
    )


class TestAddDependency:
    def test_upload_to_transformation(self, svc, db_session, org, upload, transformation):
        dep = svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        assert isinstance(dep, Dependency)
        assert isinstance(dep.id, int)
        assert dep.upstream_type == NodeType.UPLOAD
        assert dep.upstream_id == upload.id
        assert dep.downstream_type == NodeType.TRANSFORMATION
        assert dep.downstream_id == transformation.id
        assert dep.org_id == org.id

    def test_transformation_to_transformation(
        self, svc, db_session, org, transformation, transformation2
    ):
        dep = svc.add_dependency(
            db_session,
            org.id,
            NodeType.TRANSFORMATION,
            transformation.id,
            NodeType.TRANSFORMATION,
            transformation2.id,
        )
        assert dep.upstream_type == NodeType.TRANSFORMATION
        assert dep.downstream_type == NodeType.TRANSFORMATION

    def test_upload_to_upload(self, svc, db_session, org, upload, upload2):
        dep = svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.UPLOAD,
            upload2.id,
        )
        assert dep.upstream_type == NodeType.UPLOAD
        assert dep.downstream_type == NodeType.UPLOAD

    def test_self_reference_rejected(self, svc, db_session, org, upload):
        with pytest.raises(DependencyConfigError, match="self-reference"):
            svc.add_dependency(
                db_session,
                org.id,
                NodeType.UPLOAD,
                upload.id,
                NodeType.UPLOAD,
                upload.id,
            )

    def test_duplicate_rejected(self, svc, db_session, org, upload, transformation):
        svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        with pytest.raises(DependencyConfigError, match="already exists"):
            svc.add_dependency(
                db_session,
                org.id,
                NodeType.UPLOAD,
                upload.id,
                NodeType.TRANSFORMATION,
                transformation.id,
            )

    def test_nonexistent_upstream_rejected(self, svc, db_session, org, transformation):
        with pytest.raises(DependencyConfigError, match="upstream"):
            svc.add_dependency(
                db_session,
                org.id,
                NodeType.UPLOAD,
                99999,
                NodeType.TRANSFORMATION,
                transformation.id,
            )

    def test_nonexistent_downstream_rejected(self, svc, db_session, org, upload):
        with pytest.raises(DependencyConfigError, match="downstream"):
            svc.add_dependency(
                db_session,
                org.id,
                NodeType.UPLOAD,
                upload.id,
                NodeType.TRANSFORMATION,
                99999,
            )


class TestAddDependencyTimeframe:
    def test_with_valid_timeframe(self, svc, db_session, org, upload, transformation):
        dep = svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload.id,
            NodeType.TRANSFORMATION, transformation.id,
            check_timeframe_value=30,
            check_timeframe_unit="minutes",
        )
        assert dep.check_timeframe_value == 30
        assert dep.check_timeframe_unit == "minutes"

    def test_with_hours_unit(self, svc, db_session, org, upload, transformation):
        dep = svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload.id,
            NodeType.TRANSFORMATION, transformation.id,
            check_timeframe_value=2,
            check_timeframe_unit="hours",
        )
        assert dep.check_timeframe_value == 2
        assert dep.check_timeframe_unit == "hours"

    def test_none_timeframe_is_metadata_only(self, svc, db_session, org, upload, transformation):
        dep = svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload.id,
            NodeType.TRANSFORMATION, transformation.id,
        )
        assert dep.check_timeframe_value is None
        assert dep.check_timeframe_unit is None

    def test_negative_value_rejected(self, svc, db_session, org, upload, transformation):
        with pytest.raises(DependencyConfigError, match="positive"):
            svc.add_dependency(
                db_session, org.id,
                NodeType.UPLOAD, upload.id,
                NodeType.TRANSFORMATION, transformation.id,
                check_timeframe_value=-5,
                check_timeframe_unit="minutes",
            )

    def test_zero_value_rejected(self, svc, db_session, org, upload, transformation):
        with pytest.raises(DependencyConfigError, match="positive"):
            svc.add_dependency(
                db_session, org.id,
                NodeType.UPLOAD, upload.id,
                NodeType.TRANSFORMATION, transformation.id,
                check_timeframe_value=0,
                check_timeframe_unit="minutes",
            )

    def test_invalid_unit_rejected(self, svc, db_session, org, upload, transformation):
        with pytest.raises(DependencyConfigError, match="check_timeframe_unit"):
            svc.add_dependency(
                db_session, org.id,
                NodeType.UPLOAD, upload.id,
                NodeType.TRANSFORMATION, transformation.id,
                check_timeframe_value=30,
                check_timeframe_unit="days",
            )

    def test_unit_without_value_rejected(self, svc, db_session, org, upload, transformation):
        with pytest.raises(DependencyConfigError, match="requires check_timeframe_value"):
            svc.add_dependency(
                db_session, org.id,
                NodeType.UPLOAD, upload.id,
                NodeType.TRANSFORMATION, transformation.id,
                check_timeframe_unit="minutes",
            )


class TestRemoveDependency:
    def test_sets_deleted_at(self, svc, db_session, org, upload, transformation):
        dep = svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        result = svc.remove_dependency(db_session, org.id, dep.id)
        assert result is True
        db_session.refresh(dep)
        assert dep.deleted_at is not None

    def test_nonexistent_returns_false(self, svc, db_session, org):
        result = svc.remove_dependency(db_session, org.id, 99999)
        assert result is False


class TestGetDependency:
    def test_existing(self, svc, db_session, org, upload, transformation):
        created = svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        fetched = svc.get_dependency(db_session, org.id, created.id)
        assert fetched is not None
        assert fetched.id == created.id

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_dependency(db_session, org.id, 99999) is None

    def test_wrong_org(self, svc, db_session, org, other_org, upload, transformation):
        created = svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        assert svc.get_dependency(db_session, other_org.id, created.id) is None

    def test_soft_deleted_excluded(self, svc, db_session, org, upload, transformation):
        created = svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        svc.remove_dependency(db_session, org.id, created.id)
        assert svc.get_dependency(db_session, org.id, created.id) is None


class TestListDependencies:
    def test_empty(self, svc, db_session, org):
        result = svc.list_dependencies(db_session, org.id)
        assert result == []

    def test_multiple(self, svc, db_session, org, upload, transformation, transformation2):
        svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation2.id,
        )
        result = svc.list_dependencies(db_session, org.id)
        assert len(result) == 2

    def test_excludes_deleted(self, svc, db_session, org, upload, transformation, transformation2):
        dep1 = svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation2.id,
        )
        svc.remove_dependency(db_session, org.id, dep1.id)
        result = svc.list_dependencies(db_session, org.id)
        assert len(result) == 1


class TestGetUpstream:
    def test_returns_correct_edges(
        self, svc, db_session, org, upload, transformation, transformation2
    ):
        svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        svc.add_dependency(
            db_session,
            org.id,
            NodeType.TRANSFORMATION,
            transformation2.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        upstream = svc.get_upstream(db_session, org.id, NodeType.TRANSFORMATION, transformation.id)
        assert len(upstream) == 2

    def test_empty_when_none(self, svc, db_session, org, upload):
        upstream = svc.get_upstream(db_session, org.id, NodeType.UPLOAD, upload.id)
        assert upstream == []

    def test_excludes_deleted(self, svc, db_session, org, upload, transformation):
        dep = svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        svc.remove_dependency(db_session, org.id, dep.id)
        upstream = svc.get_upstream(db_session, org.id, NodeType.TRANSFORMATION, transformation.id)
        assert upstream == []


class TestGetDownstream:
    def test_returns_correct_edges(
        self, svc, db_session, org, upload, transformation, transformation2
    ):
        svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation2.id,
        )
        downstream = svc.get_downstream(db_session, org.id, NodeType.UPLOAD, upload.id)
        assert len(downstream) == 2

    def test_org_scoped(self, svc, db_session, org, other_org, upload, transformation):
        svc.add_dependency(
            db_session,
            org.id,
            NodeType.UPLOAD,
            upload.id,
            NodeType.TRANSFORMATION,
            transformation.id,
        )
        downstream = svc.get_downstream(db_session, other_org.id, NodeType.UPLOAD, upload.id)
        assert downstream == []

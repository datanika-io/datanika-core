"""TDD tests for dependency check service — verifies upstream satisfaction logic."""

from datetime import UTC, datetime, timedelta

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus
from datanika.models.transformation import Materialization
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.dependency_check import (
    DependencyCheckResult,
    check_upstream_dependencies,
)
from datanika.services.dependency_service import DependencyService
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
def dep_svc(upload_svc, transform_svc):
    return DependencyService(upload_svc, transform_svc)


@pytest.fixture
def org(db_session):
    org = Organization(name="CheckOrg", slug="check-org")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def upload(upload_svc, conn_svc, db_session, org):
    src = conn_svc.create_connection(
        db_session, org.id, "SrcCheck", ConnectionType.POSTGRES, ConnectionDirection.SOURCE,
        {"h": "x"},
    )
    dst = conn_svc.create_connection(
        db_session, org.id, "DstCheck", ConnectionType.POSTGRES, ConnectionDirection.DESTINATION,
        {"h": "y"},
    )
    return upload_svc.create_upload(db_session, org.id, "checkupload", "desc", src.id, dst.id, {})


@pytest.fixture
def upload2(upload_svc, conn_svc, db_session, org):
    src = conn_svc.create_connection(
        db_session, org.id, "Src2Check", ConnectionType.POSTGRES, ConnectionDirection.SOURCE,
        {"h": "x"},
    )
    dst = conn_svc.create_connection(
        db_session, org.id, "Dst2Check", ConnectionType.POSTGRES, ConnectionDirection.DESTINATION,
        {"h": "y"},
    )
    return upload_svc.create_upload(
        db_session, org.id, "checkupload2", "desc", src.id, dst.id, {},
    )


@pytest.fixture
def transformation(transform_svc, db_session, org):
    return transform_svc.create_transformation(
        db_session, org.id, "check_model_a", "SELECT 1", Materialization.VIEW
    )


class TestCheckUpstreamDependencies:
    def test_no_upstream_deps_is_satisfied(self, db_session, org, dep_svc, transformation):
        """A node with no upstream dependencies should always be satisfied."""
        result = check_upstream_dependencies(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id, dep_service=dep_svc
        )
        assert isinstance(result, DependencyCheckResult)
        assert result.satisfied is True
        assert result.unsatisfied_nodes == []

    def test_metadata_only_dep_is_skipped(
        self, db_session, org, dep_svc, upload, transformation
    ):
        """Dependencies without check_timeframe_value are metadata-only and skipped."""
        dep_svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload.id,
            NodeType.TRANSFORMATION, transformation.id,
        )
        result = check_upstream_dependencies(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id, dep_service=dep_svc
        )
        assert result.satisfied is True

    def test_unsatisfied_when_no_recent_run(
        self, db_session, org, dep_svc, upload, transformation
    ):
        """Dependency with timeframe but no recent SUCCESS run is unsatisfied."""
        dep_svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload.id,
            NodeType.TRANSFORMATION, transformation.id,
            check_timeframe_value=30,
            check_timeframe_unit="minutes",
        )
        result = check_upstream_dependencies(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id, dep_service=dep_svc
        )
        assert result.satisfied is False
        assert f"upload:{upload.id}" in result.unsatisfied_nodes

    def test_satisfied_with_recent_success_run(
        self, db_session, org, dep_svc, upload, transformation
    ):
        """Dependency is satisfied when upstream has a SUCCESS run within the timeframe."""
        dep_svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload.id,
            NodeType.TRANSFORMATION, transformation.id,
            check_timeframe_value=30,
            check_timeframe_unit="minutes",
        )
        now = datetime.now(UTC)
        run = Run(
            org_id=org.id,
            target_type=NodeType.UPLOAD,
            target_id=upload.id,
            status=RunStatus.SUCCESS,
            finished_at=now - timedelta(minutes=10),
        )
        db_session.add(run)
        db_session.flush()

        result = check_upstream_dependencies(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id,
            dep_service=dep_svc, now=now,
        )
        assert result.satisfied is True

    def test_unsatisfied_when_run_outside_timeframe(
        self, db_session, org, dep_svc, upload, transformation
    ):
        """A SUCCESS run that's older than the timeframe doesn't satisfy the dep."""
        dep_svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload.id,
            NodeType.TRANSFORMATION, transformation.id,
            check_timeframe_value=30,
            check_timeframe_unit="minutes",
        )
        now = datetime.now(UTC)
        run = Run(
            org_id=org.id,
            target_type=NodeType.UPLOAD,
            target_id=upload.id,
            status=RunStatus.SUCCESS,
            finished_at=now - timedelta(minutes=60),
        )
        db_session.add(run)
        db_session.flush()

        result = check_upstream_dependencies(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id,
            dep_service=dep_svc, now=now,
        )
        assert result.satisfied is False

    def test_hours_unit(self, db_session, org, dep_svc, upload, transformation):
        """Timeframe with 'hours' unit works correctly."""
        dep_svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload.id,
            NodeType.TRANSFORMATION, transformation.id,
            check_timeframe_value=2,
            check_timeframe_unit="hours",
        )
        now = datetime.now(UTC)
        run = Run(
            org_id=org.id,
            target_type=NodeType.UPLOAD,
            target_id=upload.id,
            status=RunStatus.SUCCESS,
            finished_at=now - timedelta(hours=1),
        )
        db_session.add(run)
        db_session.flush()

        result = check_upstream_dependencies(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id,
            dep_service=dep_svc, now=now,
        )
        assert result.satisfied is True

    def test_failed_run_does_not_satisfy(
        self, db_session, org, dep_svc, upload, transformation
    ):
        """Only SUCCESS runs count — FAILED doesn't satisfy."""
        dep_svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload.id,
            NodeType.TRANSFORMATION, transformation.id,
            check_timeframe_value=30,
            check_timeframe_unit="minutes",
        )
        now = datetime.now(UTC)
        run = Run(
            org_id=org.id,
            target_type=NodeType.UPLOAD,
            target_id=upload.id,
            status=RunStatus.FAILED,
            finished_at=now - timedelta(minutes=5),
        )
        db_session.add(run)
        db_session.flush()

        result = check_upstream_dependencies(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id,
            dep_service=dep_svc, now=now,
        )
        assert result.satisfied is False

    def test_multiple_upstreams_all_must_satisfy(
        self, db_session, org, dep_svc, upload, upload2, transformation
    ):
        """When there are multiple upstream deps with timeframes, all must be satisfied."""
        dep_svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload.id,
            NodeType.TRANSFORMATION, transformation.id,
            check_timeframe_value=30,
            check_timeframe_unit="minutes",
        )
        dep_svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload2.id,
            NodeType.TRANSFORMATION, transformation.id,
            check_timeframe_value=30,
            check_timeframe_unit="minutes",
        )
        now = datetime.now(UTC)
        # Only one upstream has a recent run
        run = Run(
            org_id=org.id,
            target_type=NodeType.UPLOAD,
            target_id=upload.id,
            status=RunStatus.SUCCESS,
            finished_at=now - timedelta(minutes=5),
        )
        db_session.add(run)
        db_session.flush()

        result = check_upstream_dependencies(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id,
            dep_service=dep_svc, now=now,
        )
        assert result.satisfied is False
        assert len(result.unsatisfied_nodes) == 1
        assert f"upload:{upload2.id}" in result.unsatisfied_nodes

    def test_multiple_upstreams_all_satisfied(
        self, db_session, org, dep_svc, upload, upload2, transformation
    ):
        """All upstream deps satisfied → overall result is satisfied."""
        dep_svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload.id,
            NodeType.TRANSFORMATION, transformation.id,
            check_timeframe_value=30,
            check_timeframe_unit="minutes",
        )
        dep_svc.add_dependency(
            db_session, org.id,
            NodeType.UPLOAD, upload2.id,
            NodeType.TRANSFORMATION, transformation.id,
            check_timeframe_value=30,
            check_timeframe_unit="minutes",
        )
        now = datetime.now(UTC)
        for uid in [upload.id, upload2.id]:
            run = Run(
                org_id=org.id,
                target_type=NodeType.UPLOAD,
                target_id=uid,
                status=RunStatus.SUCCESS,
                finished_at=now - timedelta(minutes=5),
            )
            db_session.add(run)
        db_session.flush()

        result = check_upstream_dependencies(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id,
            dep_service=dep_svc, now=now,
        )
        assert result.satisfied is True

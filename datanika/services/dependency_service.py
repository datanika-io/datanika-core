"""Dependency management service — CRUD with validation for upload/transformation edges."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.dependency import Dependency, NodeType
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService


class DependencyConfigError(ValueError):
    """Raised when dependency configuration fails validation."""


class DependencyService:
    def __init__(
        self,
        upload_service: UploadService,
        transformation_service: TransformationService,
        pipeline_service: PipelineService | None = None,
    ):
        self._upload_svc = upload_service
        self._transform_svc = transformation_service
        self._pipeline_svc = pipeline_service or PipelineService()

    def _validate_node(
        self, session: Session, org_id: int, node_type: NodeType, node_id: int, label: str
    ) -> None:
        if node_type == NodeType.UPLOAD:
            target = self._upload_svc.get_upload(session, org_id, node_id)
        elif node_type == NodeType.TRANSFORMATION:
            target = self._transform_svc.get_transformation(session, org_id, node_id)
        elif node_type == NodeType.PIPELINE:
            target = self._pipeline_svc.get_pipeline(session, org_id, node_id)
        else:
            target = None

        if target is None:
            raise DependencyConfigError(
                f"{label} {node_type.value} with id {node_id} not found in org {org_id}"
            )

    _VALID_TIMEFRAME_UNITS = ("minutes", "hours")

    def add_dependency(
        self,
        session: Session,
        org_id: int,
        upstream_type: NodeType,
        upstream_id: int,
        downstream_type: NodeType,
        downstream_id: int,
        check_timeframe_value: int | None = None,
        check_timeframe_unit: str | None = None,
    ) -> Dependency:
        # Validate timeframe params
        if check_timeframe_unit is not None and check_timeframe_value is None:
            raise DependencyConfigError("check_timeframe_unit requires check_timeframe_value")
        if check_timeframe_value is not None and check_timeframe_value <= 0:
            raise DependencyConfigError("check_timeframe_value must be positive")
        if (
            check_timeframe_unit is not None
            and check_timeframe_unit not in self._VALID_TIMEFRAME_UNITS
        ):
            raise DependencyConfigError(
                f"check_timeframe_unit must be one of {self._VALID_TIMEFRAME_UNITS}"
            )

        # Reject self-references
        if upstream_type == downstream_type and upstream_id == downstream_id:
            raise DependencyConfigError("self-reference: upstream and downstream are the same node")

        # Validate both nodes exist
        self._validate_node(session, org_id, upstream_type, upstream_id, "upstream")
        self._validate_node(session, org_id, downstream_type, downstream_id, "downstream")

        # Reject duplicates
        existing = session.execute(
            select(Dependency).where(
                Dependency.org_id == org_id,
                Dependency.upstream_type == upstream_type,
                Dependency.upstream_id == upstream_id,
                Dependency.downstream_type == downstream_type,
                Dependency.downstream_id == downstream_id,
                Dependency.deleted_at.is_(None),
            )
        ).scalar_one_or_none()

        if existing is not None:
            raise DependencyConfigError("dependency already exists")

        dep = Dependency(
            org_id=org_id,
            upstream_type=upstream_type,
            upstream_id=upstream_id,
            downstream_type=downstream_type,
            downstream_id=downstream_id,
            check_timeframe_value=check_timeframe_value,
            check_timeframe_unit=check_timeframe_unit,
        )
        session.add(dep)
        session.flush()
        return dep

    def remove_dependency(self, session: Session, org_id: int, dependency_id: int) -> bool:
        dep = self.get_dependency(session, org_id, dependency_id)
        if dep is None:
            return False
        dep.deleted_at = datetime.now(UTC)
        session.flush()
        return True

    def get_dependency(
        self, session: Session, org_id: int, dependency_id: int
    ) -> Dependency | None:
        stmt = select(Dependency).where(
            Dependency.id == dependency_id,
            Dependency.org_id == org_id,
            Dependency.deleted_at.is_(None),
        )
        return session.execute(stmt).scalar_one_or_none()

    def list_dependencies(self, session: Session, org_id: int) -> list[Dependency]:
        stmt = (
            select(Dependency)
            .where(Dependency.org_id == org_id, Dependency.deleted_at.is_(None))
            .order_by(Dependency.created_at.desc())
        )
        return list(session.execute(stmt).scalars().all())

    def get_upstream(
        self, session: Session, org_id: int, node_type: NodeType, node_id: int
    ) -> list[Dependency]:
        stmt = (
            select(Dependency)
            .where(
                Dependency.org_id == org_id,
                Dependency.downstream_type == node_type,
                Dependency.downstream_id == node_id,
                Dependency.deleted_at.is_(None),
            )
            .order_by(Dependency.created_at.desc())
        )
        return list(session.execute(stmt).scalars().all())

    def get_downstream(
        self, session: Session, org_id: int, node_type: NodeType, node_id: int
    ) -> list[Dependency]:
        stmt = (
            select(Dependency)
            .where(
                Dependency.org_id == org_id,
                Dependency.upstream_type == node_type,
                Dependency.upstream_id == node_id,
                Dependency.deleted_at.is_(None),
            )
            .order_by(Dependency.created_at.desc())
        )
        return list(session.execute(stmt).scalars().all())

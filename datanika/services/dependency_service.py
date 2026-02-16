"""Dependency management service — CRUD with validation for pipeline/transformation edges."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.dependency import Dependency, NodeType
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService


class DependencyConfigError(ValueError):
    """Raised when dependency configuration fails validation."""


class DependencyService:
    def __init__(
        self,
        pipeline_service: PipelineService,
        transformation_service: TransformationService,
    ):
        self._pipe_svc = pipeline_service
        self._transform_svc = transformation_service

    def _validate_node(
        self, session: Session, org_id: int, node_type: NodeType, node_id: int, label: str
    ) -> None:
        if node_type == NodeType.PIPELINE:
            target = self._pipe_svc.get_pipeline(session, org_id, node_id)
        elif node_type == NodeType.TRANSFORMATION:
            target = self._transform_svc.get_transformation(session, org_id, node_id)
        else:
            target = None

        if target is None:
            raise DependencyConfigError(
                f"{label} {node_type.value} with id {node_id} not found in org {org_id}"
            )

    def add_dependency(
        self,
        session: Session,
        org_id: int,
        upstream_type: NodeType,
        upstream_id: int,
        downstream_type: NodeType,
        downstream_id: int,
    ) -> Dependency:
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

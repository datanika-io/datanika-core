"""Dependency check service — verifies upstream dependencies are satisfied before execution."""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.dependency import Dependency, NodeType
from datanika.models.run import Run, RunStatus
from datanika.services.dependency_service import DependencyService
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService


@dataclass
class DependencyCheckResult:
    satisfied: bool
    unsatisfied_nodes: list[str] = field(default_factory=list)


def check_upstream_dependencies(
    session: Session,
    org_id: int,
    target_type: NodeType,
    target_id: int,
    dep_service: DependencyService | None = None,
    now: datetime | None = None,
) -> DependencyCheckResult:
    """Check whether all upstream dependencies with a check_timeframe have a recent SUCCESS run.

    Dependencies without check_timeframe_value are metadata-only and are skipped.
    """
    if dep_service is None:
        from datanika.config import settings
        from datanika.services.connection_service import ConnectionService
        from datanika.services.encryption import EncryptionService

        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()
        pipeline_svc = PipelineService()
        dep_service = DependencyService(upload_svc, transform_svc, pipeline_svc)

    if now is None:
        now = datetime.now(UTC)

    upstream_deps: list[Dependency] = dep_service.get_upstream(
        session, org_id, target_type, target_id
    )

    unsatisfied: list[str] = []

    for dep in upstream_deps:
        if dep.check_timeframe_value is None:
            continue

        unit = dep.check_timeframe_unit or "minutes"
        if unit == "hours":
            cutoff = now - timedelta(hours=dep.check_timeframe_value)
        else:
            cutoff = now - timedelta(minutes=dep.check_timeframe_value)

        success_run = session.execute(
            select(Run).where(
                Run.org_id == org_id,
                Run.target_type == dep.upstream_type,
                Run.target_id == dep.upstream_id,
                Run.status == RunStatus.SUCCESS,
                Run.finished_at >= cutoff,
            )
        ).first()

        if success_run is None:
            unsatisfied.append(f"{dep.upstream_type.value}:{dep.upstream_id}")

    return DependencyCheckResult(
        satisfied=len(unsatisfied) == 0,
        unsatisfied_nodes=unsatisfied,
    )

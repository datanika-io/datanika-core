"""Schedule management service — CRUD with cron validation and target validation."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.dependency import NodeType
from datanika.models.schedule import Schedule
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService

if TYPE_CHECKING:
    from datanika.services.scheduler_integration import SchedulerIntegrationService


class ScheduleConfigError(ValueError):
    """Raised when schedule configuration fails validation."""


class ScheduleService:
    def __init__(
        self,
        upload_service: UploadService,
        transformation_service: TransformationService,
        scheduler_integration: SchedulerIntegrationService | None = None,
        pipeline_service: PipelineService | None = None,
    ):
        self._upload_svc = upload_service
        self._transform_svc = transformation_service
        self._scheduler = scheduler_integration
        self._pipeline_svc = pipeline_service or PipelineService()

    def create_schedule(
        self,
        session: Session,
        org_id: int,
        target_type: NodeType,
        target_id: int,
        cron_expression: str,
        timezone: str = "UTC",
        is_active: bool = True,
    ) -> Schedule:
        self.validate_cron_expression(cron_expression)
        self.validate_target(session, org_id, target_type, target_id)

        schedule = Schedule(
            org_id=org_id,
            target_type=target_type,
            target_id=target_id,
            cron_expression=cron_expression,
            timezone=timezone,
            is_active=is_active,
        )
        session.add(schedule)
        session.flush()

        if self._scheduler is not None:
            self._scheduler.sync_schedule(schedule)

        return schedule

    def get_schedule(self, session: Session, org_id: int, schedule_id: int) -> Schedule | None:
        stmt = select(Schedule).where(
            Schedule.id == schedule_id,
            Schedule.org_id == org_id,
            Schedule.deleted_at.is_(None),
        )
        return session.execute(stmt).scalar_one_or_none()

    def list_schedules(self, session: Session, org_id: int) -> list[Schedule]:
        stmt = (
            select(Schedule)
            .where(Schedule.org_id == org_id, Schedule.deleted_at.is_(None))
            .order_by(Schedule.created_at.desc())
        )
        return list(session.execute(stmt).scalars().all())

    def update_schedule(
        self, session: Session, org_id: int, schedule_id: int, **kwargs
    ) -> Schedule | None:
        schedule = self.get_schedule(session, org_id, schedule_id)
        if schedule is None:
            return None

        if "cron_expression" in kwargs:
            self.validate_cron_expression(kwargs["cron_expression"])
            schedule.cron_expression = kwargs["cron_expression"]
        if "timezone" in kwargs:
            schedule.timezone = kwargs["timezone"]
        if "is_active" in kwargs:
            schedule.is_active = kwargs["is_active"]

        session.flush()

        if self._scheduler is not None:
            self._scheduler.sync_schedule(schedule)

        return schedule

    def delete_schedule(self, session: Session, org_id: int, schedule_id: int) -> bool:
        schedule = self.get_schedule(session, org_id, schedule_id)
        if schedule is None:
            return False
        schedule.deleted_at = datetime.now(UTC)
        session.flush()

        if self._scheduler is not None:
            self._scheduler.remove_schedule(schedule_id)

        return True

    def toggle_active(self, session: Session, org_id: int, schedule_id: int) -> Schedule | None:
        schedule = self.get_schedule(session, org_id, schedule_id)
        if schedule is None:
            return None
        schedule.is_active = not schedule.is_active
        session.flush()

        if self._scheduler is not None:
            self._scheduler.sync_schedule(schedule)

        return schedule

    @staticmethod
    def validate_cron_expression(cron_expression: str) -> None:
        if not cron_expression or not cron_expression.strip():
            raise ScheduleConfigError("cron expression must not be empty")
        fields = cron_expression.strip().split()
        if len(fields) != 5:
            raise ScheduleConfigError(
                f"cron expression must have exactly 5 fields, got {len(fields)}"
            )

    def validate_target(
        self, session: Session, org_id: int, target_type: NodeType, target_id: int
    ) -> None:
        if target_type == NodeType.UPLOAD:
            target = self._upload_svc.get_upload(session, org_id, target_id)
        elif target_type == NodeType.TRANSFORMATION:
            target = self._transform_svc.get_transformation(session, org_id, target_id)
        elif target_type == NodeType.PIPELINE:
            target = self._pipeline_svc.get_pipeline(session, org_id, target_id)
        else:
            target = None

        if target is None:
            raise ScheduleConfigError(
                f"target {target_type.value} with id {target_id} not found in org {org_id}"
            )

"""Schedule management service — CRUD with cron validation and target validation."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.errors import UserFacingError
from datanika.models.dependency import NodeType
from datanika.models.schedule import Schedule
from datanika.models.user import MemberRole
from datanika.services.authorization import assert_org_role
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService


class ScheduleConfigError(UserFacingError):
    """Raised when schedule configuration fails validation."""


class ScheduleService:
    def __init__(
        self,
        upload_service: UploadService,
        transformation_service: TransformationService,
        pipeline_service: PipelineService | None = None,
    ):
        # core#648 — this service deliberately takes NO scheduler.
        #
        # It used to accept a SchedulerIntegrationService and call sync_schedule() /
        # remove_schedule() on it after each write. That only worked because the scheduler
        # lived in this same process, which is the defect: every granian worker had one, and
        # APScheduler 3.x claims due jobs with an unlocked SELECT, so each fired every job.
        #
        # The `schedules` row is now the only channel. `datanika/scheduler_main.py`
        # reconciles from the table every settings.scheduler_reconcile_seconds. Re-adding a
        # scheduler argument here would put one back in the web process — kept out by
        # tests/test_deploy/test_scheduler_singleton.py rather than by this comment.
        self._upload_svc = upload_service
        self._transform_svc = transformation_service
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
        *,
        actor_user_id: int,
    ) -> Schedule:
        from datanika.hooks import emit

        # core#681 §1: editor. Before the quota emit -- an actor who may not create should
        # not consume a quota check, and a quota refusal must not mask an authorization
        # one, because the two send the caller to different people.
        assert_org_role(
            session,
            org_id,
            actor_user_id,
            required=MemberRole.EDITOR,
            operation="create_schedule",
        )
        emit("schedule.before_create", session=session, org_id=org_id)
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
        self, session: Session, org_id: int, schedule_id: int, *, actor_user_id: int, **kwargs
    ) -> Schedule | None:
        schedule = self.get_schedule(session, org_id, schedule_id)
        if schedule is None:
            return None
        # §7.3: after the org-scoped lookup, before the mutation.
        assert_org_role(
            session,
            org_id,
            actor_user_id,
            required=MemberRole.EDITOR,
            operation="update_schedule",
        )

        if "cron_expression" in kwargs:
            self.validate_cron_expression(kwargs["cron_expression"])
            schedule.cron_expression = kwargs["cron_expression"]
        if "timezone" in kwargs:
            schedule.timezone = kwargs["timezone"]
        if "is_active" in kwargs:
            schedule.is_active = kwargs["is_active"]

        session.flush()

        return schedule

    def delete_schedule(
        self, session: Session, org_id: int, schedule_id: int, *, actor_user_id: int
    ) -> bool:
        schedule = self.get_schedule(session, org_id, schedule_id)
        if schedule is None:
            return False
        # §7.3: after the org-scoped lookup, before the mutation.
        assert_org_role(
            session,
            org_id,
            actor_user_id,
            required=MemberRole.ADMIN,
            operation="delete_schedule",
        )
        schedule.deleted_at = datetime.now(UTC)
        session.flush()

        return True

    def toggle_active(
        self, session: Session, org_id: int, schedule_id: int, *, actor_user_id: int
    ) -> Schedule | None:
        schedule = self.get_schedule(session, org_id, schedule_id)
        if schedule is None:
            return None
        # §7.3: after the org-scoped lookup, before the mutation.
        assert_org_role(
            session,
            org_id,
            actor_user_id,
            required=MemberRole.EDITOR,
            operation="toggle_schedule",
        )
        schedule.is_active = not schedule.is_active
        session.flush()

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

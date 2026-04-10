"""Onboarding service — Getting Started checklist progress for a single org.

The checklist has 5 fixed steps. Progress is derived from existing tables
(connections, pipelines, runs, transformations, schedules) filtered by
org_id — no new columns are stored for the progress itself. Dismissal is
tracked separately on the user model.
"""

from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from datanika.models.connection import Connection
from datanika.models.pipeline import Pipeline
from datanika.models.run import Run
from datanika.models.schedule import Schedule
from datanika.models.transformation import Transformation

CHECKLIST_TOTAL = 5


class ChecklistProgress(BaseModel):
    has_connection: bool = False
    has_pipeline: bool = False
    has_run: bool = False
    has_transformation: bool = False
    has_schedule: bool = False

    @property
    def done_count(self) -> int:
        return sum(
            (
                self.has_connection,
                self.has_pipeline,
                self.has_run,
                self.has_transformation,
                self.has_schedule,
            )
        )

    @property
    def total(self) -> int:
        return CHECKLIST_TOTAL

    @property
    def all_done(self) -> bool:
        return self.done_count == CHECKLIST_TOTAL


def compute_checklist(
    *,
    connection_count: int,
    pipeline_count: int,
    run_count: int,
    transformation_count: int,
    schedule_count: int,
) -> ChecklistProgress:
    return ChecklistProgress(
        has_connection=connection_count > 0,
        has_pipeline=pipeline_count > 0,
        has_run=run_count > 0,
        has_transformation=transformation_count > 0,
        has_schedule=schedule_count > 0,
    )


def load_checklist_for_org(session: Session, org_id: int) -> ChecklistProgress:
    def _count(model) -> int:
        stmt = select(func.count()).select_from(model).where(model.org_id == org_id)
        return int(session.execute(stmt).scalar_one())

    return compute_checklist(
        connection_count=_count(Connection),
        pipeline_count=_count(Pipeline),
        run_count=_count(Run),
        transformation_count=_count(Transformation),
        schedule_count=_count(Schedule),
    )

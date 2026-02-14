"""Execution service — run lifecycle management."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from etlfabric.models.dependency import NodeType
from etlfabric.models.run import Run, RunStatus


class ExecutionService:
    def create_run(
        self,
        session: Session,
        org_id: int,
        target_type: NodeType,
        target_id: int,
    ) -> Run:
        run = Run(
            org_id=org_id,
            target_type=target_type,
            target_id=target_id,
            status=RunStatus.PENDING,
        )
        session.add(run)
        session.flush()
        return run

    def start_run(self, session: Session, run_id: int) -> Run | None:
        run = session.get(Run, run_id)
        if run is None:
            return None
        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(UTC)
        session.flush()
        return run

    def complete_run(
        self,
        session: Session,
        run_id: int,
        rows_loaded: int,
        logs: str,
    ) -> Run | None:
        run = session.get(Run, run_id)
        if run is None:
            return None
        run.status = RunStatus.SUCCESS
        run.finished_at = datetime.now(UTC)
        run.rows_loaded = rows_loaded
        run.logs = logs
        session.flush()
        return run

    def fail_run(
        self,
        session: Session,
        run_id: int,
        error_message: str,
        logs: str,
    ) -> Run | None:
        run = session.get(Run, run_id)
        if run is None:
            return None
        run.status = RunStatus.FAILED
        run.finished_at = datetime.now(UTC)
        run.error_message = error_message
        run.logs = logs
        session.flush()
        return run

    def cancel_run(self, session: Session, run_id: int) -> Run | None:
        run = session.get(Run, run_id)
        if run is None:
            return None
        if run.status not in (RunStatus.PENDING, RunStatus.RUNNING):
            return None
        run.status = RunStatus.CANCELLED
        run.finished_at = datetime.now(UTC)
        session.flush()
        return run

    def get_run(self, session: Session, org_id: int, run_id: int) -> Run | None:
        stmt = select(Run).where(Run.id == run_id, Run.org_id == org_id)
        return session.execute(stmt).scalar_one_or_none()

    def list_runs(
        self,
        session: Session,
        org_id: int,
        target_type: NodeType | None = None,
        target_id: int | None = None,
        status: RunStatus | None = None,
        limit: int | None = None,
    ) -> list[Run]:
        stmt = select(Run).where(Run.org_id == org_id)

        if target_type is not None:
            stmt = stmt.where(Run.target_type == target_type)
        if target_id is not None:
            stmt = stmt.where(Run.target_id == target_id)
        if status is not None:
            stmt = stmt.where(Run.status == status)

        stmt = stmt.order_by(Run.created_at.desc())

        if limit is not None:
            stmt = stmt.limit(limit)

        return list(session.execute(stmt).scalars().all())

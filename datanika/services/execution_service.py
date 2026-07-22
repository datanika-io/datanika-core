"""Execution service — run lifecycle management."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus


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

        # A failed run has never produced a notification — Slack, email or
        # in-app. Both handlers already branch on `status == "failed"`, but
        # nothing ever sent that status, so the branch was unreachable
        # (core#465).
        #
        # Announced from here rather than from each task's failure path
        # because this is the one place every failure passes through: five
        # call sites today, two of which are not `except` blocks at all (a dbt
        # command failing, and upstream dependencies never being satisfied).
        # Adding calls to those paths is what the issue warned against; this
        # leaves them untouched and cannot be forgotten by a sixth caller.
        #
        # `announce`, not `emit`: no subscriber may veto a failure that has
        # already happened, and a broken notifier must not mask the real error
        # (core#456).
        from datanika.hooks import announce

        announce(
            "run.failed",
            session=session,
            org_id=run.org_id,
            run_id=run.id,
            status=RunStatus.FAILED.value,
            error_message=error_message,
            target_type=getattr(run.target_type, "value", run.target_type),
            target_id=run.target_id,
        )
        return run

    def append_logs(self, session: Session, run_id: int, text: str) -> Run | None:
        """Add a line to a finished run's logs.

        For things that happen *after* the load completes and must not change
        its status — catalog sync is the case that motivated it (core#494).
        Swallowing that failure into the worker log alone kept a permanently
        broken feature invisible for as long as DuckDB had been a destination:
        the run row said success, Catalog stayed empty, and only SSH could
        connect the two.
        """
        run = session.get(Run, run_id)
        if run is None:
            return None
        run.logs = f"{run.logs}\n{text}" if run.logs else text
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

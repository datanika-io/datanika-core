"""One starter for a node that has no active schedule.

SPEC_DBT_PROJECT_AND_DOWNSTREAM. A direct upstream SUCCESS starts that node
once. A self-edge starts nothing. A node with an active schedule stays on
its cron, including the follow-up. Dispatch runs after commit, with
``scheduled=True``, so the existing 60-second, five-retry check still gates
the work. ``caused_by_run_id`` is how the success that started the in-flight
run is kept out of the follow-up; "after that run started" is
``finished_at`` after the run row's ``created_at``.
"""

import importlib
from datetime import UTC, datetime

from sqlalchemy import event, select
from sqlalchemy.orm import Session

from datanika.models.dependency import NodeType
from datanika.models.pipeline import Pipeline
from datanika.models.run import NON_TERMINAL_RUN_STATUSES, TERMINAL_RUN_STATUSES, Run, RunStatus
from datanika.models.schedule import Schedule
from datanika.models.transformation import Transformation
from datanika.models.upload import Upload

_TARGET_MODELS = {
    NodeType.UPLOAD: Upload,
    NodeType.TRANSFORMATION: Transformation,
    NodeType.PIPELINE: Pipeline,
}

_TASKS = {
    NodeType.UPLOAD: ("datanika.tasks.upload_tasks", "run_upload_task"),
    NodeType.TRANSFORMATION: ("datanika.tasks.transformation_tasks", "run_transformation_task"),
    NodeType.PIPELINE: ("datanika.tasks.pipeline_tasks", "run_pipeline_task"),
}


def note_terminal_run(session: Session, run: Run) -> None:
    """Apply the starter to a run that just reached a terminal status."""
    if run.status == RunStatus.SUCCESS:
        _start_direct_downstreams(session, run)
    if run.status in TERMINAL_RUN_STATUSES:
        _arm_follow_up(session, run)


def _dependencies():
    from cryptography.fernet import Fernet

    from datanika.services.connection_service import ConnectionService
    from datanika.services.dependency_service import DependencyService
    from datanika.services.encryption import EncryptionService
    from datanika.services.transformation_service import TransformationService
    from datanika.services.upload_service import UploadService

    service = getattr(_dependencies, "_service", None)
    if service is None:
        encryption = EncryptionService(Fernet.generate_key().decode())
        service = DependencyService(
            UploadService(ConnectionService(encryption)),
            TransformationService(),
        )
        _dependencies._service = service
    return service


def _start_direct_downstreams(session: Session, run: Run) -> None:
    edges = _dependencies().get_downstream(session, run.org_id, run.target_type, run.target_id)
    for edge in edges:
        if edge.downstream_type == run.target_type and edge.downstream_id == run.target_id:
            continue
        if edge.downstream_type not in _TARGET_MODELS:
            continue
        _start_one(
            session,
            run.org_id,
            edge.downstream_type,
            edge.downstream_id,
            caused_by_run_id=run.id,
        )


def _arm_follow_up(session: Session, run: Run) -> None:
    qualifying = _qualifying_upstream_successes(session, run)
    if not qualifying:
        return
    cause = min(qualifying, key=lambda item: item.id)
    _start_one(
        session,
        run.org_id,
        run.target_type,
        run.target_id,
        caused_by_run_id=cause.id,
    )


def _start_one(
    session: Session,
    org_id: int,
    target_type: NodeType,
    target_id: int,
    *,
    caused_by_run_id: int,
) -> None:
    if _has_active_schedule(session, org_id, target_type, target_id):
        return
    _lock_target(session, org_id, target_type, target_id)
    if _non_terminal_run_id(session, org_id, target_type, target_id) is not None:
        return
    from datanika.services.execution_service import ExecutionService

    created = ExecutionService().create_run(
        session,
        org_id,
        target_type,
        target_id,
        caused_by_run_id=caused_by_run_id,
    )
    _queue_dispatch(session, created.id, org_id, target_type)


def _has_active_schedule(session: Session, org_id: int, target_type, target_id: int) -> bool:
    row = session.execute(
        select(Schedule.id)
        .where(
            Schedule.org_id == org_id,
            Schedule.target_type == target_type,
            Schedule.target_id == target_id,
            Schedule.is_active.is_(True),
            Schedule.deleted_at.is_(None),
        )
        .limit(1)
    ).scalar_one_or_none()
    return row is not None


def _lock_target(session: Session, org_id: int, target_type, target_id: int) -> None:
    model = _TARGET_MODELS.get(target_type)
    if model is None:
        return
    # The row lock serialises two workers. A missing entity row locks nothing;
    # the non-terminal read below is still what decides, and SQLite ignores
    # FOR UPDATE.
    session.execute(
        select(model.id).where(model.id == target_id, model.org_id == org_id).with_for_update()
    )


def _non_terminal_run_id(session: Session, org_id: int, target_type, target_id: int) -> int | None:
    return session.execute(
        select(Run.id)
        .where(
            Run.org_id == org_id,
            Run.target_type == target_type,
            Run.target_id == target_id,
            Run.status.in_(tuple(NON_TERMINAL_RUN_STATUSES)),
            Run.deleted_at.is_(None),
        )
        .limit(1)
    ).scalar_one_or_none()


def _qualifying_upstream_successes(session: Session, run: Run) -> list[Run]:
    started = _as_utc(run.created_at)
    if started is None:
        return []
    found: list[Run] = []
    edges = _dependencies().get_upstream(session, run.org_id, run.target_type, run.target_id)
    for edge in edges:
        if edge.upstream_type == run.target_type and edge.upstream_id == run.target_id:
            continue
        rows = session.execute(
            select(Run).where(
                Run.org_id == run.org_id,
                Run.target_type == edge.upstream_type,
                Run.target_id == edge.upstream_id,
                Run.status == RunStatus.SUCCESS,
                Run.deleted_at.is_(None),
                Run.finished_at.is_not(None),
            )
        ).scalars()
        for upstream in rows:
            if run.caused_by_run_id is not None and upstream.id == run.caused_by_run_id:
                continue
            finished = _as_utc(upstream.finished_at)
            if finished is not None and finished > started:
                found.append(upstream)
    return found


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _queue_dispatch(session: Session, run_id: int, org_id: int, target_type) -> None:
    session.info.setdefault("downstream_dispatches", []).append((run_id, org_id, target_type))
    if session.info.get("_downstream_hooked"):
        return
    session.info["_downstream_hooked"] = True

    def _after_commit(sess: Session) -> None:
        pending = list(sess.info.pop("downstream_dispatches", []))
        for queued_run_id, queued_org_id, queued_type in pending:
            _delay(queued_run_id, queued_org_id, queued_type)

    def _after_rollback(sess: Session) -> None:
        sess.info.pop("downstream_dispatches", None)

    event.listen(session, "after_commit", _after_commit)
    event.listen(session, "after_rollback", _after_rollback)


def _delay(run_id: int, org_id: int, target_type) -> None:
    module_name, attr = _TASKS[target_type]
    task = getattr(importlib.import_module(module_name), attr)
    task.delay(run_id=run_id, org_id=org_id, scheduled=True)

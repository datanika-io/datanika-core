"""Execution service — run lifecycle management."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus


def get_org_run(session: Session, org_id: int, run_id: int) -> Run | None:
    """Resolve a run *within* an org — the single definition of ownership.

    Every read of a `Run` goes through here (#732). A bare
    `session.get(Run, run_id)` returns whichever tenant's row happens to carry
    that primary key, and run ids are small sequential integers.

    The defect this closes was not a live gap: the `/api/v1` run routes already
    resolved through this predicate before calling a mutator, so no
    user-supplied id reached one unscoped. It was that the mutators below
    *took a run_id with no org_id at all* — so a future route calling one
    directly would have been unscoped by default rather than by mistake, and
    nothing would have said so. `tests/test_security/test_tenant_fk_boundary.py`
    fails the build if a tenant-owned model is resolved without an org filter
    anywhere under `datanika/`.
    """
    stmt = select(Run).where(Run.id == run_id, Run.org_id == org_id)
    return session.execute(stmt).scalar_one_or_none()


def is_cancelled(run: Run) -> bool:
    """Has this run already reached the one terminal state a later report must not undo?

    core#657. ``POST /api/v1/runs/{id}/cancel`` returned 200 with ``status: cancelled``
    while the worker ran on and then called ``complete_run``, which set ``SUCCESS``
    unconditionally. So the cancellation was cosmetic and transient: the row flipped back,
    and a caller who re-read it saw its own cancellation undone.

    ⚠️ ``==``, not ``is``. ``RunStatus`` is a ``StrEnum``, so a member and its string value
    compare equal; identity would silently stop matching if this column ever round-trips as
    a bare string, and it would fail *open* — back to overwriting.

    🚨 Deliberately only ``CANCELLED``. Whether a terminal ``SUCCESS`` or ``FAILED`` should
    also be write-once is a real question — ``append_logs`` exists because post-completion
    work must not change status — but it is a wider contract change across call sites this
    did not audit, and core#657 AC2 asks for this one. Pinned by
    ``TestTheGuardIsNarrow::test_a_success_run_is_not_protected_by_this_change``.
    """
    return run.status == RunStatus.CANCELLED


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

    def start_run(self, session: Session, org_id: int, run_id: int) -> Run | None:
        run = get_org_run(session, org_id, run_id)
        if run is None:
            return None
        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(UTC)
        session.flush()
        return run

    def complete_run(
        self,
        session: Session,
        org_id: int,
        run_id: int,
        # `None` = not measured, and it is NOT zero (core#1170 AC3). `rows_loaded` is
        # `Mapped[int | None]` for exactly this reason; `_extract_rows_loaded` returns
        # `None` when it cannot read dlt's trace, and coercing it here would put the
        # conflation back one call later.
        rows_loaded: int | None,
        logs: str,
        bytes_processed: int | None = None,
    ) -> Run | None:
        """Finish a run.

        ``bytes_processed`` is optional because only *ingestion* runs have one
        (core#912). A dbt model run and a transformation run read no source and
        produce no byte count, so their rows stay NULL — which is the honest
        value, and distinguishable from a measured zero.
        """
        run = get_org_run(session, org_id, run_id)
        if run is None:
            return None
        # core#657 AC2. Only `status` and `finished_at` are withheld -- they are the
        # cancellation's own record. The observational fields below are still written:
        # they are the evidence of what the worker did before it was told to stop, and
        # AC3 asks the product to say what happened to partially-loaded data.
        #
        # 🚨 This does NOT stop metering, and reading it that way is the easy mistake:
        # this method emits no hook at all. `run.*_completed` fires from the TASKS, after
        # this returns (upload_tasks.py, pipeline_tasks.py, transformation_tasks.py), so a
        # cancelled run still bills. That is core#657 AC4 and it is still open.
        if not is_cancelled(run):
            run.status = RunStatus.SUCCESS
            run.finished_at = datetime.now(UTC)
        run.rows_loaded = rows_loaded
        run.logs = logs
        if bytes_processed is not None:
            run.bytes_processed = bytes_processed
        session.flush()
        return run

    def fail_run(
        self,
        session: Session,
        org_id: int,
        run_id: int,
        error_message: str,
        logs: str,
    ) -> Run | None:
        run = get_org_run(session, org_id, run_id)
        if run is None:
            return None
        # core#657 AC2, same shape as `complete_run`: the error is recorded, the terminal
        # CANCELLED is not overwritten.
        cancelled = is_cancelled(run)
        if not cancelled:
            run.status = RunStatus.FAILED
            run.finished_at = datetime.now(UTC)
        run.error_message = error_message
        run.logs = logs
        session.flush()

        # A run the user cancelled that then errors is not a failure they need paging
        # about -- `run.failed` reaches Slack, email and in-app. Announcing here would
        # generate a false alarm out of the user's own cancellation.
        if cancelled:
            return run

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

    def announce_completion(
        self, session: Session, org_id: int, run_id: int, event: str, **payload
    ) -> bool:
        """Announce a ``run.*_completed`` event carrying the run's REAL status.

        core#657 AC4. All three task call sites passed ``status="success"`` as a **hardcoded
        literal**, and a run the user CANCELLED still reaches them — nothing worker-side asks
        whether it was cancelled, so the work continues to its ordinary success path.

        The billing gate was never missing. ``datanika-cloud``'s ``billing/meter.py`` has gated
        on this field since cloud#84 (``_is_billable`` → ``status == "success"``) and its
        docstring anticipated this case exactly: *"`cancelled` is not billable either."* What it
        also recorded is the assumption it could not enforce from another repository — *"the
        `run.*_completed` events are announced solely from success paths"*. Cancellation made
        that false, so a correct gate was handed a falsehood and the user was charged for a run
        they stopped.

        Reading the status here rather than at each call site is the point: three literals is
        how a fourth arrives. Same reasoning as ``fail_run``'s announce, which lives in this
        service so a sixth caller cannot forget it.
        ``tests/test_services/test_cancelled_run_is_not_billed.py`` fails on a hardcoded status
        anywhere under ``datanika/tasks/``.

        Returns ``False`` without announcing when the run does not resolve within ``org_id`` —
        the tenancy predicate applies here as everywhere else.
        """
        run = get_org_run(session, org_id, run_id)
        if run is None:
            return False

        from datanika.hooks import announce

        announce(
            event,
            session=session,
            org_id=org_id,
            run_id=run_id,
            status=getattr(run.status, "value", run.status),
            **payload,
        )
        return True

    def append_logs(self, session: Session, org_id: int, run_id: int, text: str) -> Run | None:
        """Add a line to a finished run's logs.

        For things that happen *after* the load completes and must not change
        its status — catalog sync is the case that motivated it (core#494).
        Swallowing that failure into the worker log alone kept a permanently
        broken feature invisible for as long as DuckDB had been a destination:
        the run row said success, Catalog stayed empty, and only SSH could
        connect the two.
        """
        run = get_org_run(session, org_id, run_id)
        if run is None:
            return None
        run.logs = f"{run.logs}\n{text}" if run.logs else text
        session.flush()
        return run

    def cancel_run(self, session: Session, org_id: int, run_id: int) -> Run | None:
        run = get_org_run(session, org_id, run_id)
        if run is None:
            return None
        if run.status not in (RunStatus.PENDING, RunStatus.RUNNING):
            return None
        run.status = RunStatus.CANCELLED
        run.finished_at = datetime.now(UTC)
        session.flush()
        return run

    def get_run(self, session: Session, org_id: int, run_id: int) -> Run | None:
        """The public reader. Kept as a method for its callers; one predicate."""
        return get_org_run(session, org_id, run_id)

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

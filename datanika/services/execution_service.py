"""Execution service — run lifecycle management."""

from datetime import UTC, datetime

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session, aliased

from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus
from datanika.models.upload import Upload
from datanika.models.user import MemberRole
from datanika.services.authorization import assert_org_role


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


#: What a run that never reached its engine records, beside ``rows_loaded = 0`` (core#657 §7 2a).
CANCELLED_BEFORE_START_LOG = (
    "Cancelled before it started: the engine never ran, so nothing was read from the source "
    "and nothing was written to the destination."
)


def _transition_unless_cancelled(session: Session, org_id: int, run: Run, **values) -> bool:
    """Write a lifecycle transition IN THE DATABASE, refusing to overwrite ``CANCELLED``.

    core#657, measured 2026-09-16. The guards used to read ``run.status`` off ``run =
    get_org_run(session, ...)``, an ORM ``SELECT`` — which does **not** overwrite the loaded
    attributes of an instance already in the session's identity map. Every task holds such an
    instance across its engine call, so a cancel committed by the API's own session never reached
    the worker's guard: ``complete_run`` read its stale ``RUNNING`` and wrote ``SUCCESS`` over the
    cancellation, ``fail_run`` wrote ``FAILED`` and paged, and ``start_run`` started a run cancelled
    while ``PENDING``. Two-session probe: ``success`` / ``failed`` / ``running``; the same sequence
    with no reference held preserved ``cancelled``, which is what attributes it to the identity map.

    So the predicate lives in the ``UPDATE`` (``… AND status <> 'CANCELLED'``). It reads the row as
    it is at write time: no stale read, and no gap between a read and a write for a cancel to land
    in. The instance is refreshed afterwards so the caller's object agrees with the row.

    Returns whether this call wrote the transition. ``False`` means the run was cancelled.
    """
    result = session.execute(
        update(Run)
        .where(Run.id == run.id, Run.org_id == org_id, Run.status != RunStatus.CANCELLED)
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    session.refresh(run, attribute_names=list(values))
    return result.rowcount == 1


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

    def create_requested_run(
        self,
        session: Session,
        org_id: int,
        target_type: NodeType,
        target_id: int,
        *,
        actor_user_id: int,
    ) -> Run:
        """A run a MEMBER asked for -- the Run button or a REST trigger (core#681).

        `SPEC_SERVICE_AUTHORIZATION` §1 puts `run_upload` and `run_pipeline` at `editor`; running a
        transformation is the same act, and §2's rule gives it the same threshold. Enforced here so
        the Reflex handlers and the REST triggers inherit one check (§4).

        ⚠️ The scheduler does not come through here. It is not a member and has no actor, so it
        keeps :meth:`create_run`; giving that method an actor would either fail every scheduled run
        closed or invent an actor, and both are wrong.
        """
        assert_org_role(
            session,
            org_id,
            actor_user_id,
            required=MemberRole.EDITOR,
            operation=f"run_{target_type.value}",
        )
        return self.create_run(session, org_id, target_type, target_id)

    def start_run(self, session: Session, org_id: int, run_id: int) -> Run | None:
        run = get_org_run(session, org_id, run_id)
        if run is None:
            return None
        # core#657: a run cancelled while PENDING stays CANCELLED, and gets no start time.
        _transition_unless_cancelled(
            session, org_id, run, status=RunStatus.RUNNING, started_at=datetime.now(UTC)
        )
        return run

    def start_exclusive_upload_run(
        self, session: Session, org_id: int, run_id: int, upload_id: int
    ) -> int | None:
        """Start a run of an incremental upload unless another run of it is running (core#1404).

        ``SPEC_INCREMENTAL_UPLOADS`` §2.4. An incremental upload's runs share one cursor, so two of
        them extracting at once would both read past the same value and both land the same rows.

        Returns ``None`` when this call started the run, or when the run was cancelled, which the
        caller's pre-engine checkpoint then stops exactly as after :meth:`start_run`. Otherwise it
        returns the id of the run in progress and leaves this run ``PENDING``. The caller commits.

        🚨 **The row lock on the upload is the guarantee, not the ``NOT EXISTS``.** Two claims whose
        ``UPDATE``s overlap each read the committed state from before either wrote, so each finds no
        run in progress and both start. Locking the upload first makes the second claim wait until
        the first commits, and its ``UPDATE`` then reads the first run as running.
        ``tests/test_tasks/test_incremental_upload_runs_do_not_overlap.py`` holds a claim
        uncommitted and asserts the second one waits. SQLite ignores ``FOR UPDATE``, and serialises
        writers instead.

        ⚠️ **No timeout decides that a run is stale.** A run left ``RUNNING`` by a worker that died
        keeps refusing, and the refusal says to cancel it, which the user can do. A timeout would
        let a long, healthy run be joined by a second one.
        """
        run = get_org_run(session, org_id, run_id)
        if run is None:
            return None
        other = aliased(Run)
        in_progress = select(other.id).where(
            other.org_id == org_id,
            other.target_type == NodeType.UPLOAD,
            other.target_id == upload_id,
            other.status == RunStatus.RUNNING,
            other.id != run_id,
        )
        session.execute(
            select(Upload.id)
            .where(Upload.id == upload_id, Upload.org_id == org_id)
            .with_for_update()
        )
        # A run in progress can finish between the refused UPDATE and the read of its id, so the
        # claim is retried rather than reporting a run that no longer holds the cursor.
        for _attempt in range(3):
            started = session.execute(
                update(Run)
                .where(
                    Run.id == run_id,
                    Run.org_id == org_id,
                    Run.status != RunStatus.CANCELLED,
                    ~in_progress.exists(),
                )
                .values(status=RunStatus.RUNNING, started_at=datetime.now(UTC))
                .execution_options(synchronize_session=False)
            )
            session.refresh(run, attribute_names=["status", "started_at"])
            if started.rowcount == 1 or run.status == RunStatus.CANCELLED:
                return None
            busy = session.execute(in_progress.order_by(other.id).limit(1)).scalar_one_or_none()
            if busy is not None:
                return busy
        return None

    def is_cancelled_now(self, session: Session, org_id: int, run_id: int) -> bool:
        """Is the run cancelled, read from the DATABASE — never from the session's identity map?

        core#657 §7 2a, the pre-flight checkpoint. A column ``SELECT`` returns the row's current
        value; ``get_org_run(...).status`` returns whatever this session loaded earlier, which is
        exactly the reading that never saw a cancel (see ``_transition_unless_cancelled``).
        """
        status = session.execute(
            select(Run.status).where(Run.id == run_id, Run.org_id == org_id)
        ).scalar_one_or_none()
        return status == RunStatus.CANCELLED

    def skip_if_cancelled(self, session: Session, org_id: int, run_id: int) -> bool:
        """The pre-flight checkpoint (``SPEC_RUN_CANCELLATION`` §7 **2a**, not 2b).

        ``True`` means the caller must not invoke its engine. The run then records what actually
        happened — ``rows_loaded = 0`` and a log line saying the engine never ran — which is a
        measurement, not an unknown. Its status is already ``CANCELLED`` and stays so.

        ⚠️ This stops a run that has not reached its engine. A run already inside
        ``pipeline.run()`` or a dbt invoke is not stopped by anything here: both are single opaque
        calls and §6 rules revocation out. Reporting this as "cancellation stops runs" is the
        overclaim §7.2 warns about.
        """
        if not self.is_cancelled_now(session, org_id, run_id):
            return False
        self.complete_run(session, org_id, run_id, rows_loaded=0, logs=CANCELLED_BEFORE_START_LOG)
        return True

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
        # 🔴 Written as a compare-and-set in the database (core#657, 2026-09-16). The previous
        # `if not is_cancelled(run)` read this session's identity map, which in every task still
        # held the RUNNING loaded before the engine call, so a cancel committed by the API was
        # overwritten with SUCCESS. See `_transition_unless_cancelled`.
        #
        # This method emits no hook: `run.*_completed` is announced by the TASKS, with the run's
        # real status read at that point (AC4, `announce_completion`).
        _transition_unless_cancelled(
            session, org_id, run, status=RunStatus.SUCCESS, finished_at=datetime.now(UTC)
        )
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
        # CANCELLED is not overwritten — decided in the database, and the announce below follows
        # what THIS write did rather than a second read that a cancel could land between.
        cancelled = not _transition_unless_cancelled(
            session, org_id, run, status=RunStatus.FAILED, finished_at=datetime.now(UTC)
        )
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
        ``tests/test_services/test_cancelled_run_announces_its_real_status.py`` fails on a
        hardcoded status anywhere under ``datanika/tasks/``.

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

    def cancel_run(
        self, session: Session, org_id: int, run_id: int, *, actor_user_id: int
    ) -> Run | None:
        run = get_org_run(session, org_id, run_id)
        if run is None:
            return None
        # core#681: cancelling is the ordinary lifecycle, so `editor` (SPEC §2). §7.3: after the
        # org-scoped lookup, before the mutation.
        assert_org_role(
            session,
            org_id,
            actor_user_id,
            required=MemberRole.EDITOR,
            operation="cancel_run",
        )
        if run.status not in (RunStatus.PENDING, RunStatus.RUNNING):
            return None
        run.status = RunStatus.CANCELLED
        run.finished_at = datetime.now(UTC)
        session.flush()
        return run

    def get_run(self, session: Session, org_id: int, run_id: int) -> Run | None:
        """The public reader. Kept as a method for its callers; one predicate."""
        return get_org_run(session, org_id, run_id)

    def latest_upload_catalog_verdicts(self, session: Session, org_id: int) -> list:
        """The most recent successful run of each upload, and what its catalogue sync found.

        Rows of ``(target_id, run_id, catalog_sync_verdict, catalog_sync_schema)``, one per upload
        with a successful run (core#1398). `/models` asks this on every load, so it selects four
        columns and never a run's logs. "Most recent" is the latest ``finished_at``, then the
        highest id.
        """
        ranked = (
            select(
                Run.target_id,
                Run.id.label("run_id"),
                Run.catalog_sync_verdict,
                Run.catalog_sync_schema,
                func.row_number()
                .over(
                    partition_by=Run.target_id,
                    order_by=(Run.finished_at.desc().nulls_last(), Run.id.desc()),
                )
                .label("recency"),
            )
            .where(
                Run.org_id == org_id,
                Run.target_type == NodeType.UPLOAD,
                Run.status == RunStatus.SUCCESS,
            )
            .subquery()
        )
        stmt = (
            select(
                ranked.c.target_id,
                ranked.c.run_id,
                ranked.c.catalog_sync_verdict,
                ranked.c.catalog_sync_schema,
            )
            .where(ranked.c.recency == 1)
            .order_by(ranked.c.target_id)
        )
        return list(session.execute(stmt).all())

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

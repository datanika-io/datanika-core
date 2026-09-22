"""Run state for Reflex UI."""

import reflex as rx
from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.models.run import (
    CANCEL_REQUESTED_RUN_STATUSES,
    CANCELLABLE_RUN_STATUSES,
    NON_TERMINAL_RUN_STATUSES,
    RunStatus,
)
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.run_cancellation import bills_usage as _bills_usage
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from datanika.ui.state.base_state import BaseState, get_sync_session

#: The marker a never-measured row count renders as (core#1170 AC3).
#:
#: ⚠️ Not translated, deliberately. `WORKFLOW_RULES` §6 lists technical identifiers and
#: status markers under *Skip*, and an em dash is typography rather than copy — it reads the
#: same in all nine locales. The *explanation* of what the Rows column counts is a different
#: string and does need nine keys; that is AC4, and it is not in this change.
ROWS_NOT_MEASURED = "—"


def _format_rows(value: int | None) -> str:
    """``None`` -> an em dash; a measured count -> its digits, **including zero**.

    🔑 The zero branch is the point. ``str(value or "")`` and
    ``value or ROWS_NOT_MEASURED`` both look right and both render a genuinely empty load as
    "not measured", which is this defect's mirror image.
    """
    return ROWS_NOT_MEASURED if value is None else str(value)


# What a row on /runs offers (core#657, SPEC_RUN_CANCELLATION §5.3). Derived from the model's
# status sets and computed here, in Python, so the template branches on two booleans and never
# on status literals — `runs.py` comparing to "pending"/"running" would be an eighth
# hand-maintained status list, which is the defect §4 exists to retire.
#
# Every status gets exactly one of three answers — cancel offered, stopping, or finished — and
# `TestWhatARowOffersIsDerived` fails for a status that gets none.


def can_offer_cancel(status: RunStatus) -> bool:
    """An active Cancel control: the run accepts a cancel and nobody has asked for one yet.

    ``CANCELLING`` is in ``CANCELLABLE_RUN_STATUSES`` because a second API cancel is the same
    request arriving twice (§5.2), not because the UI should offer it twice.
    """
    return status in CANCELLABLE_RUN_STATUSES and status not in CANCEL_REQUESTED_RUN_STATUSES


def is_stopping(status: RunStatus) -> bool:
    """A stop was asked for and the worker has not confirmed it: the badge reads *Stopping…*."""
    return status in CANCEL_REQUESTED_RUN_STATUSES and status in NON_TERMINAL_RUN_STATUSES


class RunItem(BaseModel):
    id: int = 0
    target_type: str = ""
    target_id: int = 0
    target_name: str = ""
    status: str = ""
    started_at: str = ""
    finished_at: str = ""
    #: Rendered text, not a number: the em dash when the count was never measured
    #: (core#1170 AC3). Formatted in the state rather than the template on purpose —
    #: an `rx.cond` in two pages would leave this var lying to every other reader,
    #: and `model_state` already made a decision on the coerced value.
    rows_loaded: str = ""
    error_message: str = ""
    logs: str = ""
    #: :func:`can_offer_cancel` — the row renders an active Cancel control.
    can_cancel: bool = False
    #: :func:`is_stopping` — the badge reads *Stopping…* and the control is disabled, not
    #: removed, because a control that vanishes reads as a failed click (§5.3).
    stopping: bool = False


class RunState(BaseState):
    runs: list[RunItem] = []
    filter_status: str = ""
    filter_target_type: str = ""
    selected_run_logs: str = ""
    selected_run_id: int = 0

    @rx.var
    def bills_usage(self) -> bool:
        """Whether the Cancel dialog says what is billed (``SPEC_RUN_CANCELLATION`` AC12).

        The open-source edition bills nobody, so the sentence renders only where it is true.
        """
        return _bills_usage()

    async def load_runs(self):
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        user_id = auth.current_user.id or 0
        if org_id == 0 or user_id == 0:
            return

        svc = ExecutionService()
        status_filter = RunStatus(self.filter_status) if self.filter_status else None
        target_type_filter = NodeType(self.filter_target_type) if self.filter_target_type else None

        # Build name lookups
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()
        pipeline_svc = PipelineService()

        with get_sync_session() as session:
            uploads = upload_svc.list_uploads(session, org_id)
            upload_names = {u.id: u.name for u in uploads}
            transformations = transform_svc.list_transformations(session, org_id)
            trans_names = {t.id: t.name for t in transformations}
            pipelines = pipeline_svc.list_pipelines(session, org_id)
            pipeline_names = {p.id: p.name for p in pipelines}

            rows = svc.list_runs(
                session,
                org_id,
                status=status_filter,
                target_type=target_type_filter,
                limit=100,
            )
            self.runs = [
                RunItem(
                    id=r.id,
                    target_type=r.target_type.value,
                    target_id=r.target_id,
                    target_name=self._resolve_target_name(
                        r.target_type.value,
                        r.target_id,
                        upload_names,
                        trans_names,
                        pipeline_names,
                    ),
                    status=r.status.value,
                    started_at=str(r.started_at) if r.started_at else "",
                    finished_at=str(r.finished_at) if r.finished_at else "",
                    rows_loaded=_format_rows(r.rows_loaded),
                    error_message=r.error_message or "",
                    logs=r.logs or "",
                    can_cancel=can_offer_cancel(r.status),
                    stopping=is_stopping(r.status),
                )
                for r in rows
            ]
        # core#1398: `/runs?run=<id>` opens that run's log. It is how `/models` links to the run
        # that explains a missing catalog entry. Only a run in this org's loaded list can open.
        requested = str(self.router.page.params.get("run", "") or "")
        if requested.isdigit():
            self._select_run(int(requested))
        self.error_message = ""

    @staticmethod
    def _resolve_target_name(
        target_type: str,
        target_id: int,
        upload_names: dict,
        trans_names: dict,
        pipeline_names: dict | None = None,
    ) -> str:
        if target_type == "upload":
            name = upload_names.get(target_id, f"#{target_id}")
            return f"upload: {name}"
        if target_type == "pipeline":
            name = (pipeline_names or {}).get(target_id, f"#{target_id}")
            return f"pipeline: {name}"
        name = trans_names.get(target_id, f"#{target_id}")
        return f"transformation: {name}"

    async def set_filter(self, status: str):
        self.filter_status = status
        await self.load_runs()

    async def set_target_type_filter(self, target_type: str):
        self.filter_target_type = target_type
        await self.load_runs()

    def _select_run(self, run_id: int) -> None:
        for r in self.runs:
            if r.id == run_id:
                self.selected_run_id = run_id
                self.selected_run_logs = r.logs or "(no logs)"
                return
        self.selected_run_id = 0
        self.selected_run_logs = ""

    def view_logs(self, run_id: int):
        self._select_run(run_id)

    async def cancel_run(self, run_id: int):
        """Stop a run from `/runs` (core#657, ``SPEC_RUN_CANCELLATION`` §5.3).

        Until this existed, a user with a runaway run had no way to stop it without an API key.

        🚨 **The role check is here as well as on the control, and both are mandatory.** Hiding
        the button from a ``viewer`` is the affordance; this is the enforcement (D5: cancelling
        is gated at ``editor``, the role that may start a run). The service repeats the check
        against the database (``assert_org_role``), so a role changed since sign-in is refused
        too.

        What the stop *did* is the service's answer and is said back to the user: a pending
        run is cancelled at once, a running one is ``cancelling`` until its worker finishes —
        and a run that finished first is refused rather than reported as stopped.
        """
        if not await self._check_role("editor"):
            return
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        user_id = auth.current_user.id or 0
        try:
            with get_sync_session() as session:
                run = ExecutionService().cancel_run(session, org_id, run_id, actor_user_id=user_id)
                outcome = run.status if run is not None else None
                session.commit()
        except Exception as exc:
            self._set_error(exc, "The run could not be cancelled")
            auth.action_error = self.error_message
            return
        await self.load_runs()
        if outcome is None:
            # Not in this org, or already terminal — `cancel_run` answers both with None, and
            # from this page the second is the one a member can reach: the run finished between
            # the page rendering its Cancel button and the click arriving.
            #
            # Written to `AuthState.action_error`, the channel `page_layout` renders, and not to
            # this state's own `error_message`, which no page reads (core#744, core#887).
            auth.action_error = await self._translated(
                "runs.cannot_cancel",
                "This run has already finished, so there is nothing to cancel.",
            )
            return
        self.error_message = ""
        auth.action_error = ""
        if outcome == RunStatus.CANCELLING:
            yield await self._saved_toast(
                "runs.stopping_toast",
                "Stopping. The run is marked cancelled when its current work ends.",
            )
        else:
            yield await self._saved_toast("runs.cancelled_toast", "Run cancelled")

    def close_logs(self):
        self.selected_run_id = 0
        self.selected_run_logs = ""

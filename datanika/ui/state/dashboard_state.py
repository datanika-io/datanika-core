"""Dashboard state — stats and recent runs."""

import reflex as rx
from pydantic import BaseModel

from datanika.config import settings
from datanika.hooks import emit
from datanika.models.run import RunStatus
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.schedule_service import ScheduleService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from datanika.ui.state.base_state import BaseState, get_sync_session
from datanika.ui.state.run_state import RunItem, _format_rows


def usage_context(org_id: int) -> dict:
    """The context dict the ``usage.get_summary`` hook is invited to fill.

    Core creates it, the cloud plugin fills it (``SPEC_USAGE_VISIBILITY`` §3.2). The values here
    are the **no-cloud-plugin reading**, not placeholders: an OSS deployment has no plan rows, so
    every allowance is legitimately zero and no meter is drawn.

    It is a function rather than a literal inside :meth:`DashboardState.load_dashboard` so the
    contract can be asserted. A field the card renders but this dict never offers is a field
    cloud has no way to supply — which is core#1513's defect one level down: the byte fields
    were offered here all along and simply never assigned.
    """
    return {
        "org_id": org_id,
        "runs_used": 0,
        "runs_limit": 0,
        "plan_name": "",
        "bytes_used": 0,
        "bytes_limit": 0,
        #: ``True`` when exceeding ``bytes_limit`` STOPS runs, ``False`` when it bills overage
        #: (core#713). The two must not be shown the same way — ``SPEC_USAGE_VISIBILITY`` §2.4.
        "bytes_hard_cap": False,
        #: The overage sentence's three figures, **pre-formatted by whoever owns the pricing
        #: arithmetic** — i.e. the biller, not this page. ``SPEC_USAGE_VISIBILITY`` §2.5: every
        #: published figure is read from the plan row, never recomputed in the page. Core paints
        #: them into the locale's own template; it does not know the rate and must not guess a
        #: total (the biller bills a *started* GB, so a floor division here would disagree with
        #: the invoice). Empty means "not supplied", and the overage line is then not drawn at
        #: all — silence beats a fabricated ``$0.00``.
        #:
        #: ⚠️ **Bare numbers, no unit and no currency symbol**: the locale template already
        #: supplies both (``"Overage: {gb} GB x ${rate} = ${total} this month"``), so ``"3"``
        #: and ``"0.50"``, never ``"3 GB"`` or ``"$0.50"``.
        "bytes_overage_gb": "",
        "bytes_overage_rate": "",
        "bytes_overage_total": "",
        "cycle_ends_at": "",
    }


class DashboardStats(BaseModel):
    total_uploads: int = 0
    total_transformations: int = 0
    total_pipelines: int = 0
    total_schedules: int = 0
    recent_runs_success: int = 0
    recent_runs_failed: int = 0
    recent_runs_total: int = 0


class DashboardState(BaseState):
    stats: DashboardStats = DashboardStats()
    recent_runs: list[RunItem] = []

    # Usage bar (populated via hook — zero when cloud plugin not active)
    runs_used: int = 0
    runs_limit: int = 0
    plan_name: str = ""

    # Volume (bytes) usage — the BILLED dimension. Populated by the cloud plugin via
    # `usage.get_summary`; zero when no plugin is active or the plan carries no volume
    # allowance. No longer gated on settings.datanika_dual_mode_ux_enabled (core#1513):
    # that flag also mounts the ETL/ELT mode selector, which persists nothing.
    bytes_used: int = 0
    bytes_limit: int = 0
    #: True when the volume allowance is a WALL (runs stop), False when exceeding it bills
    #: overage. Free is the only hard-capped tier and was the only one with no meter.
    bytes_hard_cap: bool = False
    #: Pre-formatted overage figures from the biller — see :func:`usage_context`.
    bytes_overage_gb: str = ""
    bytes_overage_rate: str = ""
    bytes_overage_total: str = ""

    # Billing cycle close date (ISO YYYY-MM-DD). Populated via
    # usage.get_summary hook; empty string when cloud plugin is not
    # active or subscription has no renews_at yet.
    cycle_ends_at: str = ""

    @rx.var
    def runs_percent(self) -> int:
        if self.runs_limit <= 0:
            return 0
        return min(int(self.runs_used / self.runs_limit * 100), 100)

    @rx.var
    def runs_color(self) -> str:
        pct = self.runs_percent
        if pct >= 80:
            return "red"
        if pct >= 60:
            return "yellow"
        return "green"

    @rx.var
    def has_usage_data(self) -> bool:
        return self.runs_limit > 0

    @rx.var
    def bytes_percent(self) -> int:
        if self.bytes_limit <= 0:
            return 0
        return min(int(self.bytes_used / self.bytes_limit * 100), 100)

    @rx.var
    def bytes_color(self) -> str:
        pct = self.bytes_percent
        if pct >= 80:
            return "red"
        if pct >= 60:
            return "yellow"
        return "green"

    @rx.var
    def has_volume_data(self) -> bool:
        return self.bytes_limit > 0

    @rx.var
    def has_any_usage_data(self) -> bool:
        """Whether the usage card is shown at all.

        ``SPEC_USAGE_VISIBILITY`` §2.1: a dimension the plan row carries is a dimension the
        customer sees. Gating the whole card on the RUNS allowance hid the volume meter from any
        plan with a ``NULL`` ``runs_included`` — so one dimension must never gate the other.

        Reads the raw fields rather than the two vars above because a computed var is resolved
        through the state machinery; ``test_usage_is_visible`` pins this against them.
        """
        return self.runs_limit > 0 or self.bytes_limit > 0

    @rx.var
    def shows_both_dimensions(self) -> bool:
        """True only when both meters are drawn — i.e. when a separator between them is real."""
        return self.runs_limit > 0 and self.bytes_limit > 0

    @rx.var
    def has_overage_figures(self) -> bool:
        """Whether the biller supplied every number the overage sentence names.

        The template is ``"Overage: {gb} GB x ${rate} = ${total} this month"``. Painting it with
        any figure missing produces either a visible ``{gb}`` or a confident ``$0.00`` — and
        ``$0.00`` is the worse of the two, because it says *free* on the one screen whose job is
        to say what something costs. All three or none.
        """
        return bool(self.bytes_overage_gb and self.bytes_overage_rate and self.bytes_overage_total)

    @rx.var
    def bytes_used_display(self) -> str:
        gb = self.bytes_used / (1024**3)
        return f"{gb:.1f} GB"

    @rx.var
    def bytes_limit_display(self) -> str:
        gb = self.bytes_limit / (1024**3)
        return f"{gb:.0f} GB"

    # The two above carry the unit, which is right for a labelled row ("Volume included:
    # 10 GB"). The two below do NOT, because they are substituted into a template that already
    # supplies it — `quota.volume_usage` is "{used} / {limit} GB processed this month", so
    # feeding it the unit-bearing form yields "10 GB GB processed". Same divisor, same
    # precision; only the unit differs, and `test_usage_is_visible` pins that pairing.
    @rx.var
    def bytes_used_gb(self) -> str:
        return f"{self.bytes_used / (1024**3):.1f}"

    @rx.var
    def bytes_limit_gb(self) -> str:
        return f"{self.bytes_limit / (1024**3):.0f}"

    @rx.var
    def has_cycle_end(self) -> bool:
        """True when cloud plugin populated a cycle close date."""
        return bool(self.cycle_ends_at)

    @rx.var
    def cycle_days_remaining(self) -> int:
        """Days until billing cycle closes; -1 when date is unknown.

        Computed at render time so the countdown reflects the current
        wall-clock without requiring a scheduled refresh. Negative
        when the stored cycle end is already in the past (cloud
        plugin will have rolled to the next cycle on next
        ``usage.get_summary`` fetch).
        """
        if not self.cycle_ends_at:
            return -1
        from datetime import date

        try:
            end = date.fromisoformat(self.cycle_ends_at)
        except ValueError:
            return -1
        today = date.today()
        return (end - today).days

    async def load_dashboard(self):
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        user_id = auth.current_user.id or 0
        if org_id == 0 or user_id == 0:
            return

        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()
        pipeline_svc = PipelineService()
        schedule_svc = ScheduleService(upload_svc, transform_svc)
        exec_svc = ExecutionService()

        with get_sync_session() as session:
            uploads = upload_svc.list_uploads(session, org_id)
            transformations = transform_svc.list_transformations(session, org_id)
            pipelines = pipeline_svc.list_pipelines(session, org_id)
            schedules = schedule_svc.list_schedules(session, org_id)

            upload_names = {u.id: u.name for u in uploads}
            trans_names = {t.id: t.name for t in transformations}
            pipeline_names = {p.id: p.name for p in pipelines}

            recent = exec_svc.list_runs(session, org_id, limit=10)
            success_count = sum(1 for r in recent if r.status == RunStatus.SUCCESS)
            failed_count = sum(1 for r in recent if r.status == RunStatus.FAILED)

            self.stats = DashboardStats(
                total_uploads=len(uploads),
                total_transformations=len(transformations),
                total_pipelines=len(pipelines),
                total_schedules=len(schedules),
                recent_runs_success=success_count,
                recent_runs_failed=failed_count,
                recent_runs_total=len(recent),
            )
            self.recent_runs = [
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
                )
                for r in recent
            ]

        # Load usage data via hook (cloud plugin fills this in)
        usage_ctx = usage_context(org_id)
        emit("usage.get_summary", context=usage_ctx)
        self.runs_used = usage_ctx["runs_used"]
        self.runs_limit = usage_ctx["runs_limit"]
        self.plan_name = usage_ctx["plan_name"]
        self.bytes_used = usage_ctx["bytes_used"]
        self.bytes_limit = usage_ctx["bytes_limit"]
        # `.get` rather than `[...]`: a cloud build predating core#1513 replaces the dict
        # wholesale in some handlers, and a missing cap flag must read as "not a wall" rather
        # than raise — the safe direction, since the overage wording promises nothing is blocked.
        self.bytes_hard_cap = bool(usage_ctx.get("bytes_hard_cap", False))
        self.bytes_overage_gb = usage_ctx.get("bytes_overage_gb", "")
        self.bytes_overage_rate = usage_ctx.get("bytes_overage_rate", "")
        self.bytes_overage_total = usage_ctx.get("bytes_overage_total", "")
        self.cycle_ends_at = usage_ctx.get("cycle_ends_at", "")

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

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
from datanika.ui.state.run_state import RunItem


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

    # Volume (bytes) usage — V2 pricing pivot dual-dim bar, gated by
    # settings.datanika_dual_mode_ux_enabled at the component level.
    bytes_used: int = 0
    bytes_limit: int = 0

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
    def bytes_used_display(self) -> str:
        gb = self.bytes_used / (1024**3)
        return f"{gb:.1f} GB"

    @rx.var
    def bytes_limit_display(self) -> str:
        gb = self.bytes_limit / (1024**3)
        return f"{gb:.0f} GB"

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
        org_id = await self._get_org_id()
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
                    rows_loaded=r.rows_loaded or 0,
                    error_message=r.error_message or "",
                )
                for r in recent
            ]

        # Load usage data via hook (cloud plugin fills this in)
        usage_ctx = {
            "org_id": org_id,
            "runs_used": 0,
            "runs_limit": 0,
            "plan_name": "",
            "bytes_used": 0,
            "bytes_limit": 0,
            "cycle_ends_at": "",
        }
        emit("usage.get_summary", context=usage_ctx)
        self.runs_used = usage_ctx["runs_used"]
        self.runs_limit = usage_ctx["runs_limit"]
        self.plan_name = usage_ctx["plan_name"]
        self.bytes_used = usage_ctx["bytes_used"]
        self.bytes_limit = usage_ctx["bytes_limit"]
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

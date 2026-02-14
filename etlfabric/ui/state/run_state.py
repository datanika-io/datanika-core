"""Run state for Reflex UI."""

import reflex as rx

from etlfabric.models.run import RunStatus
from etlfabric.services.execution_service import ExecutionService
from etlfabric.ui.state.base_state import BaseState, get_sync_session


class RunItem(rx.Base):
    id: int = 0
    target_type: str = ""
    target_id: int = 0
    status: str = ""
    started_at: str = ""
    finished_at: str = ""
    rows_loaded: int = 0
    error_message: str = ""


class RunState(BaseState):
    runs: list[RunItem] = []
    filter_status: str = ""

    def load_runs(self):
        svc = ExecutionService()
        status_filter = RunStatus(self.filter_status) if self.filter_status else None
        with get_sync_session() as session:
            rows = svc.list_runs(session, self.org_id, status=status_filter, limit=100)
            self.runs = [
                RunItem(
                    id=r.id,
                    target_type=r.target_type.value,
                    target_id=r.target_id,
                    status=r.status.value,
                    started_at=str(r.started_at) if r.started_at else "",
                    finished_at=str(r.finished_at) if r.finished_at else "",
                    rows_loaded=r.rows_loaded or 0,
                    error_message=r.error_message or "",
                )
                for r in rows
            ]
        self.error_message = ""

    def set_filter(self, status: str):
        self.filter_status = status
        self.load_runs()

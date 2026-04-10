"""Getting Started checklist state — 5 derived steps + per-user dismissal."""

from datetime import UTC, datetime

import reflex as rx

from datanika.models.user import User
from datanika.services.onboarding import ChecklistProgress, load_checklist_for_org
from datanika.ui.state.base_state import BaseState, get_sync_session


class OnboardingState(BaseState):
    progress: ChecklistProgress = ChecklistProgress()
    dismissed: bool = False
    loaded: bool = False

    @rx.var
    def visible(self) -> bool:
        return self.loaded and not self.dismissed

    @rx.var
    def done_count(self) -> int:
        return self.progress.done_count

    @rx.var
    def total_count(self) -> int:
        return self.progress.total

    @rx.var
    def all_done(self) -> bool:
        return self.progress.all_done

    async def load_checklist(self):
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        user_id = auth.current_user.id or 0

        if org_id == 0 or user_id == 0:
            self.loaded = False
            return

        with get_sync_session() as session:
            self.progress = load_checklist_for_org(session, org_id)
            user = session.get(User, user_id)
            self.dismissed = bool(user and user.onboarding_checklist_dismissed_at is not None)

        self.loaded = True

    async def dismiss_checklist(self):
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        user_id = auth.current_user.id or 0
        if user_id == 0:
            return

        with get_sync_session() as session:
            user = session.get(User, user_id)
            if user is None:
                return
            user.onboarding_checklist_dismissed_at = datetime.now(UTC)
            session.commit()

        self.dismissed = True

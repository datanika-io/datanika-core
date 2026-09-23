"""A Cancel control on `/runs` (core#657, ``SPEC_RUN_CANCELLATION`` §5.3, AC9).

*"A user with a runaway run currently has no way to stop it without an API key."* The spec calls
that the launch blocker, and held the button back until the mechanism it calls existed (§8: *do
not ship the button before the mechanism*). The mechanism is ``ExecutionService.cancel_run`` with
the ``cancelling`` state; this is the button.

Three requirements from §5.3, each asserted by behaviour where behaviour is observable:

* **The affordance and the enforcement are separate, and both are mandatory.** The control
  renders under ``AuthState.can_edit``, and the handler refuses a ``viewer`` on its own. AC7 asks
  for *"the runtime denial, not the source"*, so the viewer test drives the real ``_check_role``
  against a real run and reads the row afterwards — with an editor on the same fixture as the
  control, so the refusal is not a fixture that nobody could cancel.
* **While ``cancelling`` the badge reads Stopping… and the control is disabled, not removed** —
  a control that vanishes reads as a failed click.
* **What a row offers is derived from the model's status sets**, never from literals in the
  template. `runs.py` hard-coding `"pending"`/`"running"` would be the eighth hand-maintained
  status list §4 exists to retire.
"""

from __future__ import annotations

import ast
import inspect
import uuid
from unittest.mock import MagicMock, patch

import pytest

import datanika.ui.state.auth_state as auth_state_module
import datanika.ui.state.run_state as run_state_module
from datanika.models.dependency import NodeType
from datanika.models.run import (
    NON_TERMINAL_RUN_STATUSES,
    TERMINAL_RUN_STATUSES,
    Run,
    RunStatus,
)
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.ui.components import run_status as run_status_component
from datanika.ui.components.run_status import RUN_STATUS_COLORS
from datanika.ui.pages import runs as runs_page
from datanika.ui.state.auth_state import OrgInfo, UserInfo
from datanika.ui.state.base_state import BaseState
from datanika.ui.state.run_state import RunState, can_offer_cancel, is_stopping
from tests.factories import make_user
from tests.test_ui.test_models_empty_state_after_a_load import _session_patch
from tests.test_ui.test_rbac_ui_visibility import _walk_ui, ungated_controls

SECRET = "test-secret-key-for-the-runs-cancel-control"
REFUSAL = "SENTINEL-editor-refusal"


# ---------------------------------------------------------------------------------------------
# What each row offers
# ---------------------------------------------------------------------------------------------


class TestWhatARowOffersIsDerived:
    @pytest.mark.parametrize("status", list(RunStatus))
    def test_every_status_gets_exactly_one_of_the_three_answers(self, status):
        """Totality, the same shape as §4's guard: a status added later that is in none of the
        three (a run mid-rollback, say) fails here until somebody decides what its row offers,
        instead of rendering no control and no badge by accident."""
        answers = [can_offer_cancel(status), is_stopping(status), status in TERMINAL_RUN_STATUSES]
        assert answers.count(True) == 1, (status, answers)

    def test_a_pending_and_a_running_run_offer_cancel(self):
        assert can_offer_cancel(RunStatus.PENDING)
        assert can_offer_cancel(RunStatus.RUNNING)

    def test_a_cancelling_run_is_stopping_and_offers_no_second_request(self):
        assert is_stopping(RunStatus.CANCELLING)
        assert not can_offer_cancel(RunStatus.CANCELLING)


# ---------------------------------------------------------------------------------------------
# The handler — real run, real role gate, real service
# ---------------------------------------------------------------------------------------------


def _auth_stand_in(role: str, org_id: int, user_id: int):
    """An ``AuthState`` stand-in with its real field defaults (as test_connection_test_role_gate).

    ⚠️ A bare ``MagicMock`` answers every attribute truthily, so a guard like
    ``if not auth.session_expired`` is never taken and the test measures nothing.
    """
    st = MagicMock()
    for name, field in auth_state_module.AuthState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    svc = AuthService(SECRET)
    st.access_token = svc.create_access_token(user_id, org_id, expires_minutes=10)
    st.refresh_token = svc.create_refresh_token(user_id)
    st.current_user = UserInfo(id=user_id, email="a@b.c", full_name="A")
    st.current_org = OrgInfo(id=org_id, name="Org", slug="org")
    st.current_role = role
    st.action_error = ""
    st.translations = {"errors.role_required_editor": REFUSAL}
    st._revalidate_session = lambda: auth_state_module.AuthState._revalidate_session(st)
    st._clear_session = lambda: auth_state_module.AuthState._clear_session(st)
    st._get_user_service = lambda: auth_state_module.AuthState._get_user_service(st)
    return st


@pytest.fixture
def org_with_members(db_session):
    org = Organization(name="Acme", slug=f"acme-cancel-ui-{uuid.uuid4().hex[:8]}")
    db_session.add(org)
    db_session.flush()
    members = {}
    for role in (MemberRole.EDITOR, MemberRole.VIEWER):
        user = make_user(db_session, email=f"{role.value}-{org.id}@test.io", password_hash="x")
        db_session.add(Membership(user_id=user.id, org_id=org.id, role=role))
        members[role.value] = user.id
    db_session.flush()
    return org.id, members


@pytest.fixture
def run_state_as(monkeypatch, db_session):
    """A real ``RunState`` whose session carries ``role``, reading and writing the test database."""

    def build(role: str, org_id: int, user_id: int):
        auth = _auth_stand_in(role, org_id, user_id)

        async def _get_state(self, state_cls):
            return auth

        monkeypatch.setattr(RunState, "get_state", _get_state)
        monkeypatch.setattr(auth_state_module.settings, "secret_key", SECRET)
        monkeypatch.setattr(
            run_state_module, "get_sync_session", lambda: _session_patch(db_session)
        )
        monkeypatch.setattr(run_state_module, "EncryptionService", MagicMock())
        state = RunState(parent_state=BaseState(init_substates=False), init_substates=False)
        return state, auth

    return build


def _run(session, org_id: int, status: RunStatus) -> Run:
    run = Run(org_id=org_id, target_type=NodeType.UPLOAD, target_id=1, status=status)
    session.add(run)
    session.flush()
    return run


async def _press_stop(state, run_id: int) -> list:
    return [event async for event in RunState.cancel_run.fn(state, run_id) if event is not None]


def _status_of(session, run: Run) -> RunStatus:
    session.expire(run)
    return session.get(Run, run.id).status


class TestTheHandler:
    @pytest.mark.asyncio
    async def test_an_editor_stopping_a_running_run_leaves_it_cancelling(
        self, db_session, org_with_members, run_state_as
    ):
        org_id, members = org_with_members
        run = _run(db_session, org_id, RunStatus.RUNNING)
        state, auth = run_state_as("editor", org_id, members["editor"])

        events = await _press_stop(state, run.id)

        assert _status_of(db_session, run) == RunStatus.CANCELLING
        assert auth.action_error == ""
        assert events, "no acknowledgement — the user's next move is to click again"

    @pytest.mark.asyncio
    async def test_an_editor_cancelling_a_pending_run_ends_it_at_once(
        self, db_session, org_with_members, run_state_as
    ):
        org_id, members = org_with_members
        run = _run(db_session, org_id, RunStatus.PENDING)
        state, _ = run_state_as("editor", org_id, members["editor"])

        await _press_stop(state, run.id)

        assert _status_of(db_session, run) == RunStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_a_viewer_is_refused_at_the_handler_and_the_run_is_untouched(
        self, db_session, org_with_members, run_state_as
    ):
        """AC7: the runtime denial. The editor tests above are this one's control — same org,
        same kind of run — so a pass here is not a fixture nobody could cancel."""
        org_id, members = org_with_members
        run = _run(db_session, org_id, RunStatus.RUNNING)
        state, auth = run_state_as("viewer", org_id, members["viewer"])

        events = await _press_stop(state, run.id)

        assert _status_of(db_session, run) == RunStatus.RUNNING
        assert auth.action_error == REFUSAL, "the refusal must reach the banner the shell renders"
        assert not events, "a refused stop must not toast as if it happened"

    @pytest.mark.asyncio
    async def test_a_finished_run_is_left_alone_and_says_so(
        self, db_session, org_with_members, run_state_as
    ):
        org_id, members = org_with_members
        run = _run(db_session, org_id, RunStatus.SUCCESS)
        state, auth = run_state_as("editor", org_id, members["editor"])

        events = await _press_stop(state, run.id)

        assert _status_of(db_session, run) == RunStatus.SUCCESS
        assert auth.action_error, "a stop that did nothing must not be silent"
        assert not events

    @pytest.mark.asyncio
    async def test_another_orgs_run_is_not_touched(
        self, db_session, org_with_members, run_state_as
    ):
        org_id, members = org_with_members
        other = Organization(name="Other", slug=f"other-cancel-ui-{uuid.uuid4().hex[:8]}")
        db_session.add(other)
        db_session.flush()
        foreign = _run(db_session, other.id, RunStatus.RUNNING)
        state, _ = run_state_as("editor", org_id, members["editor"])

        await _press_stop(state, foreign.id)

        assert _status_of(db_session, foreign) == RunStatus.RUNNING

    @pytest.mark.asyncio
    async def test_a_stop_is_audited_with_who_and_what_it_did(
        self, db_session, org_with_members, run_state_as
    ):
        """core#657: a UI run TRIGGER writes an audit row and a stop wrote none, so "who stopped
        this run?" had no answer. The row records the transition the stop actually made."""
        from sqlalchemy import select

        from datanika.models.audit_log import AuditAction, AuditLog

        org_id, members = org_with_members
        run = _run(db_session, org_id, RunStatus.RUNNING)
        state, _ = run_state_as("editor", org_id, members["editor"])

        await _press_stop(state, run.id)

        rows = db_session.execute(
            select(AuditLog).where(AuditLog.org_id == org_id, AuditLog.resource_type == "run")
        ).scalars().all()
        assert [(r.action, r.resource_id, r.user_id) for r in rows] == [
            (AuditAction.UPDATE, run.id, members["editor"])
        ]
        assert rows[0].old_values == {"status": "running"}
        assert rows[0].new_values == {"status": "cancelling"}

    @pytest.mark.asyncio
    async def test_control_a_refused_stop_writes_no_audit_row(
        self, db_session, org_with_members, run_state_as
    ):
        from sqlalchemy import select

        from datanika.models.audit_log import AuditLog

        org_id, members = org_with_members
        run = _run(db_session, org_id, RunStatus.SUCCESS)
        state, _ = run_state_as("editor", org_id, members["editor"])

        await _press_stop(state, run.id)

        assert not db_session.execute(
            select(AuditLog).where(AuditLog.org_id == org_id, AuditLog.resource_type == "run")
        ).scalars().all()

    def test_the_role_check_is_the_first_thing_the_handler_does(self):
        """Anything before it runs for a member who is about to be refused."""
        tree = ast.parse(inspect.getsource(RunState.cancel_run.fn).lstrip())
        fn = tree.body[0]
        body = [
            s
            for s in fn.body
            if not isinstance(s, ast.Expr) or not isinstance(s.value, ast.Constant)
        ]
        first = ast.unparse(body[0])
        assert "self._check_role('editor')" in first, first


# ---------------------------------------------------------------------------------------------
# The page
# ---------------------------------------------------------------------------------------------


def _walk(component) -> list:
    out = [component]
    for child in getattr(component, "children", []) or []:
        out.extend(_walk(child))
    return out


def _tags(component) -> list[str]:
    return [type(c).__name__ for c in _walk(component)]


class TestThePage:
    def test_the_control_is_a_confirmation_dialog_with_a_way_out(self):
        tags = _tags(runs_page.runs_table())
        assert any("AlertDialog" in t for t in tags), "Cancel is not behind a confirmation"
        assert any("AlertDialogCancel" in t for t in tags), "no way to back out"
        assert any("AlertDialogAction" in t for t in tags), "no confirm control"

    def test_the_handler_hangs_off_the_confirm_button_not_the_trigger(self):
        source = inspect.getsource(runs_page)
        segment = source[source.index("def _cancel_dialog") :]
        trigger = segment.index("alert_dialog.trigger")
        action = segment.index("alert_dialog.action")
        handler = segment.index("RunState.cancel_run")
        assert trigger < action < handler, "the stop fires before the user has confirmed it"

    def test_the_dialog_names_the_run_and_labels_both_buttons_by_outcome(self):
        """§5.3: never a bare "Are you sure?" — name the target, say Stop / Keep running."""
        source = inspect.getsource(runs_page)
        segment = source[source.index("def _cancel_dialog") : source.index("def _cancel_control")]
        for needle in ("r.id", "r.target_name", '"runs.cancel_confirm"', '"runs.cancel_keep"'):
            assert needle in segment, needle
        assert '"runs.cancel_body"' in segment, "the dialog must carry D3's sentence (AC10)"
        assert '"runs.cancel_billing"' in segment, "and AC12's, where billing applies"

    def test_the_trigger_is_a_labelled_button_not_an_icon(self):
        """core#1409: an icon-only button inside an `alert_dialog.trigger` is exactly the shape
        axe reports as `button-name` + `aria-allowed-attr`. This control must not add one."""
        source = inspect.getsource(runs_page)
        segment = source[source.index("def _cancel_dialog") : source.index("def _cancel_control")]
        trigger = segment[
            segment.index("alert_dialog.trigger") : segment.index("alert_dialog.content")
        ]
        assert "rx.button(" in trigger and '_t["runs.cancel"]' in trigger, trigger
        assert "icon_button" not in trigger, trigger

    def test_the_row_reads_the_derived_fields_not_status_literals(self):
        source = inspect.getsource(runs_page)
        segment = source[source.index("def _cancel_control") :]
        segment = segment[: segment.index("\ndef ")]
        assert "r.can_cancel" in segment and "r.stopping" in segment, segment

    def test_a_stopping_row_keeps_a_disabled_control(self):
        source = inspect.getsource(runs_page)
        segment = source[source.index("def _cancel_control") :]
        segment = segment[: segment.index("\ndef ")]
        stopping_branch = segment[segment.index("r.stopping") :]
        assert "disabled=True" in stopping_branch, "a control that vanishes reads as a failed click"

    def test_the_badge_reads_stopping_while_cancelling(self):
        badge = inspect.getsource(runs_page._status_badge)
        assert "r.stopping" in badge and '_t["runs.stopping"]' in badge, badge
        assert "_status_badge(r)" in inspect.getsource(runs_page.runs_table)


class TestTheControlIsGatedForEditors:
    """The repository's gate scan already fails an ungated role-declaring control; these two
    tests make sure it is looking at THIS one, so its green is about the Cancel control."""

    def test_the_scan_sees_the_cancel_control(self):
        seen = [
            (w.module, handler)
            for w in _walk_ui()
            for _m, _fn, state, handler, _gates, _line in w.controls
            if state == "RunState" and handler == "cancel_run"
        ]
        assert seen, "the gate scan cannot see RunState.cancel_run — its green says nothing"

    def test_the_cancel_control_is_behind_an_editor_gate(self):
        offenders = [v for v in ungated_controls() if v[3] == "cancel_run"]
        assert not offenders, offenders


# ---------------------------------------------------------------------------------------------
# Colours — one map, total over the enum
# ---------------------------------------------------------------------------------------------


class TestOneColourMap:
    def test_the_map_is_total_over_run_status(self):
        assert set(RUN_STATUS_COLORS) == set(RunStatus)

    def test_cancelling_is_not_the_colour_of_pending(self):
        """§5.3: falling through to `gray` made a stopping run look queued."""
        assert RUN_STATUS_COLORS[RunStatus.CANCELLING] != RUN_STATUS_COLORS[RunStatus.PENDING]

    @pytest.mark.parametrize("page", ["runs", "dashboard", "models"])
    def test_every_page_that_badges_a_run_status_uses_the_one_map(self, page):
        module = __import__(f"datanika.ui.pages.{page}", fromlist=["_"])
        assert "run_status_color(" in inspect.getsource(module), page

    @pytest.mark.parametrize("page", ["pipelines", "uploads"])
    def test_the_run_button_reads_in_progress_from_the_derived_set(self, page):
        """`(status == "running") | (status == "pending")` said a `cancelling` run — whose worker
        is still busy — was idle. The in-progress colour now covers every non-terminal status."""
        module = __import__(f"datanika.ui.pages.{page}", fromlist=["_"])
        assert "run_in_progress_color(" in inspect.getsource(module), page

    def test_the_in_progress_arms_are_the_non_terminal_set(self):
        arms = run_status_component.in_progress_values()
        assert set(arms) == {s.value for s in NON_TERMINAL_RUN_STATUSES}


# ---------------------------------------------------------------------------------------------
# The billing line follows the edition
# ---------------------------------------------------------------------------------------------


class TestTheBillingLineFollowsTheEdition:
    @pytest.mark.parametrize(("edition", "expected"), [("cloud", True), ("core", False)])
    def test_bills_usage(self, monkeypatch, edition, expected):
        from datanika.services import run_cancellation

        monkeypatch.setattr(run_cancellation.settings, "datanika_edition", edition)
        with patch.object(run_state_module, "EncryptionService", MagicMock()):
            state = RunState(parent_state=BaseState(init_substates=False), init_substates=False)
            assert RunState.bills_usage.fget(state) is expected

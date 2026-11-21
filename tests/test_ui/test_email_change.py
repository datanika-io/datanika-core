"""Email change on /settings and /confirm-email-change (SPEC_EMAIL_CHANGE).

The resend control reads the address off the user row. This one mails an
address the form names, so the live row must not move until the link is
followed, and a rate-limit refusal has to be a screen the user can see.
"""

from __future__ import annotations

import ast
import pathlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import datanika.ui.state.account_state as acc
import datanika.ui.state.email_change_state as confirm_mod
from datanika.models.pii import UserPII
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.email_change_service import EmailChangeService
from datanika.services.user_service import UserService
from datanika.ui.state.account_state import AccountState
from datanika.ui.state.email_change_state import EmailChangeState

PAGES = pathlib.Path(acc.__file__).resolve().parents[1] / "pages"


@pytest.fixture
def svc():
    return UserService(AuthService("test-secret-key-for-email-change"))


@pytest.fixture
def user(db_session, svc):
    created = svc.register_user(db_session, "alice@example.com", "correct horse", "Alice")
    org = Organization(name="Alice Org", slug=f"alice-{created.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=created.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.flush()
    return created


def _state(user_id=0, org_id=0):
    st = MagicMock()
    for name, field in AccountState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    auth = MagicMock()
    auth.current_user.id = user_id
    auth.current_org.id = org_id
    st.get_state = AsyncMock(return_value=auth)
    st._require_live_session = AsyncMock(return_value=True)
    st._service = lambda: AccountState._service(st)
    st._email_changes = lambda: AccountState._email_changes(st)
    return st


class _TestSession:
    def __init__(self, session):
        self._session = session

    def commit(self):
        self._session.flush()

    def __getattr__(self, name):
        return getattr(self._session, name)


def _session_patch(db_session):
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=_TestSession(db_session))
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx


def _allowing_limiter():
    limiter = MagicMock()
    limiter.check_window.return_value = MagicMock(allowed=True, retry_after=0)
    return limiter


def _refusing_limiter():
    limiter = MagicMock()
    limiter.check_window.return_value = MagicMock(allowed=False, retry_after=60)
    return limiter


async def _request(st, db_session, form, *, limiter=None, smtp="smtp.test"):
    st._rate_limiter = lambda: limiter if limiter is not None else _allowing_limiter()
    delay = MagicMock()
    with (
        patch.object(acc, "get_sync_session", return_value=_session_patch(db_session)),
        patch.object(acc.settings, "smtp_host", smtp),
        patch("datanika.tasks.email_tasks.send_email_change_email_task") as task,
    ):
        task.delay = delay
        await AccountState.request_email_change.fn(st, form)
    return delay


def _pii(db_session, user_id) -> UserPII:
    return db_session.get(UserPII, user_id)


class TestTheLiveAddressDoesNotMove:
    @pytest.mark.asyncio
    async def test_an_unverified_account_can_request(self, db_session, user):
        assert user.email_verified is False
        st = _state(user.id, 1)
        delay = await _request(st, db_session, {"new_email": "new@example.com"})
        db_session.refresh(user)
        assert _pii(db_session, user.id).email == "alice@example.com"
        assert _pii(db_session, user.id).pending_email == "new@example.com"
        assert st.email_change_state == "queued"
        assert delay.call_count == 1
        assert delay.call_args.args[0] == "new@example.com"

    @pytest.mark.asyncio
    async def test_a_verified_account_can_request_too(self, db_session, user):
        """The control is not the resend button. Verified accounts use it."""
        user.email_verified = True
        db_session.flush()
        st = _state(user.id, 1)
        delay = await _request(st, db_session, {"new_email": "new@example.com"})
        assert delay.call_count == 1
        assert st.email_change_state == "queued"

    def test_the_handler_does_not_read_email_verified(self):
        source = pathlib.Path(acc.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        fn = next(
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.AsyncFunctionDef) and n.name == "request_email_change"
        )
        body = ast.get_source_segment(source, fn) or ""
        assert "email_verified" not in body


class TestEveryOutcomeIsDistinguishable:
    @pytest.mark.asyncio
    async def test_no_relay_writes_nothing_and_does_not_count(self, db_session, user):
        limiter = _allowing_limiter()
        st = _state(user.id, 1)
        delay = await _request(
            st, db_session, {"new_email": "new@example.com"}, limiter=limiter, smtp=""
        )
        assert st.email_change_state == "no_relay"
        assert delay.call_count == 0
        assert limiter.check_window.call_count == 0
        assert _pii(db_session, user.id).pending_email is None

    @pytest.mark.asyncio
    async def test_a_refusal_does_not_send_and_is_its_own_state(self, db_session, user):
        st = _state(user.id, 1)
        delay = await _request(
            st,
            db_session,
            {"new_email": "new@example.com"},
            limiter=_refusing_limiter(),
        )
        assert st.email_change_state == "rate_limited"
        assert delay.call_count == 0
        assert _pii(db_session, user.id).pending_email is None

    @pytest.mark.asyncio
    async def test_a_limiter_outage_does_not_send(self, db_session, user):
        limiter = MagicMock()
        limiter.check_window.side_effect = RuntimeError("redis down")
        st = _state(user.id, 1)
        delay = await _request(st, db_session, {"new_email": "new@example.com"}, limiter=limiter)
        assert delay.call_count == 0
        assert st.email_change_state == "rate_limited"

    @pytest.mark.asyncio
    async def test_both_buckets_are_consulted(self, db_session, user):
        limiter = _allowing_limiter()
        st = _state(user.id, 1)
        await _request(st, db_session, {"new_email": "new@example.com"}, limiter=limiter)
        buckets = [call.args[0] for call in limiter.check_window.call_args_list]
        assert EmailChangeService.pair_bucket(user.id, "new@example.com") in buckets
        assert EmailChangeService.user_bucket(user.id) in buckets
        assert all("new@example.com" not in bucket for bucket in buckets)

    @pytest.mark.asyncio
    async def test_a_taken_address_is_refused_on_screen(self, db_session, user, svc):
        other = svc.register_user(db_session, "bob@example.com", "correct horse", "Bob")
        org = Organization(name="Bob Org", slug=f"bob-{other.id}")
        db_session.add(org)
        db_session.flush()
        db_session.add(Membership(user_id=other.id, org_id=org.id, role=MemberRole.OWNER))
        db_session.flush()
        st = _state(user.id, 1)
        delay = await _request(st, db_session, {"new_email": "bob@example.com"})
        assert st.email_change_state == "address_taken"
        assert delay.call_count == 0
        assert _pii(db_session, user.id).pending_email is None

    @pytest.mark.asyncio
    async def test_enqueue_failure_leaves_the_address_alone(self, db_session, user):
        st = _state(user.id, 1)
        st._rate_limiter = lambda: _allowing_limiter()
        with (
            patch.object(acc, "get_sync_session", return_value=_session_patch(db_session)),
            patch.object(acc.settings, "smtp_host", "smtp.test"),
            patch(
                "datanika.tasks.email_tasks.send_email_change_email_task.delay",
                side_effect=RuntimeError("broker down"),
            ),
        ):
            await AccountState.request_email_change.fn(st, {"new_email": "new@example.com"})
        db_session.refresh(user)
        assert st.email_change_state == "failed"
        assert _pii(db_session, user.id).pending_email is None


class TestCancel:
    @pytest.mark.asyncio
    async def test_cancel_clears_the_pending_address(self, db_session, user, svc):
        EmailChangeService(svc).prepare_change(db_session, user.id, "new@example.com")
        db_session.flush()
        st = _state(user.id, 1)
        with patch.object(acc, "get_sync_session", return_value=_session_patch(db_session)):
            await AccountState.cancel_email_change.fn(st)
        db_session.refresh(user)
        assert _pii(db_session, user.id).pending_email is None
        assert st.email_change_state == "cancelled"
        assert st.pending_email == ""


class TestTheScreenRendersEachOutcome:
    def _settings(self) -> str:
        return (PAGES / "settings.py").read_text(encoding="utf-8")

    def test_every_outcome_has_its_own_branch(self):
        source = self._settings()
        for outcome in ("queued", "no_relay", "failed", "rate_limited", "address_taken"):
            assert f'AccountState.email_change_state == "{outcome}"' in source

    def test_the_pending_address_is_substituted(self):
        source = self._settings()
        assert '.replace(\n                        "{email}", AccountState.pending_email' in source

    def test_the_form_is_outside_the_unverified_callout(self):
        source = self._settings()
        tree = ast.parse(source)
        unverified = None
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (isinstance(func, ast.Attribute) and func.attr == "cond" and node.args):
                continue
            arg = node.args[0]
            if not isinstance(arg, ast.UnaryOp) or not isinstance(arg.op, ast.Invert):
                continue
            operand = arg.operand
            if isinstance(operand, ast.Attribute) and operand.attr == "email_verified":
                unverified = ast.get_source_segment(source, node)
                break
        assert unverified is not None
        assert "email_change_state" not in unverified
        assert "_email_change_section" not in unverified
        card = next(
            n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "account_card"
        )
        assert "_email_change_section()" in (ast.get_source_segment(source, card) or "")


class TestConfirmPage:
    @pytest.mark.asyncio
    async def test_loading_the_link_does_not_consume_it(self, db_session, user, svc):
        prepared = EmailChangeService(svc).prepare_change(db_session, user.id, "new@example.com")
        db_session.flush()
        st = MagicMock()
        st.token = ""
        st.token_valid = False
        st.confirmed = False
        st.router = SimpleNamespace(page=SimpleNamespace(params={"token": prepared.token}))
        st._service = lambda: EmailChangeState._service(st)
        with patch.object(confirm_mod, "get_sync_session", return_value=_session_patch(db_session)):
            await EmailChangeState.load_token.fn(st)
        assert st.token_valid is True
        row_used = EmailChangeService(svc).validate_token(db_session, prepared.token)
        assert row_used is not None

    @pytest.mark.asyncio
    async def test_the_button_moves_the_address_once(self, db_session, user, svc):
        prepared = EmailChangeService(svc).prepare_change(db_session, user.id, "new@example.com")
        db_session.flush()
        st = MagicMock()
        st.token = prepared.token
        st.token_valid = True
        st.confirmed = False
        st._service = lambda: EmailChangeState._service(st)
        with patch.object(confirm_mod, "get_sync_session", return_value=_session_patch(db_session)):
            await EmailChangeState.confirm.fn(st)
        db_session.refresh(user)
        assert st.confirmed is True
        pii = db_session.get(UserPII, user.id)
        assert pii is not None and pii.email == "new@example.com"
        assert user.email_verified is True
        with patch.object(confirm_mod, "get_sync_session", return_value=_session_patch(db_session)):
            await EmailChangeState.confirm.fn(st)
        db_session.refresh(user)
        assert st.confirmed is False
        assert db_session.get(UserPII, user.id).email == "new@example.com"

"""The Settings change-password handler (SPEC_PASSWORD_RESET Part A).

Driven with a stand-in ``self`` and the handler's underlying ``.fn``, following
the ``test_run_dispatch.py`` convention.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import datanika.ui.state.account_state as acc
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService
from datanika.ui.state.account_state import AccountState


@pytest.fixture
def svc():
    return UserService(AuthService("test-secret"))


@pytest.fixture
def user(db_session, svc):
    u = svc.register_user(db_session, "alice@example.com", "correct horse", "Alice")
    org = Organization(name="Alice Org", slug=f"alice-{u.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=u.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.flush()
    return u


def _state(user_id=0, org_id=0):
    st = MagicMock()
    for name, field in AccountState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    auth = MagicMock()
    auth.current_user.id = user_id
    auth.current_org.id = org_id
    st.get_state = AsyncMock(return_value=auth)
    # Bind the real factory: a MagicMock ``_service()`` returns a mock whose
    # ``change_password`` succeeds silently, so the whole suite would pass
    # without writing anything.
    st._service = lambda: AccountState._service(st)
    return st


class _TestSession:
    """``db_session`` proxy whose ``commit()`` is a ``flush()``.

    The shared fixture isolates each test inside one outer transaction it rolls
    back at teardown. A handler that calls ``session.commit()`` releases that
    savepoint, so its rows survive into the next test — verified: a row written
    and committed in one test is still visible in the next. Flushing instead
    keeps every behaviour the handler depends on (writes visible to later
    queries in the same transaction, the conditional UPDATE's rowcount) while
    leaving the rollback able to do its job.
    """

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


async def _change(st, db_session, **form):
    with patch.object(acc, "get_sync_session", return_value=_session_patch(db_session)):
        return await AccountState.change_password.fn(st, form)


class TestChangePasswordHandler:
    @pytest.mark.asyncio
    async def test_happy_path_reports_success(self, db_session, user, svc):
        st = _state(user.id, 1)
        await _change(
            st,
            db_session,
            current_password="correct horse",
            password="a whole new password",
            confirm="a whole new password",
        )
        assert st.success is True
        assert st.error == ""
        db_session.flush()
        assert svc.authenticate(db_session, "alice@example.com", "a whole new password")

    @pytest.mark.asyncio
    async def test_wrong_current_password_leaves_the_hash_untouched(self, db_session, user):
        before = user.password_hash
        st = _state(user.id, 1)
        await _change(
            st,
            db_session,
            current_password="wrong",
            password="a whole new password",
            confirm="a whole new password",
        )
        assert st.error != ""
        assert st.success is False
        db_session.flush()
        assert user.password_hash == before

    @pytest.mark.asyncio
    async def test_mismatched_confirmation_never_reaches_the_service(self, db_session, user):
        """Confirm-password is a typo catcher, checked before anything is written."""
        before = user.password_hash
        st = _state(user.id, 1)
        await _change(
            st,
            db_session,
            current_password="correct horse",
            password="a whole new password",
            confirm="a different password",
        )
        assert st.error != ""
        db_session.flush()
        assert user.password_hash == before

    @pytest.mark.asyncio
    async def test_a_weak_password_names_the_actual_rule(self, db_session, user):
        st = _state(user.id, 1)
        await _change(
            st, db_session, current_password="correct horse", password="short", confirm="short"
        )
        assert "8" in st.error

    @pytest.mark.asyncio
    async def test_reusing_the_current_password_is_refused(self, db_session, user):
        st = _state(user.id, 1)
        await _change(
            st,
            db_session,
            current_password="correct horse",
            password="correct horse",
            confirm="correct horse",
        )
        assert st.error != ""
        assert st.success is False

    @pytest.mark.asyncio
    async def test_a_signed_out_caller_gets_nowhere(self, db_session, user):
        st = _state(0, 0)
        await _change(
            st,
            db_session,
            current_password="correct horse",
            password="a whole new password",
            confirm="a whole new password",
        )
        assert st.success is False


class TestOAuthOnlyAccount:
    @pytest.mark.asyncio
    async def test_it_can_set_a_password_with_no_current_one(self, db_session, svc):
        u, _ = svc.find_or_create_oauth_user(
            db_session, "bob@example.com", "Bob", "google", "g-1", email_verified=True
        )
        st = _state(u.id, 1)
        await _change(
            st, db_session, password="a password bob picked", confirm="a password bob picked"
        )
        assert st.success is True

    @pytest.mark.asyncio
    async def test_load_reports_whether_a_password_exists(self, db_session, user, svc):
        st = _state(user.id, 1)
        with patch.object(acc, "get_sync_session", return_value=_session_patch(db_session)):
            await AccountState.load_account.fn(st)
        assert st.has_password is True

        oauth_user, _ = svc.find_or_create_oauth_user(
            db_session, "bob@example.com", "Bob", "google", "g-1", email_verified=True
        )
        st2 = _state(oauth_user.id, 1)
        with patch.object(acc, "get_sync_session", return_value=_session_patch(db_session)):
            await AccountState.load_account.fn(st2)
        assert st2.has_password is False

    @pytest.mark.asyncio
    async def test_a_password_account_that_linked_google_still_reports_true(
        self, db_session, user, svc
    ):
        """The D6 trap at the state layer."""
        svc.find_or_create_oauth_user(
            db_session, "alice@example.com", "Alice", "google", "g-a", email_verified=True
        )
        st = _state(user.id, 1)
        with patch.object(acc, "get_sync_session", return_value=_session_patch(db_session)):
            await AccountState.load_account.fn(st)
        assert st.has_password is True


class TestAuditing:
    @pytest.mark.asyncio
    async def test_an_entry_is_written_and_carries_no_secret(self, db_session, user):
        from datanika.models.audit_log import AuditLog

        st = _state(user.id, 1)
        st._audit = MagicMock()
        await _change(
            st,
            db_session,
            current_password="correct horse",
            password="a whole new password",
            confirm="a whole new password",
        )
        assert st._audit.called
        payload = str(st._audit.call_args)
        assert "correct horse" not in payload
        assert "a whole new password" not in payload
        assert "$2b$" not in payload
        assert AuditLog is not None

    @pytest.mark.asyncio
    async def test_nothing_is_audited_when_the_change_fails(self, db_session, user):
        st = _state(user.id, 1)
        st._audit = MagicMock()
        await _change(
            st,
            db_session,
            current_password="wrong",
            password="a whole new password",
            confirm="a whole new password",
        )
        assert not st._audit.called

"""Sign-in records the client address in the personal-data sidecar (core#670).

The resolver is used as it stands. Cloudflare's ``CF-Connecting-IP`` wins when
it is present, including beside a multi-hop ``X-Forwarded-For``. A multi-hop
chain with no Cloudflare header stores nothing, and so does ``127.0.0.1``.
A wrong hop is worse than a blank. The address does not go into
``audit_logs.ip_address`` or into an audit payload.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select

from datanika.config import settings
from datanika.models.audit_log import AuditAction, AuditLog
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.base_state import BaseState
from tests.factories import make_user

_CF = "203.0.113.50"
_PASSWORD = "correct-horse-battery"


def _account(db_session, *, slug: str) -> tuple[Organization, int, str]:
    org = Organization(name="Address Org", slug=slug)
    db_session.add(org)
    db_session.flush()
    email = f"{slug}@example.com"
    user = make_user(
        db_session,
        email=email,
        full_name="Address Person",
        password_hash=AuthService(settings.secret_key).hash_password(_PASSWORD),
    )
    db_session.add(Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.flush()
    return org, user.id, email


def _state(headers: dict[str, str]) -> MagicMock:
    st = MagicMock()
    for name, field in AuthState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    st.router = MagicMock()
    st.router.headers.raw_headers = headers
    st.router.page.params = {}
    st._client_ip = lambda: AuthState._client_ip(st)
    st._get_user_service = lambda: UserService(AuthService(settings.secret_key))
    st._clear_session = lambda: AuthState._clear_session(st)
    st._load_current_role = lambda *_a, **_k: None
    st._post_auth_redirect_target = lambda: "/"
    # A MagicMock answers every unbound name with another mock, so the new
    # helpers would "succeed" and write nothing. Bind the real methods.
    st._write_auth_audit = lambda *args, **kwargs: AuthState._write_auth_audit(st, *args, **kwargs)
    st._audit_failed_sign_in = lambda email: AuthState._audit_failed_sign_in(st, email)
    return st


@contextmanager
def _same_session(db_session):
    yield db_session


def _login(db_session, headers: dict[str, str], email: str, password: str) -> MagicMock:
    st = _state(headers)
    with (
        patch("datanika.ui.state.auth_state.CaptchaService") as captcha,
        patch("datanika.ui.state.auth_state.get_sync_session", lambda: _same_session(db_session)),
    ):
        captcha.return_value.verify.return_value = True
        AuthState.login.fn(st, {"email": email, "password": password, "captcha_token": "tok"})
    return st


def _logs(db_session, action: AuditAction) -> list[AuditLog]:
    return list(
        db_session.execute(select(AuditLog).where(AuditLog.action == action)).scalars().all()
    )


def _sidecar_address(db_session, log: AuditLog) -> str | None:
    """The address stored for this audit row, or None when nothing was stored."""
    from datanika.models import pii

    model = getattr(pii, "AuditLogPII", None)
    assert model is not None, "audit_log_pii does not exist yet"
    row = db_session.get(model, log.id)
    assert log.ip_address is None
    blob = f"{log.old_values}{log.new_values}"
    assert _CF not in blob
    assert "198.51.100" not in blob
    return None if row is None else row.ip_address


def test_cf_connecting_ip_is_stored_when_a_multihop_chain_is_also_present(db_session):
    _org, _user_id, email = _account(db_session, slug="cf-wins")
    _login(
        db_session,
        {"cf-connecting-ip": _CF, "x-forwarded-for": "198.51.100.10, 198.51.100.11"},
        email,
        _PASSWORD,
    )
    logs = _logs(db_session, AuditAction.LOGIN)
    assert len(logs) == 1
    assert _sidecar_address(db_session, logs[0]) == _CF


def test_a_multihop_chain_without_cf_connecting_ip_stores_nothing(db_session):
    _org, _user_id, email = _account(db_session, slug="multihop-blank")
    _login(
        db_session,
        {"x-forwarded-for": "198.51.100.10, 198.51.100.11"},
        email,
        _PASSWORD,
    )
    logs = _logs(db_session, AuditAction.LOGIN)
    assert len(logs) == 1
    assert _sidecar_address(db_session, logs[0]) is None


def test_loopback_stores_nothing(db_session):
    _org, _user_id, email = _account(db_session, slug="loopback-blank")
    _login(db_session, {"cf-connecting-ip": "127.0.0.1"}, email, _PASSWORD)
    logs = _logs(db_session, AuditAction.LOGIN)
    assert len(logs) == 1
    assert _sidecar_address(db_session, logs[0]) is None


def test_a_failed_password_stores_the_cloudflare_address(db_session):
    _org, _user_id, email = _account(db_session, slug="failed-password")
    st = _login(
        db_session,
        {"cf-connecting-ip": _CF, "x-forwarded-for": "198.51.100.10, 198.51.100.11"},
        email,
        "wrong-password",
    )
    assert st.auth_error == "Invalid email or password"
    logs = _logs(db_session, AuditAction.LOGIN)
    assert len(logs) == 1
    assert _sidecar_address(db_session, logs[0]) == _CF


def test_a_failed_password_on_a_multihop_chain_stores_nothing(db_session):
    _org, _user_id, email = _account(db_session, slug="failed-multihop")
    _login(
        db_session,
        {"x-forwarded-for": "198.51.100.10, 198.51.100.11"},
        email,
        "wrong-password",
    )
    logs = _logs(db_session, AuditAction.LOGIN)
    assert len(logs) == 1
    assert _sidecar_address(db_session, logs[0]) is None


def test_sign_out_stores_the_cloudflare_address(db_session):
    org, user_id, email = _account(db_session, slug="sign-out")
    st = _state({"cf-connecting-ip": _CF, "x-forwarded-for": "198.51.100.10, 198.51.100.11"})
    st.current_user.id = user_id
    st.current_user.email = email
    st.current_org.id = org.id
    with patch("datanika.ui.state.auth_state.get_sync_session", lambda: _same_session(db_session)):
        AuthState.logout.fn(st)
    logs = _logs(db_session, AuditAction.LOGOUT)
    assert len(logs) == 1
    assert _sidecar_address(db_session, logs[0]) == _CF


def test_an_ordinary_audit_does_not_take_an_address():
    import inspect

    source = inspect.getsource(BaseState._audit)
    assert "ip_address" not in source
    assert "resolve_client_ip" not in source
    assert "AuditLogPII" not in source


@pytest.mark.parametrize(
    "headers",
    [
        {"x-forwarded-for": "198.51.100.10, 198.51.100.11"},
        {"asgi-scope-client": "127.0.0.1"},
    ],
)
def test_sign_out_stores_nothing_when_the_address_cannot_be_named(db_session, headers):
    slug = "sign-out-" + next(iter(headers)).replace("-", "")[:12]
    org, user_id, _email = _account(db_session, slug=slug)
    st = _state(headers)
    st.current_user.id = user_id
    st.current_org.id = org.id
    with patch("datanika.ui.state.auth_state.get_sync_session", lambda: _same_session(db_session)):
        AuthState.logout.fn(st)
    logs = _logs(db_session, AuditAction.LOGOUT)
    assert len(logs) == 1
    assert _sidecar_address(db_session, logs[0]) is None

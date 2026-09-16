"""Connection **Test** requires `editor`, on both surfaces (core#1370).

`SPEC_SERVICE_AUTHORIZATION` §11, ruled by Product 2026-09-16. Two connection operations enforced
**no** role:

* `ConnectionState.test_connection_from_form` — Test on the unsaved form.
* `ConnectionState.test_saved_connection` — Test on a saved row.

Test is not a read. It **exercises the org's stored credential to open an outbound connection to a
host** — the same privileged use of a stored credential that puts `edit_connection` and
`copy_connection` at `editor`. On the form path the member supplies the host and credentials
themselves and asks the server to dial them, which is a step of the create/save lifecycle.

🚨 Why this asserts the PRESENCE of a check rather than the absence of a bypass
------------------------------------------------------------------------------
The §1 census that missed these two reads each handler's own `_check_role("R")` — it reads the
requirement *off the declaration*. **A handler that declares no role contributes nothing to the
census, so a completely unguarded operation is invisible to it rather than flagged by it.** A test
written as "no viewer may get through" inherits that blind spot: it passes on a handler that refuses
everyone, on a broken fixture, and on a handler that was deleted. So every claim below is made twice
— structurally (the gate is there, awaited, and first) and behaviourally (a viewer is refused **and
an editor is not**).

⚠️ This file retires a control that used to assert the opposite
---------------------------------------------------------------
`test_connection_credential_read_is_gated.py::test_the_control_handler_stays_ungated` asserted
`"_check_role" not in test_saved_connection` — "the Test button is member-visible on purpose". That
was the standing ruling until §11 reversed it. It is retired there rather than deleted quietly, and
its discriminating job (the rule must not become "gate every handler that decrypts") is preserved by
repointing it at a handler that still decrypts without disclosing.
"""

from __future__ import annotations

import ast
import contextlib
import inspect
import json
import pathlib
import textwrap
from unittest.mock import MagicMock

import pytest
from cryptography.fernet import Fernet

import datanika.ui.state.auth_state as auth_state_module
from datanika.config import settings
from datanika.models.connection import ConnectionType
from datanika.services.auth import AuthService
from datanika.services.connection_service import ConnectionService, ConnectionVerdict
from datanika.ui.state.auth_state import OrgInfo, UserInfo
from datanika.ui.state.base_state import BaseState
from datanika.ui.state.connection_state import ConnectionState

SECRET = "test-secret-key-for-connection-test-role-gate"
LOCALES = ("en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr")
I18N = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "i18n"

#: §7.2: one key per threshold, never one per handler. 25+ handlers and three thresholds; 25
#: near-identical keys would be 225 translations that drift.
EDITOR_KEY = "errors.role_required_editor"
SENTINEL = "SENTINEL-editor-refusal"

GATED_HANDLERS = ["test_connection_from_form", "test_saved_connection"]


# --------------------------------------------------------------------------- helpers


def _unwrap(func):
    return getattr(func, "fn", func)


def _parse(name: str) -> ast.AST:
    return ast.parse(textwrap.dedent(inspect.getsource(_unwrap(getattr(ConnectionState, name)))))


def _awaited_role_gate(name: str) -> str | None:
    """The role passed to an **awaited** ``self._check_role(...)``, if any.

    Structural rather than textual because the defect that matters is invisible to a grep:
    ``_check_role`` is a coroutine, so ``if not self._check_role("editor")`` parses, reads as a
    guard in review, and refuses nobody — the un-awaited coroutine is always truthy.
    """
    for node in ast.walk(_parse(name)):
        if not isinstance(node, ast.Await):
            continue
        call = node.value
        if (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and call.func.attr == "_check_role"
            and call.args
            and isinstance(call.args[0], ast.Constant)
        ):
            return call.args[0].value
    return None


def _auth_stand_in(role: str, *, translations: dict | None = None):
    """An ``AuthState`` stand-in carrying its real field defaults.

    ⚠️ A bare ``MagicMock`` answers every attribute truthily, so a guard like
    ``if not auth.session_expired`` is never taken and the test measures nothing.

    ``translations`` is supplied because ``_check_role`` now resolves its sentence through
    ``_translated``, which asks ``get_state(I18nState)``. The stand-in stands in for both.
    """
    st = MagicMock()
    for name, field in auth_state_module.AuthState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    svc = AuthService(SECRET)
    st.access_token = svc.create_access_token(1, 10, expires_minutes=10)
    st.refresh_token = svc.create_refresh_token(1)
    st.current_user = UserInfo(id=1, email="a@b.c", full_name="A")
    st.current_org = OrgInfo(id=10, name="Org", slug="org")
    st.current_role = role
    st.action_error = ""
    st.translations = translations if translations is not None else {}
    st._revalidate_session = lambda: auth_state_module.AuthState._revalidate_session(st)
    st._clear_session = lambda: auth_state_module.AuthState._clear_session(st)
    st._get_user_service = lambda: auth_state_module.AuthState._get_user_service(st)
    return st


@pytest.fixture
def state_as(monkeypatch):
    """Build a ``ConnectionState`` whose session carries ``role``, with no running app."""

    def build(role: str, *, translations: dict | None = None):
        auth = _auth_stand_in(role, translations=translations)

        async def _get_state(self, state_cls):
            return auth

        monkeypatch.setattr(ConnectionState, "get_state", _get_state)
        monkeypatch.setattr(auth_state_module.settings, "secret_key", SECRET)
        state = ConnectionState(parent_state=BaseState(init_substates=False), init_substates=False)
        return state, auth

    return build


@pytest.fixture
def dialled(monkeypatch):
    """Record every call that reaches the driver, so "refused" means *before any outbound call*."""
    calls: list = []

    def record(config, connection_type):
        calls.append(connection_type)
        return ConnectionVerdict(True, "Connected successfully")

    monkeypatch.setattr(ConnectionService, "test_connection_verdict", staticmethod(record))
    return calls


def _fill_form(state) -> None:
    state.form_name = "probe"
    state.form_type = "mysql"
    state.form_host = "127.0.0.1"
    state.form_port = "3306"
    state.form_user = "probe"
    state.form_password = "probe"
    state.form_database = "probe"


def _stub_saved_row(monkeypatch) -> list:
    """Make ``test_saved_connection`` resolvable without a database."""
    rows: list = []

    class _Saved:
        connection_type = ConnectionType.MYSQL

    async def _org_id(self):
        return 10

    # ⚠️ The handler builds `EncryptionService(settings.credential_encryption_key)` before it reads
    # anything, and the test-time default is not a valid Fernet key. Without this the saved-row arms
    # die in the fixture with `binascii.Error: Incorrect padding` — red, but red about the harness,
    # which is indistinguishable from red about the gate if you only read the summary line.
    monkeypatch.setattr(settings, "credential_encryption_key", Fernet.generate_key().decode())
    monkeypatch.setattr(ConnectionState, "_get_org_id", _org_id)
    monkeypatch.setattr(
        "datanika.ui.state.connection_state.get_sync_session",
        lambda: contextlib.nullcontext(object()),
    )
    monkeypatch.setattr(
        ConnectionService,
        "get_connection_config",
        lambda *a: {"host": "127.0.0.1", "port": 3306, "user": "u", "password": "p"},
    )
    monkeypatch.setattr(ConnectionService, "get_connection", lambda *a: _Saved())
    monkeypatch.setattr(
        ConnectionState,
        "_set_row_test_status",
        lambda self, conn_id, status, note="": rows.append((conn_id, status)),
    )
    return rows


# --------------------------------------------------------------------------- 1. structural


@pytest.mark.parametrize("name", GATED_HANDLERS)
def test_the_gate_is_an_awaited_editor_check(name):
    """AC11.1 / AC11.2, asserted on the declaration the §1 census reads.

    This is the assertion that makes the operation *visible* to the census at all. Red on both
    handlers before §11: each declared no role, so each contributed nothing to the table.
    """
    assert inspect.iscoroutinefunction(_unwrap(getattr(ConnectionState, name)))
    assert _awaited_role_gate(name) == "editor", (
        f"ConnectionState.{name} declares no awaited `_check_role`, so any signed-in member — a "
        "viewer included — can make the server open an outbound connection using the org's "
        "stored credential by dispatching the event. SPEC_SERVICE_AUTHORIZATION §11 rules both "
        "Test surfaces at `editor`. Add `if not await self._check_role('editor'): return`."
    )


@pytest.mark.parametrize("name", GATED_HANDLERS)
def test_the_gate_is_the_first_thing_the_handler_does(name):
    """A present-but-late gate reads exactly like a correct one, and is not one.

    `test_connection_from_form` builds the config (which reads the typed credential) and
    `test_saved_connection` decrypts the stored one. A check placed after either still refuses the
    caller, but only once the work it exists to prevent has begun.
    """
    fn = _parse(name).body[0]
    body = [
        s for s in fn.body if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))
    ]
    first = ast.unparse(body[0])
    assert "_check_role" in first, (
        f"ConnectionState.{name}'s first statement is `{first}`, not the role check."
    )


# --------------------------------------------------------------------------- 2. behavioural


def test_a_viewer_never_reaches_the_driver_on_the_form_path(state_as, dialled):
    """AC11.1. The refusal must land *before* any outbound connection is attempted."""
    state, auth = state_as("viewer", translations={EDITOR_KEY: SENTINEL})
    _fill_form(state)

    import asyncio

    asyncio.run(state.test_connection_from_form())

    assert dialled == [], (
        "a viewer's Test dialled the host anyway — the gate refused the caller after the server "
        "had already opened an outbound connection"
    )
    assert auth.action_error == SENTINEL, (
        f"the refusal did not resolve from {EDITOR_KEY!r} (got {auth.action_error!r}). §7.2 wants "
        "one key per threshold, so eight of nine locales do not read this sentence in English."
    )


def test_an_editor_still_reaches_the_driver_on_the_form_path(state_as, dialled):
    """🔑 The control. Without it every assertion above is satisfied by refusing everyone —
    a broken fixture, a handler that returns early unconditionally, a deleted handler."""
    state, _ = state_as("editor")
    _fill_form(state)

    import asyncio

    asyncio.run(state.test_connection_from_form())

    assert dialled == [ConnectionType.MYSQL], (
        "an editor was refused, so the gate is not discriminating on role and the refusal test "
        "above proves nothing"
    )
    assert state.test_success is True


def test_a_viewer_never_reaches_the_driver_on_the_saved_row_path(state_as, dialled, monkeypatch):
    """AC11.2. The saved-row path decrypts the org's stored credential to authenticate with it."""
    rows = _stub_saved_row(monkeypatch)
    state, auth = state_as("viewer", translations={EDITOR_KEY: SENTINEL})

    import asyncio

    asyncio.run(state.test_saved_connection(7))

    assert dialled == [], "a viewer's saved-row Test dialled the host with the stored credential"
    assert rows == [], f"the row was updated for a refused caller: {rows}"
    assert auth.action_error == SENTINEL


def test_an_editor_still_reaches_the_driver_on_the_saved_row_path(state_as, dialled, monkeypatch):
    """The control for AC11.2."""
    rows = _stub_saved_row(monkeypatch)
    state, _ = state_as("editor")

    import asyncio

    asyncio.run(state.test_saved_connection(7))

    assert dialled == [ConnectionType.MYSQL], "an editor was refused on the saved-row path"
    assert rows == [(7, "ok")], rows


def test_the_english_fallback_still_names_the_role(state_as, dialled):
    """A translation table that lacks the key must not degrade into a sentence naming nothing.

    ⚠️ Asserted with an EMPTY table, which is what a locale missing the key resolves to.
    """
    state, auth = state_as("viewer", translations={})
    _fill_form(state)

    import asyncio

    asyncio.run(state.test_connection_from_form())

    assert dialled == []
    assert "editor" in auth.action_error.lower(), (
        f"the fallback does not name the required role: {auth.action_error!r}. A user told only "
        "'permission denied' has been told they have a problem and not how to end it."
    )


# --------------------------------------------------------------------------- 3. i18n (§7.2)


@pytest.mark.parametrize("locale", LOCALES)
def test_the_threshold_key_exists_in_every_locale(locale):
    data = json.loads((I18N / f"{locale}.json").read_text(encoding="utf-8"))
    assert EDITOR_KEY in data, f"{locale}.json is missing {EDITOR_KEY}"
    assert data[EDITOR_KEY].strip(), f"{locale}.json has an empty {EDITOR_KEY}"


@pytest.mark.parametrize("locale", [loc for loc in LOCALES if loc != "en"])
def test_no_locale_left_the_refusal_in_english(locale):
    """Key parity passes on nine copies of the English string.

    `tests/test_i18n` compares key *sets*, so a locale carrying the key with the English value is
    indistinguishable from a translated one — and this is callout text a person reads.
    """
    en = json.loads((I18N / "en.json").read_text(encoding="utf-8"))
    data = json.loads((I18N / f"{locale}.json").read_text(encoding="utf-8"))
    assert data[EDITOR_KEY] != en[EDITOR_KEY], f"{locale}.json's {EDITOR_KEY} is English verbatim"

"""Test Connection waits off the event loop, and no longer than its budget (core#1367).

Both Test handlers now hand the driver call to a worker thread, through one helper, and wait no
longer than ``CONNECTION_TEST_BUDGET_SECONDS``. Past that, they answer with a translated "did not
answer" verdict.

The witness drives the real handler beside a ticker on the same event loop, against a loopback
server that accepts the connection and sends nothing. The loop runs in a daemon thread. The test
waits on an event set when the handler returns, so it fails rather than hangs on code without the
change. Teardown then closes the accepted sockets, which releases the driver.

⚠️ The test waits on that event, not on the thread. ``asyncio.run`` waits, on shutdown, for the
default executor's threads. A handler that answered on budget therefore still leaves its thread
joined to the driver's own bound.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import pathlib
import socket
import threading
import time

import pytest
from cryptography.fernet import Fernet

from datanika.config import settings
from datanika.models.connection import ConnectionType
from datanika.services.connection_service import ConnectionService, ConnectionVerdict
from datanika.ui.state import connection_state
from datanika.ui.state.base_state import BaseState
from datanika.ui.state.connection_state import _VERDICT_KEYS, ConnectionState

BUDGET = 1.5
WAIT_SECONDS = 12
LOCALES = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]
I18N = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "i18n"
KEY = "connections.test_timed_out"


@pytest.fixture
def quiet_server():
    """A loopback server that accepts every connection and never sends a byte."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    held: list[socket.socket] = []

    def accept():
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            held.append(conn)

    threading.Thread(target=accept, daemon=True).start()
    try:
        yield srv.getsockname()[1]
    finally:
        srv.close()
        for conn in held:
            conn.close()


class _NoTranslations:
    translations: dict = {}


@pytest.fixture
def state_without_a_tree(monkeypatch):
    """A ConnectionState whose parent vars and translations resolve without a running app.

    `error_message` lives on BaseState, so the state gets a real BaseState parent. `_translated`
    asks `get_state(I18nState)`, so that returns an empty table and the English fallback is read.
    """

    async def _get_state(self, state_cls):
        return _NoTranslations()

    async def _allow(self, min_role):
        """core#1370 gated both Test handlers at `editor`.

        This file is about *where the driver runs and how long it waits*, not about who may ask.
        The stand-in above has no `AuthState`, so the real gate would fail on the session check and
        these tests would go red for a reason unrelated to their subject. The gate itself is
        asserted — viewer refused, editor admitted — in
        `tests/test_security/test_connection_test_role_gate.py`.
        """
        return True

    monkeypatch.setattr(ConnectionState, "get_state", _get_state)
    monkeypatch.setattr(ConnectionState, "_check_role", _allow)
    return ConnectionState(parent_state=BaseState(init_substates=False), init_substates=False)


def _drive(handler) -> dict:
    """Await `handler()` beside a 50 ms ticker on one event loop, in a daemon thread."""
    outcome: dict = {}
    returned = threading.Event()

    async def scenario():
        ticks: list[float] = []

        async def ticker():
            while True:
                ticks.append(time.monotonic())
                await asyncio.sleep(0.05)

        task = asyncio.create_task(ticker())
        await asyncio.sleep(0.2)
        started = time.monotonic()
        await handler()
        finished = time.monotonic()
        task.cancel()
        during = [t for t in ticks if started <= t <= finished]
        edges = [started, *during, finished]
        outcome["elapsed"] = finished - started
        outcome["longest_stall"] = max(b - a for a, b in zip(edges, edges[1:], strict=False))
        returned.set()

    threading.Thread(target=lambda: asyncio.run(scenario()), daemon=True).start()
    outcome["returned"] = returned.wait(WAIT_SECONDS)
    return outcome


def _assert_loop_stayed_free(outcome: dict) -> None:
    assert outcome["returned"], f"the handler had not returned after {WAIT_SECONDS}s"
    assert outcome["longest_stall"] < 0.5, (
        f"the event loop did not tick for {outcome['longest_stall']:.1f}s while the test waited"
    )
    assert outcome["elapsed"] < BUDGET + 2, f"the answer took {outcome['elapsed']:.1f}s"


class TestBothTestHandlersWaitOffTheLoop:
    def test_the_form_test_keeps_the_loop_ticking_and_answers_on_budget(
        self, quiet_server, state_without_a_tree, monkeypatch
    ):
        monkeypatch.setattr(
            connection_state, "CONNECTION_TEST_BUDGET_SECONDS", BUDGET, raising=False
        )
        state = state_without_a_tree
        state.form_name = "probe"
        state.form_type = "mysql"
        state.form_host = "127.0.0.1"
        state.form_port = str(quiet_server)
        state.form_user = "probe"
        state.form_password = "probe"
        state.form_database = "probe"

        outcome = _drive(state.test_connection_from_form)

        _assert_loop_stayed_free(outcome)
        assert state.test_success is False
        assert "did not answer within" in state.test_message, state.test_message

    def test_the_saved_connection_test_keeps_the_loop_ticking_and_answers_on_budget(
        self, quiet_server, state_without_a_tree, monkeypatch
    ):
        monkeypatch.setattr(
            connection_state, "CONNECTION_TEST_BUDGET_SECONDS", BUDGET, raising=False
        )
        monkeypatch.setattr(settings, "credential_encryption_key", Fernet.generate_key().decode())
        config = {
            "host": "127.0.0.1",
            "port": quiet_server,
            "user": "probe",
            "password": "probe",
            "database": "probe",
        }

        class _Saved:
            connection_type = ConnectionType.MYSQL

        async def _org_id(self):
            return 1

        monkeypatch.setattr(ConnectionState, "_get_org_id", _org_id)
        monkeypatch.setattr(
            "datanika.ui.state.connection_state.get_sync_session",
            lambda: contextlib.nullcontext(object()),
        )
        monkeypatch.setattr(ConnectionService, "get_connection_config", lambda *a: config)
        monkeypatch.setattr(ConnectionService, "get_connection", lambda *a: _Saved())
        rows: list[tuple] = []
        monkeypatch.setattr(
            ConnectionState,
            "_set_row_test_status",
            lambda self, conn_id, status, note="": rows.append((conn_id, status)),
        )
        state = state_without_a_tree

        outcome = _drive(lambda: state.test_saved_connection(7))

        _assert_loop_stayed_free(outcome)
        assert rows == [(7, "fail")], rows


class TestTheTimedOutVerdictIsTranslated:
    def test_the_service_builds_it_and_the_ui_maps_it(self):
        verdict = ConnectionService.timed_out_verdict(30)

        assert isinstance(verdict, ConnectionVerdict)
        assert (verdict.ok, verdict.reason, verdict.arg) == (False, "timed_out", "30")
        assert _VERDICT_KEYS.get("timed_out") == KEY

    @pytest.mark.parametrize("locale", LOCALES)
    def test_the_sentence_exists_in_every_locale_with_its_placeholder(self, locale):
        data = json.loads((I18N / f"{locale}.json").read_text(encoding="utf-8"))
        en = json.loads((I18N / "en.json").read_text(encoding="utf-8"))

        assert KEY in data, f"{locale}.json is missing {KEY}"
        assert "{arg}" in data[KEY], f"{locale}.json's {KEY} dropped the {{arg}} placeholder"
        if locale != "en":
            assert data[KEY] != en[KEY], f"{locale}.json's {KEY} is the English string verbatim"

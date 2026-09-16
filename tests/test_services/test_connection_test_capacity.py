"""A connection test occupies bounded capacity, and each surface answers in budget (core#1367).

Stated without depending on any one driver: a connection test hands a host to a driver, and a
driver handed a host that accepts TCP and then says nothing does not always return promptly. The
UI already waits off the event loop within a budget (core#1367, first half). Two gaps are left,
and neither is a driver question:

* **The API endpoint waits with no budget at all.** It is a synchronous handler, so
  `api_middleware` runs it in the process's **default executor** — the pool every synchronous API
  handler shares — and holds a database session while it waits.
* **The UI's wait borrows that same default executor** (`asyncio.to_thread`), so a stalled test
  consumes capacity every other endpoint in the process needs.

So connection tests get their **own bounded pool**, both surfaces answer within the budget, and a
test that cannot get a slot is **answered** rather than queued behind stalled work — with its own
sentence, because "the server did not answer" is untrue when we never dialled.

Every wait here has a hard join or deadline, so code without the change fails rather than hangs.
"""

from __future__ import annotations

import contextlib
import json
import pathlib
import socket
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.models.connection import ConnectionType
from datanika.services import connection_service
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.connection_service import ConnectionService, ConnectionVerdict
from datanika.services.rate_limit_service import RateLimitResult

BUDGET = 2.0
JOIN_SECONDS = 20
POOL_PREFIX = "datanika-conn-test"
LOCALES = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]
I18N = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "i18n"
BUSY_KEY = "connections.test_busy"


@pytest.fixture
def quiet_server():
    """A loopback server that accepts every connection and never sends a byte."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
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


@pytest.fixture
def short_budget(monkeypatch):
    """Keep the test fast, and keep one arm from leaving a slot held for the next one.

    `raising=False` so code without the constant fails on the wait, not here.

    🔑 **Two values, and their ORDER is the whole point.** The API arm submits a real driver call
    against the quiet server, and — by design — that thread keeps its pool slot until the driver
    gives up, which is 30 s at the production value. The mutation sweep caught the consequence:
    with the timing shifted, a later test that fills the pool found a slot still held by an earlier
    arm and failed for a reason unrelated to the mutation under test.

    So the I/O bound is shortened too, but it must stay **above** the budget. Setting it to 1 s
    made the driver answer first, and the arm that exists to exercise the budget stopped doing so:
    the endpoint returned an ordinary driver failure instead of "did not answer". 4 s sits above
    the 2 s budget and far below 30.
    """
    monkeypatch.setattr(connection_service, "CONNECTION_TEST_BUDGET_SECONDS", BUDGET, raising=False)
    monkeypatch.setattr(connection_service, "_TEST_IO_TIMEOUT_SECONDS", 4, raising=False)


def _config(port: int) -> dict:
    return {
        "host": "127.0.0.1",
        "port": port,
        "user": "probe",
        "password": "probe",
        "database": "probe",
    }


@pytest.fixture
def api_client():
    """The API endpoint, built the way production installs it: the real decorator and middleware.

    Only the collaborators outside this test's subject are replaced — auth, the rate limiter, the
    session and the connection service. ``RateLimitResult`` is constructed with **every** field:
    the first version of this fixture omitted ``reset_at``, and the three tests below then ended in
    a fixture ``TypeError`` rather than measuring anything — including the control.
    """
    app = Starlette(routes=api_v1_routes)
    key = MagicMock()
    key.id, key.org_id, key.user_id, key.scopes = 1, 10, 1, None
    allowed = RateLimitResult(
        allowed=True,
        current_count=1,
        limit=60,
        remaining=59,
        retry_after=0,
        reset_at=int(time.time()) + 60,
    )

    conn_svc = MagicMock()
    saved = MagicMock()
    saved.connection_type = ConnectionType.MYSQL

    @contextlib.contextmanager
    def fake_session():
        yield MagicMock()

    def client_for(config: dict):
        conn_svc.get_connection_config.return_value = config
        conn_svc.get_connection.return_value = saved
        stack = contextlib.ExitStack()
        svc = stack.enter_context(patch("datanika.services.api_middleware._api_key_svc"))
        rl = stack.enter_context(patch("datanika.services.api_middleware._rate_limit_svc"))
        stack.enter_context(patch("datanika.services.api_middleware._get_session", fake_session))
        stack.enter_context(
            patch("datanika.services.api_v1_routes._get_conn_svc", return_value=conn_svc)
        )
        # core#1370 put an `editor` check on this endpoint. The key here is a MagicMock with no
        # membership row and the session is a MagicMock, so the real check would refuse with a 403
        # and every test in this file would measure the gate instead of the budget. The gate is
        # asserted against REAL membership rows in `test_ac6_endpoint_refusals.py`, which is where
        # §5 AC6 says the endpoint-level witness belongs.
        stack.enter_context(
            patch("datanika.services.api_v1_routes.assert_org_role", lambda *a, **k: None)
        )
        svc.authenticate_api_key.return_value = key
        rl.get_limit_for_org.return_value = 60
        rl.check_rate_limit.return_value = allowed
        return stack, TestClient(app)

    return client_for


def _post_test_within(client_for, config: dict, seconds: float):
    """(response or None, elapsed). None means it had not answered when the join expired."""
    result: dict = {}
    stack, client = client_for(config)

    def target():
        with stack:
            result["response"] = client.post(
                "/api/v1/connections/1/test", headers={"Authorization": "Bearer etf_testkey"}
            )

    started = time.monotonic()
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(seconds)
    return result.get("response"), time.monotonic() - started


class TestTheApiPathAnswersWithinItsBudget:
    """No budget today: against a server that never speaks, the endpoint waits on the driver."""

    def test_a_server_that_sends_nothing_gets_an_answer(
        self, quiet_server, short_budget, api_client
    ):
        response, elapsed = _post_test_within(api_client, _config(quiet_server), JOIN_SECONDS)

        assert response is not None, (
            f"the API endpoint had not answered after {elapsed:.0f}s against a server that accepts "
            "the connection and sends nothing: it waits on the driver with no budget (core#1367)"
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["success"] is False, body
        assert "did not answer" in body["message"], body

    def test_control_a_refused_port_still_answers_normally(self, short_budget, api_client):
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()

        response, elapsed = _post_test_within(api_client, _config(port), JOIN_SECONDS)

        assert response is not None, f"no answer after {elapsed:.0f}s from a refused port"
        assert response.status_code == 200, response.text
        assert response.json()["success"] is False


class TestTestsRunOnTheirOwnPool:
    """A stalled test must not consume the executor every other synchronous handler shares."""

    def test_the_api_path_runs_the_driver_on_the_connection_test_pool(
        self, short_budget, api_client, monkeypatch
    ):
        seen: list[str] = []

        def record(config, connection_type):
            seen.append(threading.current_thread().name)
            return ConnectionVerdict(True, "Connected successfully")

        monkeypatch.setattr(ConnectionService, "test_connection_verdict", staticmethod(record))

        response, _ = _post_test_within(api_client, _config(1), JOIN_SECONDS)

        assert response is not None and response.status_code == 200
        assert seen, "the endpoint never called the verdict function"
        assert seen[0].startswith(POOL_PREFIX), (
            f"the driver ran on {seen[0]!r}, not on the connection-test pool. A stalled test there "
            "occupies a thread every other synchronous API handler needs."
        )


class _NoTranslations:
    translations: dict = {}


@pytest.fixture
def ui_state(monkeypatch):
    """A ConnectionState whose parent vars and translations resolve without a running app."""
    from datanika.ui.state.base_state import BaseState
    from datanika.ui.state.connection_state import ConnectionState

    async def _get_state(self, state_cls):
        return _NoTranslations()

    async def _allow(self, min_role):
        """core#1370 gated both Test handlers at `editor`; this file is about the pool, not the
        audience. The gate is asserted in `tests/test_security/test_connection_test_role_gate.py`.
        """
        return True

    monkeypatch.setattr(ConnectionState, "get_state", _get_state)
    monkeypatch.setattr(ConnectionState, "_check_role", _allow)
    return ConnectionState(parent_state=BaseState(init_substates=False), init_substates=False)


class TestTheFormTestRunsOnTheSamePool:
    """The UI half. Without this, reverting the UI to the shared executor has no red to show.

    ``test_connection_test_off_the_loop.py`` asserts the loop keeps ticking and the answer
    arrives on budget — both equally true of ``asyncio.to_thread``. Which executor the driver
    runs in is a different property, and this is what pins it.
    """

    def test_the_form_test_runs_the_driver_on_the_connection_test_pool(
        self, short_budget, ui_state, monkeypatch
    ):
        import asyncio  # local: this file's other paths are synchronous

        seen: list[str] = []

        def record(config, connection_type):
            seen.append(threading.current_thread().name)
            return ConnectionVerdict(True, "Connected successfully")

        monkeypatch.setattr(ConnectionService, "test_connection_verdict", staticmethod(record))
        state = ui_state
        state.form_name = "probe"
        state.form_type = "mysql"
        state.form_host = "127.0.0.1"
        state.form_port = "3306"
        state.form_user = "probe"
        state.form_password = "probe"
        state.form_database = "probe"

        asyncio.run(state.test_connection_from_form())

        assert seen, "the handler never reached the verdict function"
        assert seen[0].startswith(POOL_PREFIX), (
            f"the form's Test ran the driver on {seen[0]!r} — the executor every synchronous API "
            "handler in this process shares."
        )


class TestASaturatedPoolIsAnsweredNotQueued:
    def test_a_test_that_cannot_get_a_slot_is_answered_promptly(self, short_budget, monkeypatch):
        """With every slot held, the next test answers at once and says what happened.

        It must not queue: queueing turns one stalled host into a wait for every other user of this
        process, which is what the pool exists to prevent.

        🔑 **The slots are taken with `try_submit_connection_test`, and retried, for two reasons —
        both found by the sweep.** Filling the pool with `run_connection_test_bounded` cannot work:
        on a full pool it *answers busy* rather than waiting, so a refused holder exits quietly and
        the pool ends up one slot short with nothing red. That is invisible until the pool is not
        empty on entry — which depends on what ran before. The mutation sweep reordered the nodes,
        this test landed after the arm that leaves a real driver holding a slot for a couple of
        seconds, and it failed under a mutation that touches none of this. `try_submit` returns
        `None` when refused, so a retry loop can tell "refused" from "running" and the arrangement
        stops depending on order. Draining the futures in `finally` then leaves the pool as it was
        found, rather than passing the same problem to the next test.
        """
        release = threading.Event()
        started = threading.Semaphore(0)

        def blocker(config, connection_type):
            started.release()
            release.wait(JOIN_SECONDS)
            return ConnectionVerdict(False, "released")

        monkeypatch.setattr(ConnectionService, "test_connection_verdict", staticmethod(blocker))
        size = connection_service.CONNECTION_TEST_POOL_SIZE
        held: list = []
        try:
            deadline = time.monotonic() + JOIN_SECONDS
            while len(held) < size:
                future = connection_service.try_submit_connection_test(
                    _config(1), ConnectionType.MYSQL
                )
                if future is None:
                    assert time.monotonic() < deadline, (
                        f"took only {len(held)} of {size} slots in {JOIN_SECONDS}s — another "
                        "test's driver still holds one, so this never tested a saturated pool."
                    )
                    time.sleep(0.1)
                    continue
                held.append(future)
            for _ in range(size):
                assert started.acquire(timeout=JOIN_SECONDS), "a held slot never started running"

            begin = time.monotonic()
            verdict = connection_service.run_connection_test_bounded(
                _config(1), ConnectionType.MYSQL
            )
            elapsed = time.monotonic() - begin

            assert verdict.reason == "busy", verdict
            assert verdict.ok is False, verdict
            assert elapsed < BUDGET, f"it queued for {elapsed:.1f}s instead of answering"
        finally:
            release.set()
            for future in held:
                with contextlib.suppress(Exception):
                    future.result(timeout=JOIN_SECONDS)

    def test_control_an_idle_pool_runs_the_test_rather_than_refusing(
        self, short_budget, monkeypatch
    ):
        """Without this, the refusal above is satisfied by a pool that refuses everything.

        ⚠️ **The pool is process-wide, so this control has to wait for it.** Other tests in the same
        session hand a real driver a host that never answers, and by design those threads keep their
        slots until the driver gives up. Measured: this passed when the file ran alone and failed in
        a larger selection — cross-file coupling, not the property under test. So it waits for a
        slot, and a failure here names the leak rather than blaming the pool.
        """
        monkeypatch.setattr(
            ConnectionService,
            "test_connection_verdict",
            staticmethod(lambda config, connection_type: ConnectionVerdict(True, "Connected")),
        )

        deadline = time.monotonic() + JOIN_SECONDS
        while True:
            verdict = connection_service.run_connection_test_bounded(
                _config(1), ConnectionType.MYSQL
            )
            if verdict.reason != "busy" or time.monotonic() >= deadline:
                break
            time.sleep(0.2)

        assert verdict.ok is True and verdict.reason == "", (
            f"an idle pool refused the test: {verdict}. A 'busy' here means every slot was still "
            f"held after {JOIN_SECONDS}s by another test's stalled driver."
        )


class TestTheBusySentenceIsTranslated:
    def test_the_service_builds_it_and_the_ui_maps_it(self):
        from datanika.ui.state.connection_state import _VERDICT_KEYS

        verdict = connection_service.ConnectionService.busy_verdict()

        assert isinstance(verdict, ConnectionVerdict)
        assert (verdict.ok, verdict.reason) == (False, "busy")
        assert _VERDICT_KEYS.get("busy") == BUSY_KEY

    @pytest.mark.parametrize("locale", LOCALES)
    def test_the_sentence_exists_in_every_locale(self, locale):
        data = json.loads((I18N / f"{locale}.json").read_text(encoding="utf-8"))
        en = json.loads((I18N / "en.json").read_text(encoding="utf-8"))

        assert BUSY_KEY in data, f"{locale}.json is missing {BUSY_KEY}"
        if locale != "en":
            assert data[BUSY_KEY] != en[BUSY_KEY], f"{locale}.json's {BUSY_KEY} is English verbatim"

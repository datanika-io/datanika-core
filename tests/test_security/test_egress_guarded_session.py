"""Regression probe for the #338 egress residuals (core#403) — the MCP P3 gate.

``validate_egress_host`` is a **pre-flight** check on ``base_url`` only, which
leaves three ways to reach an internal address after it has passed, all of them
live on the pinned dlt 1.21.0:

  (c) **redirect hops** — ``requests`` follows 30x inside ``Session.send``, so a
      public host can bounce the worker to ``10.0.0.0/8``;
  (d) **an absolute resource path** — ``RESTClient._create_request`` uses
      ``path_or_url`` verbatim when it parses as http(s), ignoring ``base_url``
      entirely, and ``resources`` comes straight from user-supplied
      ``dlt_config``;
  (+) **paginator ``next`` URLs**, which dlt reads out of the *response body* —
      attacker-controlled by definition in an SSRF threat model.

All three dispatch through ``requests.Session.send``, so one guarded session
closes them. These tests therefore pin the *seam*, not each vector.

The hop test drives a **real local redirect server over a real socket**. Whether
every hop actually re-enters ``Session.send`` is a fact about ``requests`` and
dlt's retry session — exactly the kind of claim that reading the source can get
wrong and an empirical check cannot.
"""

import http.server
import threading

import pytest
import requests

from datanika.services.egress_guard import (
    EgressValidationError,
    build_guarded_session,
    validate_egress_host,
)


class _RedirectHandler(http.server.BaseHTTPRequestHandler):
    """``/hop`` 302s to ``/final``; ``/final`` answers 200."""

    def do_GET(self):  # noqa: N802 - stdlib callback name
        if self.path == "/hop":
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/final")
            self.end_headers()
            return
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # keep the test output clean
        pass


@pytest.fixture
def redirect_server():
    server = http.server.HTTPServer(("127.0.0.1", 0), _RedirectHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


class TestEveryHopReachesTheGuard:
    def test_redirect_target_is_validated_before_it_is_fetched(self, redirect_server, monkeypatch):
        """The whole point of (c): the *second* hop must be checked too.

        Since core#405 the authoritative check is the resolve-and-pin in the
        adapter, so that is what's recorded here — one resolution per hop, and
        no second lookup anywhere (a second one would be the rebinding window
        the pinning exists to close).
        """
        seen: list[str] = []

        def _record(hostname):
            seen.append(hostname)
            return "127.0.0.1"

        monkeypatch.setattr("datanika.services.egress_guard.resolve_public_ip", _record)

        session = build_guarded_session()
        resp = session.get(f"{redirect_server}/hop")

        assert resp.status_code == 200
        assert seen == ["127.0.0.1", "127.0.0.1"], f"redirect hop escaped the guard: {seen}"

    def test_a_redirect_to_a_private_address_is_blocked(self, redirect_server):
        """With the real validator, the 127.0.0.1 hop must raise, not fetch."""
        session = build_guarded_session()
        with pytest.raises(EgressValidationError):
            session.get(f"{redirect_server}/hop")

    def test_absolute_url_request_is_validated(self):
        """(d): an absolute resource path bypasses base_url — but not the session."""
        session = build_guarded_session()
        with pytest.raises(EgressValidationError):
            session.get("http://169.254.169.254/latest/meta-data/")

    def test_public_request_passes_through(self, monkeypatch, redirect_server):
        """A public host must still work — the guard is a filter, not a wall."""
        monkeypatch.setattr(
            "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
        )
        session = build_guarded_session()
        assert session.get(f"{redirect_server}/final").json() == {"ok": True}


class TestRetryBehaviourIsPreserved:
    def test_session_is_dlts_retrying_session_not_a_bare_one(self):
        """RESTClient only builds its retry session when none is passed in.

        Handing dlt a plain ``requests.Session`` would silently drop retry and
        backoff for every rest_api connector — a reliability regression bought
        with a security fix. So the guard has to wrap dlt's session, and this
        pins that it does.
        """
        from dlt.sources.helpers.requests.retry import Client

        guarded = build_guarded_session()
        reference = Client(raise_for_status=False).session

        assert isinstance(guarded, requests.Session)
        # Same concrete session class dlt would have constructed itself.
        assert type(guarded) is type(reference)
        # And its adapters still carry retry configuration.
        adapter = guarded.get_adapter("https://example.com")
        assert getattr(adapter, "max_retries", None) is not None


class TestGuardIsWiredIntoTheRestApiBuild:
    def test_rest_api_source_receives_the_guarded_session(self, monkeypatch):
        """A guard nobody injects is decoration — pin the wiring."""
        import datanika.services.dlt_runner as dlt_runner

        captured = {}

        def _fake_rest_api_source(config, **kwargs):
            captured["config"] = config
            return object()

        monkeypatch.setattr(dlt_runner, "rest_api_source", _fake_rest_api_source)
        monkeypatch.setattr(dlt_runner, "validate_egress_host", lambda url: None)

        dlt_runner.DltRunnerService._rest_api_from_parts(
            "https://api.example.com",
            [{"name": "r", "endpoint": {"path": "things"}}],
        )

        session = captured["config"]["client"].get("session")
        assert session is not None, "rest_api client config must carry the guarded session"
        # It must be the guarded one, not any old session.
        assert getattr(session, "_datanika_egress_guarded", False) is True


class TestPreFlightGuardStillApplies:
    """The new session does not replace the pre-flight check; both hold."""

    def test_base_url_is_still_rejected_up_front(self):
        with pytest.raises(EgressValidationError):
            validate_egress_host("http://127.0.0.1/admin")

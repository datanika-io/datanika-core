"""The SaaS REST fallback must use the pinning session, not merely call the guard.

Write-up: ``plans/qa/notes/SECURITY-saas-fallback-egress-2026-07-22.md``. Held off
public GitHub until the fix ships, per the SAML precedent.

``dlt_runner`` had two REST client builders and only one of them got core#405's
pinning session. ``_rest_api_fallback`` — **the production path for all ten SaaS
connectors**, since none of the verified dlt source modules are installed in the
image — validated ``base_url`` once and then handed dlt the *hostname*, which
resolves a second time. That is the exact TOCTOU ``egress_guard``'s own docstring
claims core#405 closed, and ``salesforce``'s ``instance_url`` is a free-form URL
from the connection form, so it is reachable rather than theoretical.

**Why 2,500 green tests said nothing.** ``TestEgressGuardWiring`` asserts
``validate_egress_host`` *is called* on both paths. That is true, and it is the
pre-flight check — but it never asserts the session is pinned. *The guard was
tested for presence, not for effect.* So the tests here are the inverse: they
assert the pinned session is the one **dlt actually uses**.

The discriminator is a hostname that cannot resolve. ``rebind.invalid`` is a
reserved TLD (RFC 2606); the pinned address is supplied only by
``resolve_public_ip``. So a row can arrive **only** if dlt connected to the
address we validated instead of resolving the name itself — which is precisely
what the missing session changed. ``TestTheProbeDiscriminates`` proves the
unguarded shape fails the same probe, so this is not a green that measures
nothing.
"""

import ast
import http.server
import importlib.util
import json
import threading
from pathlib import Path

import pytest
from dlt.sources.rest_api import rest_api_source

import datanika
import datanika.services.dlt_runner as dlt_runner

# Never resolvable (RFC 2606), so reaching the server proves the socket went to
# the pinned address rather than to a second DNS answer.
PINNED_HOST = "rebind.invalid"
PINNED_IP = "127.0.0.1"


class _ItemsHandler(http.server.BaseHTTPRequestHandler):
    """Serves one row and records the headers it was reached with."""

    seen: list[dict] = []

    def do_GET(self):  # noqa: N802 - stdlib callback name
        type(self).seen.append(dict(self.headers))
        body = json.dumps([{"id": 1, "name": "reached-the-pinned-address"}]).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def pinned_api(monkeypatch):
    """A real server on the loopback address, reachable only via the pin.

    ``validate_egress_host`` is the pre-flight and has its own tests; it is
    no-opped so loopback is testable at all. ``resolve_public_ip`` is the
    single authoritative resolve-validate-pin since core#441 — patching only
    the former no longer reaches a loopback test server.
    """
    _ItemsHandler.seen = []
    server = http.server.HTTPServer((PINNED_IP, 0), _ItemsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    monkeypatch.setattr(dlt_runner, "validate_egress_host", lambda url: None)
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: PINNED_IP
    )

    try:
        yield f"http://{PINNED_HOST}:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


def _resources():
    return [{"name": "accounts", "endpoint": {"path": "items"}}]


class TestFallbackUsesThePinnedSession:
    def test_salesforce_reaches_only_the_pinned_address(self, pinned_api):
        """The reachable connector, driven through its production entry point.

        ``salesforce`` is the one fallback connector whose host is free-form
        user input, so this drives ``_build_saas_source`` rather than the
        builder underneath it — the unguarded client was picked *by a call
        site*, and a test that skips the call site cannot see that choice.
        """
        assert importlib.util.find_spec("salesforce") is None, (
            "the verified `salesforce` source is installed here, so this test "
            "would take the non-fallback branch and prove nothing about the "
            "path that runs in the image"
        )

        source = dlt_runner.DltRunnerService()._build_saas_source(
            "salesforce",
            {"access_token": "sekrit", "instance_url": pinned_api},
            {"resources": _resources()},
        )
        rows = list(source.resources["accounts"])

        assert [r["name"] for r in rows] == ["reached-the-pinned-address"], (
            f"dlt did not connect to the pinned address; got {rows}"
        )

    def test_auth_survives_alongside_the_pinned_session(self, pinned_api):
        """Passing a session must not cost the connector its credentials.

        The open question on the fix was whether dlt overrides the session when
        ``auth`` is also configured. Measured here rather than read: the bearer
        token arrives on a request that could only have been made through the
        pinned session.
        """
        source = dlt_runner.DltRunnerService()._build_saas_source(
            "salesforce",
            {"access_token": "sekrit", "instance_url": pinned_api},
            {"resources": _resources()},
        )
        list(source.resources["accounts"])

        assert _ItemsHandler.seen, "server was never reached"
        assert _ItemsHandler.seen[0].get("Authorization") == "Bearer sekrit"

    def test_host_header_stays_the_hostname(self, pinned_api):
        """Pinning must not break virtual-hosted APIs.

        The socket goes to the IP; the ``Host`` header — and, over TLS, the SNI
        and certificate identity — stay the hostname. Half the SaaS fallbacks
        are virtual-hosted (``*.myshopify.com``, ``*.zendesk.com``), so a fix
        that sent the IP as ``Host`` would route them to the wrong tenant.
        """
        source = dlt_runner.DltRunnerService()._build_saas_source(
            "salesforce",
            {"access_token": "sekrit", "instance_url": pinned_api},
            {"resources": _resources()},
        )
        list(source.resources["accounts"])

        host = _ItemsHandler.seen[0].get("Host", "")
        assert host.startswith(PINNED_HOST), f"Host header leaked the pinned IP: {host!r}"

    def test_fallback_hands_dlt_a_guarded_session(self, pinned_api):
        """Fast structural companion to the behavioural tests above.

        Presence alone proved nothing here once — but paired with the
        behavioural tests it localises a regression to the wiring instead of
        leaving a failure that only says "no rows".
        """
        captured = {}
        original = dlt_runner.rest_api_source

        def _capture(config, *args, **kwargs):
            captured.update(config["client"])
            return original(config, *args, **kwargs)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(dlt_runner, "rest_api_source", _capture)
            dlt_runner.DltRunnerService()._build_saas_source(
                "salesforce",
                {"access_token": "sekrit", "instance_url": pinned_api},
                {"resources": _resources()},
            )

        session = captured.get("session")
        assert session is not None, "no session passed — dlt would build its own and re-resolve"
        assert getattr(session, "_datanika_egress_guarded", False) is True


class TestTheProbeDiscriminates:
    def test_the_unguarded_shape_cannot_reach_the_server(self, pinned_api):
        """The control. Without it the tests above are unfalsifiable.

        This is the exact client the fallback used to build — ``base_url`` and
        ``auth``, no session. dlt then resolves the hostname itself, and the
        hostname does not resolve, so no row can arrive. If this ever passes,
        the probe above has stopped measuring pinning and is green for some
        other reason.
        """
        source = rest_api_source(
            {
                "client": {"base_url": pinned_api, "auth": {"type": "bearer", "token": "sekrit"}},
                "resources": _resources(),
            }
        )

        with pytest.raises(Exception):  # noqa: B017 - any failure to connect proves the point
            list(source.resources["accounts"])

        assert not _ItemsHandler.seen, "the unguarded client reached the server — probe is broken"


class TestOnlyOneClientBuilderExists:
    """The defect was that a wrong choice was *available*, not just taken.

    Two builders differing in one line, with the divergence invisible at every
    call site, is how a connector picks the unguarded one by accident. Deriving
    the check from the source rather than restating a rule is the only version
    that cannot drift: a new connector calling ``rest_api_source`` directly
    fails here at PR time.
    """

    @staticmethod
    def _call_sites():
        root = Path(datanika.__file__).parent
        sites = []
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            enclosing = {}
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                    for child in ast.walk(node):
                        enclosing.setdefault(child, node.name)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                name = getattr(func, "id", None) or getattr(func, "attr", None)
                if name == "rest_api_source":
                    sites.append(
                        (path.relative_to(root).as_posix(), enclosing.get(node), node.lineno)
                    )
        return sites

    def test_every_rest_api_source_call_goes_through_the_guarded_builder(self):
        sites = self._call_sites()

        # Anti-vacuity: a scan that finds nothing reports three green assertions
        # having checked nothing (the core#483 lesson).
        assert sites, "found no rest_api_source call sites — the scan is broken, not the code"

        stray = [s for s in sites if s[1] != "_rest_api_from_parts"]
        assert not stray, (
            "rest_api_source is called outside _rest_api_from_parts, so this call site "
            f"gets no pinning session: {stray}"
        )

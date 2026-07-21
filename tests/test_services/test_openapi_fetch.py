"""Tests for OpenAPI spec URL-fetch (core#410, OpenAPI P2 slice 1).

SPEC_OPENAPI_CONNECTOR §7 makes this a ship gate, and §14 calls SSRF "the
headline risk" — so the guards are the test subject, not the happy path.

The size cap is the one that needs a **real server**: a hostile origin can lie
about ``Content-Length`` or simply never stop sending, so checking the header
or the finished body proves nothing. The test streams more than the cap from a
local server and asserts we abort mid-flight.

Host/redirect rejection reuses ``validate_egress_host`` and the guarded session
from #403 — these tests pin that they are *wired in*, not that they work
(``test_egress_guarded_session.py`` owns that).
"""

import http.server
import threading

import pytest

from datanika.services.openapi_fetch import fetch_openapi_spec, resolve_spec
from datanika.services.openapi_import import MAX_SPEC_BYTES, OpenApiImportError

_SPEC = '{"openapi":"3.0.0","info":{"title":"t","version":"1"},"paths":{}}'


class _SpecHandler(http.server.BaseHTTPRequestHandler):
    """``/spec`` returns a small spec; ``/huge`` streams past the cap forever."""

    def do_GET(self):  # noqa: N802 - stdlib callback name
        if self.path == "/huge":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            # No Content-Length: the body is delimited by connection close, so
            # the client reads until EOF and nothing but our own cap bounds it.
            # (An *understated* Content-Length is not the dangerous case —
            # urllib3 stops at the declared length and truncates. Unbounded is.)
            self.end_headers()
            chunk = b"x" * 64_000
            try:
                for _ in range((MAX_SPEC_BYTES // len(chunk)) + 20):
                    self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
                pass  # expected: the client aborts at the cap
            return

        body = _SPEC.encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def spec_server():
    server = http.server.HTTPServer(("127.0.0.1", 0), _SpecHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def allow_loopback(monkeypatch):
    """Let the local test server through the public-IP guard.

    Only for tests about *other* properties — the guard's own behaviour is
    asserted with it left alone.
    """
    monkeypatch.setattr("datanika.services.openapi_fetch.validate_egress_host", lambda url: None)
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    # Since core#405 the guarded session pins the address it resolves, so the
    # loopback test server has to come back from *that* call too.
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
    )
    # stdlib http.server speaks plaintext; the https rule is asserted separately
    # against the real code path in TestSchemeIsHttpsOnly.
    monkeypatch.setattr("datanika.services.openapi_fetch.REQUIRED_SCHEME", "http")


class TestSchemeIsHttpsOnly:
    def test_http_is_refused(self):
        """§7 is stricter than the connector guard: https only for spec fetch."""
        with pytest.raises(OpenApiImportError) as exc:
            fetch_openapi_spec("http://example.com/openapi.json")
        assert exc.value.code == "invalid_spec_url"

    def test_file_scheme_is_refused(self):
        with pytest.raises(OpenApiImportError) as exc:
            fetch_openapi_spec("file:///etc/passwd")
        assert exc.value.code == "invalid_spec_url"


class TestHostGuardIsWiredIn:
    def test_private_host_is_refused(self):
        """Reuses validate_egress_host rather than a second implementation."""
        with pytest.raises(OpenApiImportError) as exc:
            fetch_openapi_spec("https://127.0.0.1/openapi.json")
        assert exc.value.code == "invalid_spec_url"

    def test_cloud_metadata_is_refused(self):
        with pytest.raises(OpenApiImportError) as exc:
            fetch_openapi_spec("https://169.254.169.254/latest/meta-data/")
        assert exc.value.code == "invalid_spec_url"


class TestSizeCapIsEnforcedWhileStreaming:
    def test_oversized_body_aborts_and_does_not_buffer(self, spec_server, allow_loopback):
        """A hostile origin can understate Content-Length or never stop."""
        with pytest.raises(OpenApiImportError) as exc:
            fetch_openapi_spec(f"{spec_server}/huge")
        assert exc.value.code == "spec_too_large"


class TestResolveSpecSource:
    """The 'exactly one of' decision, which the route itself cannot be tested on.

    ``parse_openapi`` is wrapped by ``@api_endpoint`` for auth and exposes no
    ``__wrapped__``, so this branch would ship untested if it lived inline —
    which is why it doesn't.
    """

    def test_neither_source_is_an_error(self):
        with pytest.raises(OpenApiImportError) as exc:
            resolve_spec(None, None)
        assert exc.value.code == "invalid_request"

    def test_both_sources_is_an_error(self):
        """Ambiguous, not incomplete — silently preferring one would surprise."""
        with pytest.raises(OpenApiImportError) as exc:
            resolve_spec(_SPEC, "https://example.com/openapi.json")
        assert exc.value.code == "invalid_request"

    def test_inline_passes_through_untouched(self):
        assert resolve_spec(_SPEC, None) == _SPEC

    def test_non_string_inline_is_rejected(self):
        with pytest.raises(OpenApiImportError) as exc:
            resolve_spec({"openapi": "3.0.0"}, None)
        assert exc.value.code == "invalid_request"

    def test_url_is_fetched(self, spec_server, allow_loopback):
        assert resolve_spec(None, f"{spec_server}/spec") == _SPEC


class TestHappyPath:
    def test_returns_the_body(self, spec_server, allow_loopback):
        assert fetch_openapi_spec(f"{spec_server}/spec") == _SPEC

    def test_result_feeds_the_parser_unchanged(self, spec_server, allow_loopback):
        """The fetcher's only job is bytes; parsing stays deterministic."""
        from datanika.services.openapi_import import parse_openapi_spec

        parsed = parse_openapi_spec(
            fetch_openapi_spec(f"{spec_server}/spec"), base_url_override="https://api.example.com"
        )
        assert parsed.base_url == "https://api.example.com"

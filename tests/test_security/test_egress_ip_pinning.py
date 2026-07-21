"""DNS-rebinding TOCTOU: connect to the IP we validated (core#405).

Both earlier layers validate by *resolving a hostname*, then hand the hostname
to ``requests``, which resolves it **again** to open the socket. A hostile
resolver can answer differently the second time — public for our check, an
internal address for the connection — and the guard never sees it.

Closing that means pinning: resolve once, validate, and connect to **that
address**, while keeping the ``Host`` header and TLS SNI as the hostname so
certificates still validate and virtual hosts still route.

That last clause is the whole risk of this change. Pinning is easy; pinning
*without quietly breaking TLS verification* is the part worth testing, because
the failure mode of getting it wrong (SNI becomes the IP, or hostname checking
is disabled to make it "work") is a weaker system that still passes a naive
"did it connect" test. So the SNI and certificate assertions here run against a
real TLS server rather than being asserted about the code.
"""

import http.server
import json
import socket
import ssl
import threading
from datetime import UTC, datetime, timedelta

import pytest

from datanika.services.egress_guard import (
    EgressValidationError,
    build_guarded_session,
    resolve_public_ip,
)


class _EchoHandler(http.server.BaseHTTPRequestHandler):
    """Echoes back the Host header it received."""

    def do_GET(self):  # noqa: N802 - stdlib callback name
        body = json.dumps({"host_header": self.headers.get("Host", "")}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def echo_server():
    server = http.server.HTTPServer(("127.0.0.1", 0), _EchoHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        server.server_close()


class TestResolveValidatedAddress:
    def test_public_host_returns_an_address(self, monkeypatch):
        monkeypatch.setattr(
            "datanika.services.egress_guard.socket.getaddrinfo",
            lambda host, *a, **kw: [(socket.AF_INET, None, None, "", ("93.184.216.34", 0))],
        )
        assert resolve_public_ip("example.com") == "93.184.216.34"

    def test_private_answer_is_refused(self, monkeypatch):
        monkeypatch.setattr(
            "datanika.services.egress_guard.socket.getaddrinfo",
            lambda host, *a, **kw: [(socket.AF_INET, None, None, "", ("10.0.0.5", 0))],
        )
        with pytest.raises(EgressValidationError):
            resolve_public_ip("rebind.example")

    def test_mixed_answer_is_refused(self, monkeypatch):
        """One private address among many is still a refusal."""
        monkeypatch.setattr(
            "datanika.services.egress_guard.socket.getaddrinfo",
            lambda host, *a, **kw: [
                (socket.AF_INET, None, None, "", ("93.184.216.34", 0)),
                (socket.AF_INET, None, None, "", ("169.254.169.254", 0)),
            ],
        )
        with pytest.raises(EgressValidationError):
            resolve_public_ip("half-evil.example")

    def test_unresolvable_fails_closed(self, monkeypatch):
        def _boom(*a, **kw):
            raise socket.gaierror("nope")

        monkeypatch.setattr("datanika.services.egress_guard.socket.getaddrinfo", _boom)
        with pytest.raises(EgressValidationError):
            resolve_public_ip("does-not-resolve.invalid")


class TestPinningBeatsARebind:
    def test_connection_goes_to_the_validated_address(self, echo_server, monkeypatch):
        """The classic rebind: validation says public, the next lookup says otherwise.

        The resolver is asked once and answers with the loopback test server.
        If the adapter re-resolved at connect time it would go somewhere else
        (or fail); arriving here proves the validated address was pinned.
        """
        calls = []

        def _resolve(host):
            calls.append(host)
            return "127.0.0.1"

        monkeypatch.setattr("datanika.services.egress_guard.resolve_public_ip", _resolve)

        session = build_guarded_session()
        resp = session.get(f"http://pinned.example:{echo_server}/", timeout=5)

        assert resp.status_code == 200
        assert calls == ["pinned.example"], "resolved more than once — that is the TOCTOU"

    def test_host_header_stays_the_hostname(self, echo_server, monkeypatch):
        """Sending the IP as Host breaks every virtual-hosted API."""
        monkeypatch.setattr(
            "datanika.services.egress_guard.resolve_public_ip", lambda host: "127.0.0.1"
        )

        session = build_guarded_session()
        resp = session.get(f"http://pinned.example:{echo_server}/", timeout=5)

        assert resp.json()["host_header"].startswith("pinned.example")

    def test_a_refused_host_never_connects(self, monkeypatch):
        def _refuse(host):
            raise EgressValidationError(f"{host} resolves to a non-public address")

        monkeypatch.setattr("datanika.services.egress_guard.resolve_public_ip", _refuse)

        session = build_guarded_session()
        with pytest.raises(Exception) as exc:  # requests wraps adapter errors
            session.get("http://rebind.example/", timeout=5)
        assert "non-public" in str(exc.value)


# ---------------------------------------------------------------------------
# TLS — the part that would silently weaken if implemented carelessly
# ---------------------------------------------------------------------------


def _self_signed(tmp_path, hostname: str):
    """A throwaway cert for *hostname* so certificate matching is real."""
    crypto = pytest.importorskip("cryptography")
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    assert crypto  # imported for the skip guard
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hostname)])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(days=1))
        .not_valid_after(datetime.now(UTC) + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(hostname)]), critical=False)
        .sign(key, hashes.SHA256())
    )
    cert_path = tmp_path / "cert.pem"
    key_path = tmp_path / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return str(cert_path), str(key_path)


@pytest.fixture
def tls_server(tmp_path):
    """A TLS server on loopback that records the SNI name it was offered."""
    hostname = "pinned.example"
    cert_path, key_path = _self_signed(tmp_path, hostname)
    seen: dict = {}

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)

    def _sni_callback(sock, server_name, ctx):
        seen["sni"] = server_name

    context.sni_callback = _sni_callback

    server = http.server.HTTPServer(("127.0.0.1", 0), _EchoHandler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port, cert_path, seen
    finally:
        server.shutdown()
        server.server_close()


class TestTlsIdentityIsPreserved:
    def test_sni_is_the_hostname_not_the_pinned_ip(self, tls_server, monkeypatch):
        """If SNI became the IP, every SNI-based host would serve the wrong cert."""
        port, cert_path, seen = tls_server
        monkeypatch.setattr(
            "datanika.services.egress_guard.resolve_public_ip", lambda host: "127.0.0.1"
        )

        session = build_guarded_session()
        session.get(f"https://pinned.example:{port}/", timeout=5, verify=cert_path)

        assert seen["sni"] == "pinned.example"

    def test_certificate_is_still_verified_against_the_hostname(self, tls_server, monkeypatch):
        """The cert is for pinned.example; asking for another name must fail.

        This is the assertion that catches "made it work" by disabling
        hostname checking — the failure mode that turns a hardening change
        into a downgrade.
        """
        port, cert_path, _seen = tls_server
        monkeypatch.setattr(
            "datanika.services.egress_guard.resolve_public_ip", lambda host: "127.0.0.1"
        )

        session = build_guarded_session()
        with pytest.raises(Exception) as exc:
            session.get(f"https://wrong-name.example:{port}/", timeout=5, verify=cert_path)

        blob = str(exc.value).lower()
        assert "certificate" in blob or "hostname" in blob or "ssl" in blob

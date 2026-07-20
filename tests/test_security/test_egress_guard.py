"""Security tests for the SSRF worker-egress guard (core#338).

``validate_egress_host`` is a pre-flight guard for the ``rest_api`` connector
family. It resolves a user-supplied URL's hostname and rejects it if ANY
resolved IP is non-public (private, loopback, link-local incl. the
169.254.169.254 cloud-metadata IP, multicast, reserved, or unspecified).

All DNS is mocked via ``socket.getaddrinfo`` — these tests never touch the
network and are fully deterministic.
"""

import socket
from unittest.mock import patch

import pytest

from datanika.services.egress_guard import EgressValidationError, validate_egress_host


def _gai(*ips: str):
    """Build a ``socket.getaddrinfo``-style return value for the given IPs.

    Each tuple is ``(family, type, proto, canonname, sockaddr)``; the guard
    only reads ``sockaddr[0]`` (index ``[4][0]``), so a 2-tuple sockaddr is
    sufficient for both IPv4 and IPv6.
    """
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 0)) for ip in ips]


# ---------------------------------------------------------------------------
# REJECT — resolved IP is internal / non-public
# ---------------------------------------------------------------------------
class TestRejectsInternalIps:
    @pytest.mark.parametrize(
        ("label", "ip"),
        [
            ("loopback_v4", "127.0.0.1"),
            ("cloud_metadata_link_local", "169.254.169.254"),
            ("private_10_8", "10.0.0.1"),
            ("private_192_168", "192.168.1.1"),
            ("private_172_16", "172.16.0.1"),
            ("loopback_v6", "::1"),
            ("ula_v6", "fc00::1"),
            ("unspecified", "0.0.0.0"),
            ("ipv4_mapped_loopback", "::ffff:127.0.0.1"),
        ],
    )
    def test_rejects_internal_ip(self, label, ip):
        with (
            patch("socket.getaddrinfo", return_value=_gai(ip)),
            pytest.raises(EgressValidationError),
        ):
            validate_egress_host("https://user-supplied.example")

    def test_rejects_hostname_resolving_to_private(self):
        """A public-looking hostname that resolves to a private IP is rejected."""
        with (
            patch("socket.getaddrinfo", return_value=_gai("10.1.2.3")),
            pytest.raises(EgressValidationError),
        ):
            validate_egress_host("https://internal.corp.example")

    def test_rejects_if_any_resolved_ip_is_internal(self):
        """DNS-rebind style: one public + one private A record → reject."""
        with (
            patch("socket.getaddrinfo", return_value=_gai("93.184.216.34", "10.0.0.5")),
            pytest.raises(EgressValidationError),
        ):
            validate_egress_host("https://rebind.example")

    def test_error_message_names_host_and_ip(self):
        with (
            patch("socket.getaddrinfo", return_value=_gai("169.254.169.254")),
            pytest.raises(EgressValidationError) as exc,
        ):
            validate_egress_host("https://metadata.example")
        msg = str(exc.value)
        assert "metadata.example" in msg
        assert "169.254.169.254" in msg


# ---------------------------------------------------------------------------
# REJECT — malformed / unresolvable input
# ---------------------------------------------------------------------------
class TestRejectsBadInput:
    def test_rejects_non_http_scheme(self):
        # Scheme is checked before any DNS resolution; patch getaddrinfo to a
        # public IP so a missing scheme-check would surface as a failing test.
        with (
            patch("socket.getaddrinfo", return_value=_gai("93.184.216.34")),
            pytest.raises(EgressValidationError),
        ):
            validate_egress_host("ftp://example.com")

    def test_rejects_file_scheme(self):
        with pytest.raises(EgressValidationError):
            validate_egress_host("file:///etc/passwd")

    def test_rejects_missing_host(self):
        with pytest.raises(EgressValidationError):
            validate_egress_host("https://")

    def test_gaierror_fails_closed(self):
        """Unresolvable host → fail closed (reject), never fail open."""
        with (
            patch("socket.getaddrinfo", side_effect=socket.gaierror("no such host")),
            pytest.raises(EgressValidationError),
        ):
            validate_egress_host("https://does-not-resolve.invalid")


# ---------------------------------------------------------------------------
# ALLOW — resolved IP is public
# ---------------------------------------------------------------------------
class TestAllowsPublic:
    def test_allows_public_ipv4(self):
        with patch("socket.getaddrinfo", return_value=_gai("93.184.216.34")):
            assert validate_egress_host("https://example.com") is None

    def test_allows_public_ipv6(self):
        with patch(
            "socket.getaddrinfo",
            return_value=_gai("2606:2800:220:1:248:1893:25c8:1946"),
        ):
            assert validate_egress_host("https://ipv6.example.com") is None

    def test_allows_http_scheme(self):
        with patch("socket.getaddrinfo", return_value=_gai("93.184.216.34")):
            assert validate_egress_host("http://example.com") is None

    def test_allows_all_public_multi_ip(self):
        with patch("socket.getaddrinfo", return_value=_gai("93.184.216.34", "8.8.8.8")):
            assert validate_egress_host("https://multi.example.com") is None

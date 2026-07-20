"""Defense-in-depth PRE-FLIGHT SSRF guard for the ``rest_api`` connector family (core#338).

User-supplied base URLs (generic ``rest_api``/``openapi`` connectors and the
SaaS fallbacks whose host is user-controlled — e.g. Salesforce ``instance_url``,
Shopify store) are handed to dlt's ``requests``-based REST client and fetched
from inside the worker. Without a guard, a user could point a connector at
``http://169.254.169.254/…`` (cloud metadata) or an internal ``10.0.0.0/8`` /
``192.168.0.0/16`` service and exfiltrate it via the pipeline. ``validate_egress_host``
resolves the hostname and rejects the request if ANY resolved IP is non-public.

This is a pre-flight check only. It has TWO KNOWN RESIDUAL GAPS, deferred to P3
(to be closed before any cloud-worker migration, and before enabling the
OpenAPI-P2 URL-fetch feature):

  (c) **Redirect hops are NOT re-validated.** dlt's REST client is ``requests``
      based and follows redirects inside ``.session.send()``; a public host that
      responds with a 30x redirect to a private IP would bypass this guard.
      Closing it needs a custom ``requests.Session`` / ``HTTPAdapter`` that
      re-validates every hop's resolved address.

  (d) **A resource ``path`` that is itself an absolute ``http(s)://`` URL bypasses
      ``base_url``** (dlt ``rest_client`` ``client.py:122-124`` joins/overrides the
      base with an absolute path) and is not validated here — only ``base_url`` is
      checked.
"""

import ipaddress
import socket
from urllib.parse import urlparse

_ALLOWED_SCHEMES = {"http", "https"}


class EgressValidationError(ValueError):
    """Raised when a user-supplied URL points at a non-public / internal host."""


def validate_egress_host(url: str) -> None:
    """Reject *url* if its host resolves to any non-public IP address.

    Raises :class:`EgressValidationError` for a non-``http(s)`` scheme, a
    missing host, an unresolvable host (fail closed), or when ANY resolved IP
    is private, loopback, link-local (incl. ``169.254.169.254`` cloud metadata),
    multicast, reserved, or unspecified. Returns ``None`` when every resolved
    IP is public.
    """
    parsed = urlparse(url)
    if parsed.scheme not in _ALLOWED_SCHEMES:
        raise EgressValidationError(f"URL scheme must be http or https, got {parsed.scheme!r}")

    hostname = parsed.hostname
    if not hostname:
        raise EgressValidationError(f"URL has no host: {url!r}")

    try:
        infos = socket.getaddrinfo(hostname, None)
    except socket.gaierror as exc:
        # Fail closed: if we cannot resolve the host, we cannot prove it is
        # public, so we refuse it.
        raise EgressValidationError(f"could not resolve host {hostname!r}: {exc}") from exc

    ip_strings = {info[4][0] for info in infos}
    for ip_str in ip_strings:
        ip = ipaddress.ip_address(ip_str)
        # Normalize IPv4-mapped IPv6 (::ffff:a.b.c.d) so 127.0.0.1 et al. are
        # classified against their real IPv4 range.
        if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
            ip = ip.ipv4_mapped
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            raise EgressValidationError(
                f"host {hostname!r} resolves to non-public address {ip_str} — refusing egress"
            )

    return None

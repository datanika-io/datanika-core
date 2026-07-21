"""Defense-in-depth PRE-FLIGHT SSRF guard for the ``rest_api`` connector family (core#338).

User-supplied base URLs (generic ``rest_api``/``openapi`` connectors and the
SaaS fallbacks whose host is user-controlled — e.g. Salesforce ``instance_url``,
Shopify store) are handed to dlt's ``requests``-based REST client and fetched
from inside the worker. Without a guard, a user could point a connector at
``http://169.254.169.254/…`` (cloud metadata) or an internal ``10.0.0.0/8`` /
``192.168.0.0/16`` service and exfiltrate it via the pipeline. ``validate_egress_host``
resolves the hostname and rejects the request if ANY resolved IP is non-public.

``validate_egress_host`` alone is only a **pre-flight** check on ``base_url``.
Three things happen *after* it passes that it cannot see, so
:func:`build_guarded_session` re-validates every request the worker actually
sends (core#403 — the gate for MCP P3 write tools):

  (c) **Redirect hops.** ``requests`` follows 30x inside ``Session.send``, so a
      public host can bounce the worker to a private address.

  (d) **An absolute resource path overrides ``base_url``.** dlt's
      ``RESTClient._create_request`` uses ``path_or_url`` verbatim when it parses
      as http(s), and ``resources`` comes straight from user-supplied
      ``dlt_config``.

  (+) **Paginator ``next`` URLs**, which dlt reads out of the *response body* —
      attacker-controlled by definition in this threat model.

All three dispatch through ``requests.Session.send``, which is why the guard
lives there rather than being enumerated vector by vector.

**Still open — DNS rebinding (TOCTOU).** We resolve a hostname to validate it and
``requests`` resolves it again to connect; a hostile resolver can answer
differently each time. Closing that needs IP pinning at the adapter (connect to
the validated address while preserving SNI/Host). This module is therefore
defense-in-depth, not a boundary — say so out loud rather than let a future
reader assume the gap is covered.
"""

import ipaddress
import socket
from urllib.parse import urlparse

import requests

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


def build_guarded_session() -> requests.Session:
    """Return dlt's retrying session, hardened to validate **every** request.

    Wrapping the instance's ``send`` (rather than subclassing ``Session``) is
    deliberate on two counts:

    * ``Session.send`` is the single choke point every dispatch passes through —
      the first request, an absolute-path override, each redirect hop, and each
      paginator ``next``. Guarding it covers vectors nobody has enumerated yet.
      ``resolve_redirects`` calls ``self.send``, so the instance attribute set
      here intercepts hops as well as the initial call.

    * The session must stay **dlt's own retrying session**. ``RESTClient`` builds
      ``Client(raise_for_status=False).session`` only when it is passed nothing,
      so handing it a bare ``requests.Session`` would quietly drop retry and
      backoff for every rest_api connector — a reliability regression bought
      with a security fix.

    One DNS resolution is added per request. That is deliberate: caching the
    result is exactly what would reopen the rebinding window this guard is
    already weakest against.
    """
    from dlt.sources.helpers.requests.retry import Client

    session = Client(raise_for_status=False).session
    original_send = session.send

    def guarded_send(request, **kwargs):
        validate_egress_host(request.url)
        return original_send(request, **kwargs)

    session.send = guarded_send
    # Lets callers (and tests) assert a session came from here rather than
    # trusting that some session was passed along.
    session._datanika_egress_guarded = True
    return session

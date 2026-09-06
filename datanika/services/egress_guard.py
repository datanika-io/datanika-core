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

**DNS rebinding (TOCTOU) — closed by pinning (core#405).** Validating a
hostname and then handing that hostname to ``requests`` means resolving twice,
and a hostile resolver can answer differently the second time. The guarded
session therefore resolves **once**, validates the answer, and connects to that
address, while keeping the ``Host`` header and TLS SNI as the hostname so
certificates still validate and virtual hosts still route — see
:class:`_PinnedIPAdapter`.
"""

import ipaddress
import socket
from urllib.parse import urlparse

import requests

from datanika.errors import UserFacingError

_ALLOWED_SCHEMES = {"http", "https"}


class EgressValidationError(UserFacingError):
    """Raised when a user-supplied URL points at a non-public / internal host."""


def validate_egress_host(url: str) -> None:
    """Reject *url* if its host resolves to any non-public IP address.

    Raises :class:`EgressValidationError` for a non-``http(s)`` scheme, a
    missing host, an unresolvable host (fail closed), or when ANY resolved IP
    is private, loopback, link-local (incl. ``169.254.169.254`` cloud metadata),
    multicast, reserved, or unspecified. Returns ``None`` when every resolved
    IP is public.
    """
    hostname = _check_scheme_and_host(url)

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


def _check_scheme_and_host(url: str) -> str:
    """Cheap structural check — no DNS. Returns the hostname."""
    parsed = urlparse(url)
    if parsed.scheme not in _ALLOWED_SCHEMES:
        raise EgressValidationError(f"URL scheme must be http or https, got {parsed.scheme!r}")
    if not parsed.hostname:
        raise EgressValidationError(f"URL has no host: {url!r}")
    return parsed.hostname


def resolve_public_ip(hostname: str) -> str:
    """Resolve *hostname* once and return an address, or refuse.

    Same rules as :func:`validate_egress_host`, but it hands back the address
    it approved so the caller can connect to **that** rather than resolving
    again. Every returned answer is checked — a hostile resolver that mixes one
    public address with an internal one is refused outright rather than raced.
    """
    try:
        infos = socket.getaddrinfo(hostname, None)
    except socket.gaierror as exc:
        raise EgressValidationError(f"could not resolve host {hostname!r}: {exc}") from exc

    chosen = None
    for info in infos:
        ip_str = info[4][0]
        ip = ipaddress.ip_address(ip_str)
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
        if chosen is None:
            chosen = ip_str

    if chosen is None:
        raise EgressValidationError(f"host {hostname!r} resolved to no addresses")
    return chosen


class _PinnedIPAdapter(requests.adapters.HTTPAdapter):
    """Connect to the address we validated, not to whatever DNS says next.

    The rebinding gap is that validating a *name* and then connecting to that
    same *name* is two resolutions with a window in between. This adapter
    resolves once via :func:`resolve_public_ip` and builds the connection pool
    against that address.

    Keeping TLS honest is the delicate half, and is why the pool is created
    with ``server_hostname`` set to the real hostname: urllib3 would otherwise
    offer the **IP** as SNI and match the certificate against it, so a careless
    implementation either serves the wrong virtual host or "fixes" itself by
    disabling hostname verification. Certificate matching still happens against
    the hostname (``assert_hostname`` is left alone so urllib3 falls back to
    ``server_hostname``), and the ``Host`` header is preserved so virtual-hosted
    APIs still route. ``tests/test_security/test_egress_ip_pinning.py`` asserts
    both against a real TLS server rather than trusting this paragraph.
    """

    def send(self, request, **kwargs):
        parsed = urlparse(request.url)
        if parsed.scheme in _ALLOWED_SCHEMES and parsed.hostname:
            # Host header must survive the swap or virtual hosts break.
            request.headers.setdefault("Host", parsed.netloc)
        return super().send(request, **kwargs)

    def get_connection_with_tls_context(self, request, verify, proxies=None, cert=None):
        pool = self._pinned_pool(request)
        if pool is not None:
            return pool
        return super().get_connection_with_tls_context(request, verify, proxies, cert)

    def get_connection(self, url, proxies=None):  # pragma: no cover - older requests
        parsed = urlparse(url)
        pool = self._pool_for(parsed)
        if pool is not None:
            return pool
        return super().get_connection(url, proxies)

    def _pinned_pool(self, request):
        return self._pool_for(urlparse(request.url))

    def _pool_for(self, parsed):
        if parsed.scheme not in _ALLOWED_SCHEMES or not parsed.hostname:
            return None
        hostname = parsed.hostname
        ip = resolve_public_ip(hostname)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        pool_kwargs = {}
        if parsed.scheme == "https":
            # SNI + certificate identity stay the hostname; only the socket
            # goes to the pinned address.
            pool_kwargs["server_hostname"] = hostname
        return self.poolmanager.connection_from_host(
            ip, port=port, scheme=parsed.scheme, pool_kwargs=pool_kwargs
        )


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
        # Scheme/host only — deliberately no DNS here. The adapter below does
        # the single authoritative resolve-validate-pin; resolving in both
        # places would mean two lookups per request with a rebinding window
        # *between our own two checks*, which is the very thing core#405 closes.
        _check_scheme_and_host(request.url)
        return original_send(request, **kwargs)

    session.send = guarded_send

    # Pin the validated address for every scheme this guard allows (core#405).
    # Mounted after the send-wrapper so both layers apply: the wrapper decides
    # whether a URL may be fetched at all, the adapter decides where the socket
    # actually goes.
    pinned = _PinnedIPAdapter()
    session.mount("http://", pinned)
    session.mount("https://", pinned)

    # Lets callers (and tests) assert a session came from here rather than
    # trusting that some session was passed along.
    session._datanika_egress_guarded = True
    return session

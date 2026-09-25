"""Shared harness for exercising ``@api_endpoint`` routes over real HTTP (core#416).

Routes decorated with ``@api_endpoint`` cannot be called directly — the
decorator owns auth and exposes no ``__wrapped__`` — so the only way to test a
route's own behaviour (status codes, error mapping, response shape) is through
a client with the middleware patched. That harness existed, but **file-local**
in ``test_transformation_compile.py``, which is why most ``api_v1`` routes have
no endpoint tests at all: the cost of writing the first one was copying it.

Lifted here unchanged in behaviour, minus the compile-specific bits (dbt dir,
Fernet key) which stay with the tests that need them. What's generic is the
auth layer, and that is all this provides.

Also owns the **resolver** for this directory — see ``_dns_is_never_live``
(core#1597). A fixture lives here rather than in a module because
``tests/test_fixture_sharing.py`` forbids importing one across test modules,
and because a per-module copy is exactly how the previous guard came to cover
one module out of four.
"""

import contextlib
import ipaddress
import socket
from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services import egress_guard
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.rate_limit_service import RateLimitResult

#: A public address the egress guard accepts, so the guard's own classification runs to
#: completion rather than being short-circuited. ``93.184.216.34`` is example.com's, and is
#: used here for the same reason ``tests/test_security/test_egress_guard.py`` uses it.
_PUBLIC_IP = "93.184.216.34"

#: Names that reach the real resolver. ``localhost`` is answered from the hosts file, not
#: from DNS, and the local-vendor tests in this directory bind real HTTP servers to it.
_LOOPBACK_NAMES = frozenset({"localhost", "localhost.localdomain", "", None})

#: Captured at import, before any fixture patches the module attribute. A denier that
#: called ``socket.getaddrinfo`` would recurse into whichever stub is installed.
_REAL_GETADDRINFO = socket.getaddrinfo


def _gai(*ips: str):
    """A ``socket.getaddrinfo``-shaped return value. The guard reads only ``[4][0]``."""
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 0)) for ip in ips]


def _needs_no_lookup(host) -> bool:
    """True when answering ``host`` involves no DNS query, so the ban does not apply.

    Two cases, and the second is wider than it first looks on purpose. An **IP literal**
    is parsed, never queried — so denying one would change *why* a private address is
    refused, from ``egress_guard``'s "private address" to a bogus "could not resolve",
    and a test asserting core#405's refusal would then pass for the wrong reason. A
    **loopback name** comes from the hosts file, and the local-vendor tests here bind
    real servers to it.
    """
    if host in _LOOPBACK_NAMES:
        return True
    try:
        ipaddress.ip_address(str(host))
    except ValueError:
        return False
    return True


@pytest.fixture(autouse=True)
def _dns_is_never_live(monkeypatch):
    """Resolution in this directory is either STUBBED or REFUSED — never the network.

    Building a SaaS source resolves the host: ``build_source`` ->
    ``_build_saas_source`` -> ``_rest_api_fallback`` -> ``_rest_api_from_parts`` ->
    ``validate_egress_host`` -> ``socket.getaddrinfo``. Nothing on that path is mocked.

    core#1280 fixed that in the module where it bit. core#1597 is the same defect in
    the other three, and the reason it recurred is that the remedy was a *per-module*
    fixture: module five joins the population in silence. So the resolver is owned
    here, for the whole directory, and a module that needs one opts in by requesting
    ``no_live_dns`` — which flips **this** resolver's mode rather than installing a
    second one. That is deliberate: two competing ``monkeypatch.setattr`` calls would
    make the outcome depend on fixture ordering, and an ordering that silently
    reverses leaves a green suite doing live lookups.

    🚨 **The NETWORK is replaced, not the GUARD.** ``validate_egress_host`` still runs
    and still classifies, so core#405's DNS-rebinding refusal is still exercised.
    ``tests/test_services/test_no_live_dns_is_a_stub_not_a_bypass.py`` is what holds
    that, and holds the refusal below to naming DNS rather than a connector.
    """
    state: dict = {"stub": None, "seen": []}

    def _resolver(host, *args, **kwargs):
        if _needs_no_lookup(host):
            return _REAL_GETADDRINFO(host, *args, **kwargs)
        state["seen"].append(host)
        if state["stub"] is None:
            raise socket.gaierror(
                socket.EAI_NONAME,
                f"live DNS is disabled under tests/test_services (core#1597): "
                f"something tried to resolve {host!r}. This is NOT a connector defect. "
                f"If this module builds a source, request the `no_live_dns` fixture; "
                f"if it should not be resolving at all, that is the bug.",
            )
        return _gai(state["stub"])

    monkeypatch.setattr(socket, "getaddrinfo", _resolver)
    return state


@pytest.fixture
def no_live_dns(_dns_is_never_live):
    """Opt this test into a resolver that answers instead of refusing.

    Returns the list of hosts resolved so far, so a module can assert that building a
    source really did go through the stub (anti-vacuity: a fixture protecting nothing
    reads exactly like a working one).

    Measured on the module core#1280 fixed: with resolution unavailable it was **46
    failed, 6 passed**, and the message it printed was about connector endpoints rather
    than about DNS — so the reader starts by looking for a connector defect that is not
    there. core#1597 measured the same shape in three more modules: 12 permanently red
    locally (``graph.facebook.com`` is the one host this dev machine cannot resolve)
    and 2 modules intermittently red, all green in CI.
    """
    _dns_is_never_live["stub"] = _PUBLIC_IP

    # ASSERT THE STUB IS IN EFFECT, behaviourally, through the real call path.
    #
    # A fixture that silently fails to patch hands back exactly today's behaviour and the
    # module goes green for the wrong reason — the defect this file is fixing, one level
    # up. `.invalid` is reserved by RFC 2606 and never resolves, so were the stub not in
    # effect this call would raise and the fixture would fail loudly instead of the tests
    # passing by luck. It also proves `egress_guard` still reaches `socket.getaddrinfo`:
    # if it switched to `from socket import getaddrinfo`, patching the module attribute
    # would no longer reach it.
    egress_guard.validate_egress_host("https://dns-stub-probe.invalid/x")
    assert _dns_is_never_live["seen"] == ["dns-stub-probe.invalid"], (
        "the DNS stub is not in effect on the path that resolves hosts "
        f"(recorded {_dns_is_never_live['seen']!r}). These tests would fall back to live "
        "DNS and pass for the wrong reason."
    )
    _dns_is_never_live["seen"].clear()
    return _dns_is_never_live["seen"]


@pytest.fixture
def fake_api_key():
    """An authenticated key with full access (``scopes=None`` = unscoped)."""
    key = MagicMock()
    key.id = 1
    key.user_id = 1
    key.name = "Test Key"
    key.scopes = None
    return key


@pytest.fixture
def rate_limit_ok():
    return RateLimitResult(
        allowed=True,
        current_count=1,
        limit=60,
        remaining=59,
        retry_after=0,
        reset_at=9999999999,
    )


@pytest.fixture
def client():
    return TestClient(Starlette(routes=api_v1_routes))


def api_headers(token: str = "etf_test") -> dict:
    return {"Authorization": f"Bearer {token}"}


@contextlib.contextmanager
def patch_api_auth(
    fake_api_key,
    rate_limit_ok,
    db_session,
    org_id: int = 1,
    *,
    authenticated: bool = True,
):
    """Patch ``api_middleware`` so a request reaches the handler.

    ``authenticated=False`` makes the key lookup fail the way the real service
    does on an unknown/expired/out-of-scope key, so a 401 can be asserted
    without hand-rolling the middleware's behaviour.
    """
    fake_api_key.org_id = org_id

    @contextlib.contextmanager
    def fake_session():
        yield db_session

    with (
        patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
        patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
        patch("datanika.services.api_middleware._get_session", fake_session),
    ):
        mock_svc.authenticate_api_key.return_value = fake_api_key if authenticated else None
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = rate_limit_ok
        yield mock_svc

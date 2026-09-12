"""The endpoint picker must narrow the load, and offer names that exist (core#532).

Two failures, one cause. The form wrote `dlt_config["endpoints"]` and **no
builder read it** — only Stripe's verified-source branch did, and that branch
never runs because `stripe_analytics` is not installed anywhere (see core#543).
So unticking a resource changed nothing.

Wiring the key through is not enough on its own: the names the picker offers
came from the *verified sources*, while what actually gets built is the REST
fallback, and for six connectors the two disagreed — Stripe offered `Product`
and `Price` where the fallback defines `products` and `prices`, HubSpot offered
six resources where three exist. Honouring the selection without first aligning
the names would have turned a dead control into a failing one.

`test_every_offered_endpoint_exists` is the guard for that half, and it is the
one that matters: it builds each source and compares against the resources dlt
actually produced, rather than restating a list.
"""

import socket

import pytest

from datanika.services import egress_guard
from datanika.services.dlt_runner import DltRunnerError, DltRunnerService
from datanika.services.egress_guard import EgressValidationError
from datanika.ui.state.connection_state import SAAS_DEFAULT_ENDPOINTS

# Minimal config to get each SaaS branch past its required-field checks. Values are
# placeholders and no HTTP request is made.
#
# 🔴 This said "no request is made; the source is only constructed" until 2026-09-12,
# and constructing a source RESOLVES THE HOST — see `no_live_dns` below (core#1280).
# The sentence was true about HTTP and false about DNS, and the difference is what made
# a transient resolver failure look like a connector defect.
SAAS_PROBE_CONFIG = {
    "stripe": {"api_key": "sk_test_x"},
    "github": {"access_token": "t", "owner": "o", "repo": "r"},
    "hubspot": {"api_key": "t"},
    "salesforce": {"access_token": "t", "instance_url": "https://x.my.salesforce.com"},
    "shopify": {"api_key": "t", "store": "s"},
    "jira": {"api_token": "t", "email": "e@x.com", "domain": "d"},
    "slack": {"api_key": "t"},
    "zendesk": {"api_token": "t", "email": "e@x.com", "subdomain": "s"},
    "airtable": {"api_key": "t", "base_id": "b"},
    "notion": {"api_key": "t"},
    "pipedrive": {"api_key": "t"},
    "freshdesk": {"api_key": "t", "domain": "d"},
    "asana": {"api_key": "t"},
    # Bare id on purpose: the builder normalises it to `act_…`, and users paste
    # it both ways.
    "facebook_ads": {"access_token": "t", "account_id": "123456789"},
    # Deliberately nonsense credentials: building a GA4 source must not mint a
    # token or touch the network, so an unusable key still has to construct
    # (core#543). If this starts failing, the builder has gained eager I/O.
    "google_analytics": {"property_id": "123", "service_account_json": "{}"},
    # Same reasoning as GA4: unusable credentials must still *construct*, since
    # the OAuth exchange happens inside the resource. If this starts failing,
    # the builder has gained eager I/O (core#555).
    "google_ads": {
        "customer_id": "123-456-7890",
        "developer_token": "t",
        "client_id": "c",
        "client_secret": "s",
        "refresh_token": "r",
    },
}

# Empty, and that is the news: every SaaS connector now builds a source.
#
# `facebook_ads` left when it gained a Graph API fallback (core#554),
# `google_analytics` when the Data API gave it a report shape (core#569), and
# `google_ads` last — it was the only one whose blocker was a *credential we did
# not collect* rather than a transport nobody had written, and collecting the
# developer token turned out to be a form change (core#555).
#
# Kept rather than deleted: a connector that cannot build is a legitimate state,
# and the two tests below are how it stays honest — a raise here has to name
# something the reader can act on, not just fail.
CANNOT_BUILD: dict[str, dict] = {}


@pytest.fixture
def svc(tmp_path):
    return DltRunnerService(pipelines_dir=str(tmp_path))


def _build(svc, conn_type, dlt_config=None):
    return svc.build_source(conn_type, SAAS_PROBE_CONFIG[conn_type], dlt_config or {})


#: A public address the egress guard accepts, so the guard's own classification runs to
#: completion rather than being short-circuited. `93.184.216.34` is example.com's, and is
#: used here for the same reason tests/test_security/test_egress_guard.py uses it.
_PUBLIC_IP = "93.184.216.34"


def _gai(*ips: str):
    """A ``socket.getaddrinfo``-shaped return value. The guard reads only ``[4][0]``."""
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 0)) for ip in ips]


@pytest.fixture(autouse=True)
def no_live_dns(monkeypatch):
    """Building a source RESOLVES THE HOST, so this module did live DNS (core#1280).

    `build_source` -> `_build_saas_source` -> `_rest_api_fallback` -> `_rest_api_from_parts`
    -> `validate_egress_host` -> `socket.getaddrinfo`. Nothing on that path is mocked, so a
    file whose own docstring said *"no request is made; the source is only constructed"* was
    performing a real lookup for every SaaS host it knows.

    Measured: with DNS unavailable this module is **46 failed, 6 passed**. It failed for
    real at 69% of a pre-push on one transient resolver blip, and the message it printed was
    about connector endpoints rather than about DNS -- so the reader starts by looking for a
    connector defect that is not there.

    🚨 **The NETWORK is replaced, not the GUARD.** `validate_egress_host` still runs and
    still classifies; `test_the_stub_replaced_the_network_not_the_guard` is what stops this
    fixture from quietly becoming an SSRF bypass for the whole module.
    """
    seen: list[str] = []

    def _fake_getaddrinfo(host, *args, **kwargs):
        seen.append(host)
        return _gai(_PUBLIC_IP)

    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo)

    # ASSERT THE STUB IS IN EFFECT, behaviourally, through the real call path.
    #
    # A fixture that silently fails to patch hands back exactly today's behaviour, and the
    # module goes green for the wrong reason -- which is the defect this file is fixing, one
    # level up. `.invalid` is reserved by RFC 2606 and never resolves, so if the stub were
    # not in effect this call would raise EgressValidationError and the fixture would fail
    # loudly instead of the tests passing by luck.
    egress_guard.validate_egress_host("https://dns-stub-probe.invalid/x")
    assert seen == ["dns-stub-probe.invalid"], (
        "the DNS stub is not in effect on the path that resolves hosts "
        f"(recorded {seen!r}). These tests would fall back to live DNS and pass for the "
        "wrong reason. If `egress_guard` switched to `from socket import getaddrinfo`, "
        "patching `socket.getaddrinfo` no longer reaches it."
    )
    seen.clear()
    return seen


def test_the_stub_replaced_the_network_not_the_guard(monkeypatch):
    """`no_live_dns` must not become an SSRF bypass for this module.

    The tempting simplification is to patch `validate_egress_host` itself. That would keep
    every test here green while switching the control off for the whole file, and nothing
    would say so -- a guard is inert if anything upstream of it already answers
    (`ENGINEERING_RULES` §57, and core#896 before it).

    Patching the RESOLVER instead keeps the guard's classification running. Hand it a
    private address and it must still refuse.
    """
    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: _gai("10.0.0.5"))
    with pytest.raises(EgressValidationError):
        egress_guard.validate_egress_host("https://internal.example.test/x")


def test_the_stub_is_not_hiding_a_source_that_stopped_resolving_at_all(no_live_dns, svc):
    """Anti-vacuity for `no_live_dns`: the stub must actually be on the hot path.

    If `build_source` stopped resolving hosts, the fixture would still pass its own
    in-effect probe (which calls the guard directly) while protecting nothing. This asserts
    that building a real source goes through the stubbed resolver.
    """
    _build(svc, "freshdesk")
    assert no_live_dns, (
        "building a source resolved no host, so `no_live_dns` is protecting nothing here. "
        "Either the egress guard left the build path -- in which case this module no "
        "longer needs the fixture -- or the stub is not reaching it."
    )


class TestOfferedEndpointsExist:
    """Bind the picker's options to the resources that actually get built."""

    @pytest.mark.parametrize("conn_type", sorted(set(SAAS_DEFAULT_ENDPOINTS) - set(CANNOT_BUILD)))
    def test_every_offered_endpoint_exists(self, svc, conn_type):
        built = set(_build(svc, conn_type).resources.keys())
        offered = set(SAAS_DEFAULT_ENDPOINTS[conn_type])
        missing = offered - built
        assert not missing, (
            f"{conn_type}: the picker offers {sorted(missing)}, which the loader does not "
            f"build. It builds {sorted(built)}. Selecting a name that does not exist makes "
            "the run fail, so the two lists must agree."
        )

    @pytest.mark.parametrize("conn_type", sorted(set(SAAS_DEFAULT_ENDPOINTS) - set(CANNOT_BUILD)))
    def test_no_built_resource_is_hidden_from_the_picker(self, svc, conn_type):
        """The other direction: a resource you cannot untick is a resource you cannot avoid."""
        built = set(_build(svc, conn_type).resources.keys())
        unlisted = built - set(SAAS_DEFAULT_ENDPOINTS[conn_type])
        assert not unlisted, f"{conn_type}: {sorted(unlisted)} load but are not offered"

    @pytest.mark.parametrize("conn_type", sorted(CANNOT_BUILD))
    def test_the_unbuildable_ones_still_fail_loudly(self, svc, conn_type):
        """Documents core#543 rather than hiding it — move these up when it's fixed.

        Asserted with *valid* config so the failure is the missing capability,
        not a missing field: these two raise for every user, always.
        """
        with pytest.raises(DltRunnerError, match="not available on this deployment"):
            svc.build_source(conn_type, CANNOT_BUILD[conn_type], {})

    @pytest.mark.parametrize("conn_type", sorted(CANNOT_BUILD))
    def test_the_failure_names_something_the_reader_can_act_on(self, svc, conn_type):
        """The error used to say "run `dlt init …`" — on a server they don't operate.

        Same class as the s3 message in core#499: advice the reader cannot
        follow. Here it was also the *only* outcome, every time. The replacement
        has to name the real blocker and not send anyone to a terminal they
        have no access to.
        """
        with pytest.raises(DltRunnerError) as exc:
            svc.build_source(conn_type, CANNOT_BUILD[conn_type], {})

        message = str(exc.value)
        assert "dlt init" not in message, f"still tells the user to run dlt init: {message}"
        assert "core#543" in message, "the message should point at the tracking issue"


class TestSelectionNarrowsTheLoad:
    def test_selected_endpoints_are_the_only_ones_loaded(self, svc):
        source = _build(svc, "github", {"endpoints": ["issues", "commits"]})
        assert sorted(source.selected_resources.keys()) == ["commits", "issues"]

    def test_all_resources_load_when_nothing_is_selected(self, svc):
        source = _build(svc, "github")
        assert sorted(source.selected_resources.keys()) == [
            "commits",
            "issues",
            "pulls",
            "stargazers",
        ]

    def test_an_empty_selection_is_treated_as_no_selection(self, svc):
        """The form only writes the key when something is ticked, so [] means "unset".

        Worth pinning: dlt's own `with_resources()` with no arguments selects
        *everything*, so an empty list must not be forwarded blindly.
        """
        source = _build(svc, "github", {"endpoints": []})
        assert len(source.selected_resources) == 4

    def test_an_unknown_endpoint_fails_with_a_message_naming_it(self, svc):
        with pytest.raises(DltRunnerError) as exc:
            _build(svc, "github", {"endpoints": ["issues", "nonsuch"]})
        assert "nonsuch" in str(exc.value)
        assert "issues" in str(exc.value)

    @pytest.mark.parametrize("conn_type", sorted(set(SAAS_DEFAULT_ENDPOINTS) - set(CANNOT_BUILD)))
    def test_selection_works_for_every_connector_that_offers_it(self, svc, conn_type):
        """Not just github — the picker is rendered for all of these."""
        first = sorted(SAAS_DEFAULT_ENDPOINTS[conn_type])[0]
        source = _build(svc, conn_type, {"endpoints": [first]})
        assert list(source.selected_resources.keys()) == [first]

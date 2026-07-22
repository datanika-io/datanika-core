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

import pytest

from datanika.services.dlt_runner import DltRunnerError, DltRunnerService
from datanika.ui.state.connection_state import SAAS_DEFAULT_ENDPOINTS

# Minimal config to get each SaaS branch past its required-field checks. Values
# are placeholders — no request is made; the source is only constructed.
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

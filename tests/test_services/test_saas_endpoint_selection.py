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
}

# These three raise instead of building: no verified source is installed and they
# have no REST fallback (core#543). Their picker entries cannot be validated
# until that is resolved, and their accuracy is moot while the connector cannot
# run at all. Config here is *valid* — the point is that the raise happens after
# the required-field checks, not because of them.
CANNOT_BUILD = {
    "google_analytics": {"property_id": "1", "service_account_json": "{}"},
    "google_ads": {"customer_id": "1", "service_account_json": "{}"},
    "facebook_ads": {"access_token": "t", "account_id": "act_1"},
}


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

        Asserted with *valid* config so the failure is the missing verified
        source, not a missing field: these three raise for every user, always.
        """
        with pytest.raises(DltRunnerError, match="verified source not installed"):
            svc.build_source(conn_type, CANNOT_BUILD[conn_type], {})


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

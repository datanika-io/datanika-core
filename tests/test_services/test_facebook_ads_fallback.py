"""Facebook Ads loads via the Graph API fallback (core#543).

Three connectors could only ever fail: `google_analytics`, `google_ads` and
`facebook_ads` had no REST fallback, so their `except ImportError` re-raised
*"run `dlt init <source>`"* — an instruction for a server the user does not
operate, and the sole outcome of every run. All three are advertised on the
marketing site with setup guides walking through credential creation for a load
that cannot succeed.

They are not equivalent, which is why only one is fixed here:

* **facebook_ads** — the Graph API takes exactly the two fields the connection
  form already collects (`access_token`, `account_id`). A fallback is
  constructible today, and it is what this file covers.
* **google_analytics** — `runReport` takes a POST body naming dimensions and
  metrics. A report shape is a product decision, not a default.
* **google_ads** — every request carries a `developer-token` header and the form
  collects no such field, so nothing we store can authenticate. Needs a schema
  change and an application to Google before any transport can work.

**Verified against the documented Marketing API edges, not a live account** —
there are no Facebook credentials here or in CI. This replaces a guaranteed
failure with a plausible one; the first real run is the actual test. The parts
asserted below are the parts that do not depend on Facebook: which resources
exist, what paths they address, and that the account id is normalised.
"""

from unittest.mock import patch

import pytest

from datanika.services.dlt_runner import DEFAULT_FACEBOOK_API_VERSION, DltRunnerService


@pytest.fixture
def svc(tmp_path):
    return DltRunnerService(pipelines_dir=str(tmp_path))


def _built_config(svc, config, dlt_config=None):
    """The config handed to `rest_api_source` — base URL, auth and paths.

    A built `DltResource` does not expose its request path: `_hints` is only
    ``{'columns': {}, 'write_disposition': 'append'}``. An earlier version of
    this file asserted `"act_act_" not in repr(resource._hints)`, which passed
    over a string that never contained a path at all — a check that could not
    fail. Intercepting the call observes the thing we actually construct.
    """
    with patch("datanika.services.dlt_runner.rest_api_source") as mock_source:
        svc.build_source("facebook_ads", config, dlt_config or {})
    assert mock_source.call_count == 1, "expected exactly one rest_api_source call"
    return mock_source.call_args[0][0]


def _paths(built) -> dict[str, str]:
    return {r["name"]: r["endpoint"]["path"] for r in built["resources"]}


class TestItBuildsAtAll:
    def test_a_source_is_returned_instead_of_raising(self, svc):
        """The whole of core#543 for this connector: it used to be unreachable."""
        source = svc.build_source("facebook_ads", {"access_token": "t", "account_id": "123"}, {})
        assert source is not None

    def test_it_builds_the_documented_ad_account_edges(self, svc):
        source = svc.build_source("facebook_ads", {"access_token": "t", "account_id": "123"}, {})
        assert set(source.resources.keys()) == {"campaigns", "ad_sets", "ads", "creatives"}

    def test_leads_is_not_offered(self, svc):
        """`leads` was in the picker and is not an ad-account edge.

        Lead records hang off a lead-gen form, not the account, so offering it
        would tick a box for something the loader cannot fetch — core#532 in
        miniature, and the reason that entry was dropped rather than mapped to
        a guessed path.
        """
        source = svc.build_source("facebook_ads", {"access_token": "t", "account_id": "123"}, {})
        assert "leads" not in source.resources


class TestAccountIdNormalisation:
    """`act_` is part of the identifier and users paste it both ways."""

    @pytest.mark.parametrize("account_id", ["123456789", "act_123456789"])
    def test_either_form_is_accepted(self, svc, account_id):
        source = svc.build_source(
            "facebook_ads", {"access_token": "t", "account_id": account_id}, {}
        )
        assert set(source.resources.keys()) == {"campaigns", "ad_sets", "ads", "creatives"}

    def test_the_prefix_is_not_doubled(self, svc):
        """`act_act_123` would 404 on every request — a connection that looks
        fine and returns nothing."""
        built = _built_config(svc, {"access_token": "t", "account_id": "act_123"})
        paths = _paths(built)

        assert paths["campaigns"] == "act_123/campaigns"
        assert not any("act_act_" in p for p in paths.values()), paths

    def test_a_bare_id_gets_the_prefix(self, svc):
        built = _built_config(svc, {"access_token": "t", "account_id": "123"})
        assert _paths(built)["campaigns"] == "act_123/campaigns"

    def test_every_resource_addresses_the_account(self, svc):
        built = _built_config(svc, {"access_token": "t", "account_id": "123"})
        for name, path in _paths(built).items():
            assert path.startswith("act_123/"), f"{name} addresses {path!r}, not the ad account"


class TestApiVersionIsConfigurable:
    def test_it_defaults_to_the_pinned_version(self, svc):
        built = _built_config(svc, {"access_token": "t", "account_id": "123"})
        assert built["client"]["base_url"] == (
            f"https://graph.facebook.com/{DEFAULT_FACEBOOK_API_VERSION}/"
        )

    def test_the_version_can_be_overridden_without_a_release(self, svc):
        """Facebook retires versions on a rolling schedule.

        When the default expires, that must be a setting to change rather than
        a release to cut — otherwise every Facebook user stays broken until a
        deploy lands.
        """
        built = _built_config(
            svc, {"access_token": "t", "account_id": "123"}, {"api_version": "v99.0"}
        )
        assert built["client"]["base_url"] == "https://graph.facebook.com/v99.0/"

    def test_the_token_is_sent_as_a_bearer(self, svc):
        built = _built_config(svc, {"access_token": "secret-token", "account_id": "123"})
        assert built["client"]["auth"] == {"type": "bearer", "token": "secret-token"}


class TestSelectionApplies:
    def test_the_endpoint_picker_narrows_the_load(self, svc):
        """core#532's selection must work here too, not just for the older ten."""
        source = svc.build_source(
            "facebook_ads",
            {"access_token": "t", "account_id": "123"},
            {"endpoints": ["campaigns"]},
        )
        assert set(source.selected_resources.keys()) == {"campaigns"}

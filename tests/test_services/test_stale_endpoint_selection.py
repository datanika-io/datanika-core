"""A stale endpoint selection must not fail the run (core#532 follow-up).

core#532 made the endpoint picker real: the names it offers are now the names
the loader builds, and a selection narrows the load. It raises on a name that
does not exist, which is right — a picker list that has drifted from the
builders is exactly how core#532 happened, and silence there would recreate it.

But it has a blast radius that only shows up on *existing* data. Before the fix
the picker wrote `dlt_config["endpoints"]` and **no builder read it**, and it
defaulted to *every* endpoint ticked. So every SaaS upload saved before that
change carries a full list of names that no longer resolve — `Subscription`,
`Account`, `Coupon` against a Stripe loader that builds `subscriptions`,
`charges`, `customers`. There is no migration: the values live inside the
`uploads.dlt_config` JSON blob.

Raising on those turns "loads more than you asked for" into "fails every run",
for uploads whose owner never opened the picker.

The rule this file pins:

* **nothing resolves** → the selection is residue, not a choice. Ignore it,
  load everything (what actually happened before), and log loudly.
* **something resolves** → somebody chose. A name that has since vanished is
  worth stopping for, because that is drift and drift is the original bug.
"""

import logging

import pytest

from datanika.services.dlt_runner import DltRunnerError, DltRunnerService

#: What the picker offered for Stripe before core#532 — taken from the dlt
#: verified source, which is not installed anywhere, so none of these are built.
LEGACY_STRIPE_SELECTION = [
    "Subscription",
    "Account",
    "Coupon",
    "Customer",
    "Invoice",
    "Product",
    "Price",
]

STRIPE_CONFIG = {"api_key": "sk_test_1"}


@pytest.fixture
def svc(tmp_path):
    return DltRunnerService(pipelines_dir=str(tmp_path))


def _build(svc, dlt_config):
    return svc.build_source("stripe", STRIPE_CONFIG, dlt_config)


class TestAStaleSelectionIsTreatedAsUnset:
    def test_a_pre_532_upload_still_runs(self, svc):
        """The regression this exists to prevent: it used to raise here."""
        source = _build(svc, {"endpoints": LEGACY_STRIPE_SELECTION})

        assert source is not None

    def test_it_loads_everything_rather_than_nothing(self, svc):
        """Not just "does not raise" — it must load what it loaded before.

        Selecting nothing would be the quieter failure and the worse one: a
        green run with an empty table, which is the whole family of bugs
        core#492/#493 were about.
        """
        source = _build(svc, {"endpoints": LEGACY_STRIPE_SELECTION})

        assert set(source.selected_resources.keys()) == set(source.resources.keys())
        assert source.selected_resources, "a stale selection emptied the load"

    def test_it_says_so(self, svc, caplog):
        """Silently ignoring input is how the original bug felt to the user."""
        with caplog.at_level(logging.WARNING):
            _build(svc, {"endpoints": LEGACY_STRIPE_SELECTION})

        assert any("stale endpoint selection" in r.message for r in caplog.records), (
            f"no warning logged; got {[r.message for r in caplog.records]}"
        )


class TestARealSelectionStillBinds:
    def test_a_valid_subset_still_narrows(self, svc):
        source = _build(svc, {"endpoints": ["customers", "charges"]})

        assert set(source.selected_resources.keys()) == {"customers", "charges"}

    def test_a_partial_match_still_raises(self, svc):
        """Somebody chose, and one of their choices vanished — that is drift.

        Loading everything here would silently widen a deliberate selection;
        loading only the survivors would silently narrow it. Both are wrong in
        the way core#532 was wrong, so it stops.
        """
        with pytest.raises(DltRunnerError, match="Unknown endpoint"):
            _build(svc, {"endpoints": ["customers", "Coupon"]})

    def test_the_partial_error_names_both_sides(self, svc):
        with pytest.raises(DltRunnerError) as exc:
            _build(svc, {"endpoints": ["customers", "Coupon"]})

        message = str(exc.value)
        assert "Coupon" in message, "should name what was not found"
        assert "customers" in message, "should name what is available"

    def test_an_absent_selection_is_untouched(self, svc):
        source = _build(svc, {})

        assert set(source.selected_resources.keys()) == set(source.resources.keys())

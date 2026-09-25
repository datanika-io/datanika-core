"""A default resource list is an assertion about a third-party API (core#1574 AC4).

Asana's `tasks` shipped as `{"name": "tasks", "endpoint": {"path": "tasks"}}` for the life of the
connector, and `GET /tasks` is **invalid** without a scope:

    GET /tasks                              -> 400  "You should specify one of workspace,
                                                     project, tag, section, user_task_list"
    GET /tasks?project=<a gid that is not mine>  -> 403  "You do not have access to this project."

That pair is the discriminator this module is built on. **400 means Asana rejected the request for
having no scope; 403 means the scope was ACCEPTED and only authorization failed.** So "does our
request carry a scope this vendor accepts?" is answerable without a credential and without a live
call, *provided* the rule itself came from a measurement.

⚠️ **The honest limit, stated so a green run is not over-read.** Nothing here contacts a vendor. The
rules below are **recorded measurements**, and a connector with no recorded measurement is listed in
:data:`NOT_MEASURED` rather than passing silently — an instrument built from part of its population
reports the rest as clean, which is how `tasks` survived: it is one resource of five and the other
four return 200, so a load either fails naming one table or **completes with four tables**, and four
tables arriving is not obviously wrong.

Why the whole default list matters and not just the endpoint somebody happened to look at: a
resource list is written once, by hand, from a vendor's documentation, and then never re-read.
Every entry is a claim that the vendor accepts that request. This module is where those claims live.
"""

from __future__ import annotations

import pytest

from datanika.services.dlt_runner import (
    REST_FALLBACK_SAAS_TYPES,
    SAAS_DEFAULT_RESOURCE_BUILDERS,
    asana_default_resources,
)

#: (connector, resource) -> the query parameters the vendor requires on that endpoint, and the
#: measurement that established it. **At least one of the named params must be present**, because
#: several vendors accept any one of a set of scopes.
#:
#: Each entry is a reading taken against the live API, dated, with a control in both directions.
#: Do not add an entry from documentation alone: Asana's docs name `workspace` as a `/tasks` filter
#: and `workspace` ALONE still 400s ("Must specify exactly one of project, tag, section, user task
#: list, or assignee + workspace"), which is precisely the shape a doc-derived rule would have
#: blessed.
MEASURED_REQUIRED_PARAMS: dict[tuple[str, str], tuple[frozenset[str], str]] = {
    ("asana", "tasks"): (
        frozenset({"project", "tag", "section", "user_task_list", "assignee"}),
        "Measured 2026-09-25 against app.asana.com with secrets/asana.env. `GET /tasks` -> 400 "
        "naming these scopes; `GET /tasks?project=1` -> 403 (scope accepted, gid not ours); "
        "`GET /tasks?workspace=<gid>` -> 400 (workspace alone is NOT a scope); "
        "`GET /tasks?workspace=<gid>&assignee=me` -> 200. Controls: no auth header -> 401, "
        "garbage bearer -> 401.",
    ),
}

#: Connectors whose default resource lists have **not** been measured against their vendor. Listed
#: rather than omitted: a connector absent from both this set and the rules above would be
#: indistinguishable from one that was checked and found correct.
#:
#: 🔑 Two-way, like every ratchet in this suite. Measuring one of these means moving it out; a
#: connector added later and left out of both sets fails
#: :meth:`test_every_saas_connector_is_accounted_for`.
#:
#: ⚠️ **13 of 14 is the honest state and this module does not pretend otherwise.** Measuring a
#: vendor needs a working credential for it, and 2 of the 3 Wave-1 credentials on disk are dead
#: (`plans/SECRETS_INVENTORY.md`). What the ledger buys today is that the *population* is named, so
#: the next person to hold a live credential knows exactly which row they can fill in — and a
#: connector added next year cannot join this set silently.
NOT_MEASURED = frozenset(
    {
        "stripe",
        "github",
        "hubspot",
        "salesforce",
        "shopify",
        "jira",
        "slack",
        "zendesk",
        "airtable",
        "notion",
        "pipedrive",
        "freshdesk",
        "facebook_ads",
    }
)


def _params_of(resource: dict) -> dict:
    endpoint = resource.get("endpoint")
    if isinstance(endpoint, str):
        return {}
    return dict((endpoint or {}).get("params") or {})


class TestTheAsanaRuleIsTheOneThatWasMeasured:
    """AC4's rule set is only as good as its first entry, so that entry is asserted directly."""

    def test_the_default_task_resource_carries_a_scope_asana_accepts(self):
        resources = {r["name"]: r for r in asana_default_resources()}
        required, _why = MEASURED_REQUIRED_PARAMS[("asana", "tasks")]
        present = set(_params_of(resources["tasks"])) & required
        assert present, (
            "the default `tasks` resource carries none of the scopes Asana accepts "
            f"({sorted(required)}), so GET /tasks is a 400 and the table the connector exists for "
            "cannot load: " + repr(resources["tasks"])
        )

    def test_a_workspace_alone_would_not_satisfy_the_rule(self):
        """The negative control, and it is the whole reason this rule is a measurement.

        `workspace` is the field the issue asked for and the obvious thing to add. Asana refuses it
        on `/tasks` unless `assignee` comes with it, so a rule that accepted `workspace` would pass
        a request that 400s.
        """
        required, _why = MEASURED_REQUIRED_PARAMS[("asana", "tasks")]
        assert "workspace" not in required, (
            "`workspace` has been added to the accepted scopes for asana/tasks. Measured "
            "2026-09-25: GET /tasks?workspace=<gid> returns 400. If that has changed, replace the "
            "measurement in MEASURED_REQUIRED_PARAMS with the new one and date it."
        )
        assert not ({"workspace"} & required)

    def test_the_predicate_flags_a_resource_list_with_no_scope(self):
        """Anti-vacuity: the pre-fix list must fail the check the post-fix list passes."""
        pre_fix = {"name": "tasks", "endpoint": {"path": "tasks"}}
        required, _why = MEASURED_REQUIRED_PARAMS[("asana", "tasks")]
        assert not (set(_params_of(pre_fix)) & required), (
            "the predicate cannot tell the shipped-and-broken resource from the fixed one, so its "
            "verdict on the fixed one says nothing"
        )


class TestEveryMeasuredRuleHolds:
    @pytest.mark.parametrize(
        "connector, resource_name",
        sorted(MEASURED_REQUIRED_PARAMS),
        ids=lambda v: v if isinstance(v, str) else str(v),
    )
    def test_the_built_resource_satisfies_its_vendors_rule(self, connector, resource_name):
        assert connector in SAAS_DEFAULT_RESOURCE_BUILDERS, (
            f"{connector} has a measured rule but its default resource list is still inline in "
            "`_build_saas_source`, so this test cannot read it. Extract it to a module-level "
            "builder and register it — that extraction is what makes a measurement checkable."
        )
        builder = SAAS_DEFAULT_RESOURCE_BUILDERS[connector]
        resources = {r["name"]: r for r in builder()}
        assert resource_name in resources, (
            f"{connector} no longer builds a `{resource_name}` resource; if it was removed "
            "deliberately, remove its rule too rather than leaving a rule with no subject"
        )
        required, why = MEASURED_REQUIRED_PARAMS[(connector, resource_name)]
        present = set(_params_of(resources[resource_name])) & required
        assert present, f"{connector}/{resource_name} carries none of {sorted(required)}.\n  {why}"


class TestTheLedgerCoversItsPopulation:
    def test_every_saas_connector_is_accounted_for(self):
        """Every connector with a hand-written resource list is measured or explicitly not measured.

        The population is ``REST_FALLBACK_SAAS_TYPES`` — the set that reaches
        ``_rest_api_fallback``, i.e. exactly the connectors whose default resources are a
        hand-written list of vendor endpoints. ``google_analytics`` and ``google_ads`` are
        deliberately outside it: their sources are built by hand (`@dlt.resource` functions), so
        there is no resource list to be wrong about in this way.

        A connector in neither set is the failure this module exists to prevent: it would be
        reported as clean by a check that never looked at it.
        """
        measured = {c for c, _ in MEASURED_REQUIRED_PARAMS}
        accounted = measured | NOT_MEASURED
        assert set(REST_FALLBACK_SAAS_TYPES) == accounted, (
            "connectors in neither MEASURED_REQUIRED_PARAMS nor NOT_MEASURED:\n"
            f"  unaccounted: {sorted(set(REST_FALLBACK_SAAS_TYPES) - accounted)}\n"
            f"  named here but not a REST-fallback type: "
            f"{sorted(accounted - set(REST_FALLBACK_SAAS_TYPES))}\n"
            "Measure it, or list it as unmeasured. Silence is the one option this module removes."
        )

    def test_the_unmeasured_set_does_not_outlive_the_measurements(self):
        """Two-way: a connector cannot be both measured and listed as unmeasured."""
        measured = {c for c, _ in MEASURED_REQUIRED_PARAMS}
        assert not (measured & NOT_MEASURED), (
            f"{sorted(measured & NOT_MEASURED)} appear in both sets — remove them from "
            "NOT_MEASURED now that a measurement exists"
        )

    def test_the_population_is_not_empty(self):
        assert len(REST_FALLBACK_SAAS_TYPES) >= 14, (
            f"only {len(REST_FALLBACK_SAAS_TYPES)} REST-fallback types found; the import has "
            "stopped resolving and every set comparison above is between two nearly-empty things"
        )
        assert MEASURED_REQUIRED_PARAMS, "no rule is recorded, so nothing above grades anything"

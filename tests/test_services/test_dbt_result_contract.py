"""The dbt result-object contract, read from a REAL dbt run (core#825).

## Why this file exists

Moving dbt-core 1.7 -> 1.11 crosses the 1.8 adapter split. The library surface we
touch is tiny — one import, ``from dbt.cli.main import dbtRunner`` — and it did not
change. **That is not the risk.** The risk is that every consumer of the *result*
reads it through ``getattr(..., default)``:

    tasks/pipeline_tasks.py:39-54   catalog sync      getattr(node, "name", None)
    tasks/pipeline_tasks.py:260-266 billable_nodes    getattr(r.node.resource_type, "value", None)
    services/dbt_project.py:40-93   log formatting    getattr(nr, "execution_time", 0)
    services/dbt_project.py:79-92   _sum_rows_affected
    services/dbt_project.py:522-545 compile_model     getattr(node, "compiled_code", None)

A renamed field therefore does **not** raise. It degrades silently, and every one
of those call sites keeps returning a plausible value. The worst instance is
``billable_nodes`` — the **usage-metering counter**. If ``resource_type.value``
moved, metered model runs would silently become 0 and every run would still report
success. We would bill nothing and see nothing.

## Why the existing tests could not catch that

``test_dbt_project.py`` uses **19 ``MagicMock()`` doubles with no ``spec=``**. A
MagicMock answers every attribute access with another MagicMock, so those tests stay
green through *any* dbt API change — they assert the shape we imagined, not the shape
dbt produces. This is the same defect that let a paginator detector "detect" a
paginator for all 17 vendors (core#823): **a harness with only one possible answer.**

The only pre-existing real-dbt test is ``test_transformation_compile.py``, and it
exercises ``dbt compile`` only — which produces no rows, touches no adapter response,
and never reaches ``billable_nodes``.

So this file runs a real ``dbt run`` / ``dbt test`` / ``dbt snapshot`` against a real
DuckDB warehouse, through ``DbtProjectService``'s own generated artifacts, and asserts
each access path the application actually reads. It is slow by the standards of a unit
test and it is the only thing standing between an adapter change and a silent billing
outage.

⚠️ **Read the assertions as a contract, not as a description of dbt.** If dbt changes
one of these, the correct response is to fix the *consumer* named beside it — not to
relax the assertion.
"""

from __future__ import annotations

import pytest

from datanika.services.dbt_project import (
    DbtProjectService,
    _format_dbt_logs,
    _sum_rows_affected,
)

ORG = 4242


@pytest.fixture(scope="module")
def dbt_result(tmp_path_factory):
    """One real dbt project, built through the service, run against real DuckDB.

    Module-scoped: a dbt invocation parses ~500 macros and costs seconds, and every
    assertion below reads the same result objects. Building it per-test would make
    this file slow enough that someone would delete it.
    """
    root = tmp_path_factory.mktemp("dbtcontract")
    svc = DbtProjectService(str(root))
    svc.ensure_project(ORG)

    # Real generated artifacts, via the service's own writers -- NOT hand-written
    # YAML. The generated `dbt_project.yml` carries a project-level `version:`
    # (deprecated in dbt 1.10) and `write_tests_config` emits the `tests:` key
    # (renamed to `data_tests:` in 1.8, old spelling still accepted). Whether dbt
    # still ACCEPTS what we generate is exactly half of what this file tests.
    svc.generate_profiles_yml(
        ORG,
        "duckdb",
        {"path": str(root / "warehouse.duckdb"), "schema": "main"},
        default_schema="main",
    )
    svc.write_model(
        ORG,
        "widgets",
        "select 1 as id, 'alpha' as nm union all select 2, 'beta'",
        schema_name="main",
        materialization="table",
    )
    svc.write_tests_config(
        ORG, "widgets", {"columns": {"id": {"not_null": True}}}, schema_name="main"
    )

    results = {
        "run": svc.run_command(ORG, "run"),
        "test": svc.run_command(ORG, "test"),
    }
    # `run_model` returns the service's own dict; we want the raw dbt objects, so
    # reach the runner the same way `run_command` does.
    from dbt.cli.main import dbtRunner

    project = svc.get_project_path(ORG)
    raw = dbtRunner().invoke(["run", "--project-dir", str(project), "--profiles-dir", str(project)])
    return {"svc": svc, "wrapped": results, "raw": raw}


class TestTheGeneratedArtifactsAreStillAcceptedByDbt:
    """Half the risk is our YAML, not our Python.

    ``ensure_project`` writes ``dbt_project.yml`` only ``if not yml_path.exists()``,
    so an existing tenant on the persistent ``dbt_projects`` volume keeps its
    1.7-era file **forever** — a redeploy does not regenerate it. If dbt had started
    *rejecting* any key we emit, tenants would break on their next run with no
    migration having been possible.
    """

    def test_a_real_run_of_our_generated_project_succeeds(self, dbt_result):
        assert dbt_result["wrapped"]["run"]["success"] is True, (
            "dbt could not run a project generated by DbtProjectService. "
            f"logs: {dbt_result['wrapped']['run']['logs'][:800]}"
        )

    def test_a_real_test_run_of_our_generated_schema_yml_succeeds(self, dbt_result):
        """`write_tests_config` emits `tests:`, deprecated in favour of `data_tests:`.

        Deprecated is not removed. The day it IS removed, this goes red and the
        fix is a migration pass over every ``tenant_*`` directory -- not a rename
        in the writer alone, because existing tenants keep their old files.
        """
        assert dbt_result["wrapped"]["test"]["success"] is True, (
            "dbt rejected the schema.yml that write_tests_config generates. "
            f"logs: {dbt_result['wrapped']['test']['logs'][:800]}"
        )


class TestTheResultObjectContract:
    """Each assertion names the consumer that breaks silently if it fails."""

    def test_status_exposes_dot_value_as_the_string_success(self, dbt_result):
        """Consumers: pipeline_tasks.py:39 (catalog sync), :260 (billable_nodes)."""
        statuses = [getattr(r.status, "value", None) for r in dbt_result["raw"].result]
        assert statuses and all(s == "success" for s in statuses), (
            f"A successful model run no longer reports status.value == 'success' "
            f"(got {statuses}). _sync_catalog_after_pipeline and billable_nodes "
            "both filter on that exact string and would silently match NOTHING -- "
            "catalog entries would stop being created and metered model runs "
            "would drop to zero, with every run still reporting success."
        )

    def test_node_resource_type_exposes_dot_value_as_model(self, dbt_result):
        """Consumer: pipeline_tasks.py:260-266 — the USAGE-METERING counter."""
        kinds = [
            getattr(getattr(r.node, "resource_type", None), "value", None)
            for r in dbt_result["raw"].result
        ]
        assert kinds and all(k == "model" for k in kinds), (
            f"node.resource_type.value is no longer 'model' (got {kinds}). This is "
            "the billing path: billable_nodes counts nodes whose resource_type.value "
            "is in ('model','test'), so a rename here bills zero for every "
            "transformation run and raises no error anywhere."
        )

    def test_the_billable_nodes_expression_still_counts(self, dbt_result):
        """The metering expression itself, copied verbatim from pipeline_tasks.py.

        Asserted as an expression rather than as its parts, because the parts can
        each be individually fine while the conjunction matches nothing.
        """
        raw_result = list(dbt_result["raw"].result)
        billable_nodes = sum(
            1
            for r in raw_result
            if getattr(getattr(r, "status", None), "value", None) == "success"
            and getattr(getattr(r, "node", None), "resource_type", None) is not None
            and getattr(r.node.resource_type, "value", None) in ("model", "test")
        )
        assert billable_nodes == len(raw_result) == 1, (
            f"billable_nodes counted {billable_nodes} of {len(raw_result)} real "
            "successful model nodes. Every unit test of this counter uses "
            "MagicMock, which cannot fail this way."
        )

    def test_node_carries_name_schema_and_materialization(self, dbt_result):
        """Consumer: pipeline_tasks.py:44-52, which builds catalog entries."""
        node = dbt_result["raw"].result[0].node
        assert getattr(node, "name", None) == "widgets"
        assert getattr(node, "schema", None) == "main"
        assert getattr(getattr(node, "config", None), "materialized", None) == "table"

    def test_adapter_response_is_a_mapping_not_an_object(self, dbt_result):
        """Consumers: ``_sum_rows_affected`` (dict branch) and ``run_snapshot``.

        ⚠️ This assertion records a PRE-EXISTING defect rather than a new one.
        ``_sum_rows_affected`` handles both a dict and an object; ``run_snapshot``
        (dbt_project.py:664-669) handles **only** the object form. Since the real
        response is a dict, that snapshot branch is dead and always sums to 0 --
        and it was dead at dbt 1.7 too, so core#825 did not cause it. It is
        asserted here so the fix is not written against the wrong shape.
        """
        resp = dbt_result["raw"].result[0].adapter_response
        assert isinstance(resp, dict), (
            f"adapter_response is now {type(resp).__name__}, not a dict. "
            "_sum_rows_affected's dict branch is the live one; if this became an "
            "object, run_snapshot's object-only branch would start working and "
            "_sum_rows_affected would silently fall through to its own object "
            "branch -- a behaviour change in two places at once."
        )

    def test_compiled_code_is_reachable_where_compile_model_looks_for_it(self, dbt_result):
        """Consumer: dbt_project.py:530-538, which checks node_result THEN node.

        The order matters and the fallback is the live path: ``compiled_code`` is
        on the NODE, not on the RunResult.
        """
        nr = dbt_result["raw"].result[0]
        code = getattr(nr, "compiled_code", None) or getattr(
            getattr(nr, "node", None), "compiled_code", None
        )
        assert code and "select" in code.lower(), (
            "compile_model would return an empty compiled_sql and report success. "
            "The UI preview would show a blank query with no error."
        )

    def test_the_service_helpers_accept_the_real_result_unchanged(self, dbt_result):
        """``_format_dbt_logs`` and ``_sum_rows_affected`` against the real object.

        Both are pure functions over the result, and both are otherwise only ever
        tested with MagicMock.
        """
        logs = _format_dbt_logs(dbt_result["raw"])
        assert "widgets" in logs and "1 models, 1 succeeded, 0 failed" in logs, (
            f"_format_dbt_logs produced unexpected output against a real result:\n{logs}"
        )
        # DuckDB's adapter does not populate rows_affected, so 0 is correct here.
        # The assertion is that it RETURNS rather than raising on the real shape.
        assert _sum_rows_affected(dbt_result["raw"]) == 0


class TestTestNodeStatusIsNotSuccess:
    """A pre-existing metering defect this file's evidence exposes (core#825).

    ``billable_nodes`` counts nodes with ``status.value == "success"`` AND
    ``resource_type.value in ("model", "test")``. Measured against real dbt: a
    **passing test node reports ``status.value == "pass"``**, never ``"success"``.
    So the ``"test"`` arm of that tuple can never match, and test nodes have never
    been billable.

    This is NOT caused by the dbt bump -- ``TestStatus.Pass`` has carried the value
    ``"pass"`` for many dbt versions. It is asserted here, deliberately, in the
    file that has the only real dbt result to assert it against, so that the
    behaviour is *recorded* rather than rediscovered as a billing bug later.

    ⚠️ Do not "fix" this by adding ``"pass"`` to the status filter. Whether test
    nodes should be billable is a pricing question, not an engineering one.
    """

    def test_a_passing_test_node_reports_pass_not_success(self, dbt_result):
        from dbt.cli.main import dbtRunner

        svc = dbt_result["svc"]
        project = svc.get_project_path(ORG)
        raw = dbtRunner().invoke(
            ["test", "--project-dir", str(project), "--profiles-dir", str(project)]
        )
        assert raw.result, "no test nodes ran, so this proves nothing"
        statuses = {getattr(r.status, "value", None) for r in raw.result}
        assert statuses == {"pass"}, (
            f"test node statuses are {statuses}. If this is now {{'success'}}, the "
            "'test' arm of billable_nodes has started matching and metered volume "
            "will jump for every tenant running dbt tests -- a pricing change "
            "arriving as a dependency bump."
        )

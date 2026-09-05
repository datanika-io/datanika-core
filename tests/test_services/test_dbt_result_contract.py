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
        """The **shipped** predicate, against a real dbt result.

        Asserted as the whole predicate rather than as its parts, because the parts can
        each be individually fine while the conjunction matches nothing.

        🚨 **This used to be a verbatim COPY of the expression in `pipeline_tasks.py`,
        and that made it green whatever the shipped code did** (core#864). It is the same
        defect as the `MagicMock` the message below warns about, one level out: a test
        that re-types the thing under test is asserting against itself. It now imports
        `is_billable_node` and calls it, so a change to the real counter reaches here.
        """
        from datanika.tasks.pipeline_tasks import is_billable_node

        raw_result = list(dbt_result["raw"].result)
        billable_nodes = sum(1 for r in raw_result if is_billable_node(r))
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
    """The premise behind core#864's decision, asserted against real dbt (core#825).

    🆕 **Rewritten 2026-09-05, because the code this described has changed and a
    docstring that contradicts the code is worse than none.** It used to say
    ``billable_nodes`` counts ``resource_type.value in ("model", "test")`` and that the
    ``"test"`` arm could never match. The first half is now **false**:
    ``_BILLABLE_RESOURCE_TYPES`` is ``("model",)`` — core#864, decided by Product
    (option 2: dbt test nodes are **not** metered).

    What this class still asserts is unchanged and is the *premise* that made that
    decision cheap: a **passing test node reports ``status.value == "pass"``**, never
    ``"success"``. That is why deleting the arm changed no count, no bill and no block,
    and it is why the change was a pure intent correction rather than a pricing move.

    NOT caused by any dbt bump -- ``TestStatus.Pass`` has carried ``"pass"`` for many
    dbt versions. Asserted here, in the file with the only real dbt result to assert it
    against.

    ⚠️ **What a red here means has changed too, and it is now smaller.** Before, a flip
    to ``"success"`` would have started billing test nodes silently — a pricing change
    arriving as a dependency bump. With the arm deleted that release is a non-event for
    billing, and a red here means only that Product's premise no longer holds and the
    decision is worth re-reading. Still do not "fix" it by adding ``"pass"`` to the
    status filter: whether test nodes bill is a pricing question, and the answer on
    record is no.
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
            "premise behind core#864's decision no longer holds: test nodes would be "
            "indistinguishable from models by status alone, and the only thing keeping "
            "them unmetered is _BILLABLE_RESOURCE_TYPES == ('model',). That is still "
            "correct -- re-read the decision, do not widen the tuple."
        )

    def test_a_passing_test_node_is_not_metered_whatever_its_status(self, dbt_result):
        """🔑 The property the decision actually guarantees, asserted end to end.

        The assertion above is about dbt's *status vocabulary*, which is a fact about a
        dependency. This one is about **our** meter, and it holds even if that fact
        changes -- which is the whole value of deleting the arm rather than relying on
        ``"pass"`` never becoming ``"success"``.

        ⚠️ **Stated so nobody counts it as evidence it is not: this test is green under
        BOTH tuples today and cannot currently go red.** A passing test node reports
        ``"pass"``, so the status filter rejects it before the resource-type filter is
        reached, whether or not ``"test"`` is in the tuple. It becomes discriminating
        only in the scenario it exists for. The discriminating assertion today is
        ``test_a_test_node_reporting_success_is_still_not_billable`` in
        ``tests/test_tasks/test_pipeline_tasks.py``, which constructs that scenario
        directly and goes red against the pre-decision tuple.
        """
        from dbt.cli.main import dbtRunner

        from datanika.tasks.pipeline_tasks import is_billable_node

        svc = dbt_result["svc"]
        project = svc.get_project_path(ORG)
        raw = dbtRunner().invoke(
            ["test", "--project-dir", str(project), "--profiles-dir", str(project)]
        )
        assert raw.result, "no test nodes ran, so this proves nothing"
        kinds = {getattr(getattr(r, "node", None), "resource_type", None) for r in raw.result}
        assert kinds, "no node carried a resource_type, so the assertion below is vacuous"
        billable = [r for r in raw.result if is_billable_node(r)]
        assert not billable, (
            f"{len(billable)} of {len(raw.result)} real dbt TEST nodes are being metered "
            "as model runs. core#864 decided they are not billable."
        )

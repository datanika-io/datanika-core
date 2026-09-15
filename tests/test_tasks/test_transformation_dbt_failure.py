"""A transformation run whose dbt model fails must end FAILED (core#1361).

What was measured before the fix
--------------------------------
On core ``origin/dev`` ``d79c46f``, with dbt-core 1.11.14 and a real dbt-duckdb warehouse, through
``run_transformation_task.apply()``:

- ``select 1 as id``, the control: run ``success``, Celery ``SUCCESS``.
- ``select * from table_that_does_not_exist``: run ``success``, ``error_message`` empty, Celery
  ``SUCCESS``, and ``run.transformation_completed`` announced with ``status="success"``.
- ``selec 1 as id``: the same.
- ``select * from {{ ref('model_that_does_not_exist') }}``: run ``success``, ``error_message``
  empty, Celery ``SUCCESS``.

Every failing case also wrote a catalog entry for a model dbt never built, and dbt's error was in
``logs`` each time.

The mechanism: dbt reports a failed model through ``dbtRunnerResult.success`` and does not raise.
For the two warehouse errors a probe read ``success=False`` and ``exception=None``, with one node
result whose status is ``error``. The unresolved ``ref`` fails at compile time with no node
results, and ``run_model`` puts the compilation error in ``logs``. ``run_transformation`` read none
of it, and core#1352's check in the Celery wrapper reads the row, so it could not see the failure
either.

The witness
-----------
Everything the failure should change, read after the real task has run a real dbt project: the
row's status, whether ``error_message`` carries dbt's error, what the run announced, Celery's
verdict, and whether a catalog entry was written for a model that was never built. ``logs`` are not
the witness: they held dbt's error before the fix too.

Each case asserts one dict of observations, so a red run shows every discrepancy at once rather
than stopping at the first.
"""

from __future__ import annotations

import uuid

import pytest
from cryptography.fernet import Fernet

from datanika import hooks
from datanika.config import settings
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.transformation import Materialization, Transformation
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService
from datanika.services.dbt_project import describe_dbt_failure
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.run_outcome import RunFailedError
from datanika.tasks.transformation_tasks import run_transformation_task

FAILING_MODELS = [
    pytest.param(
        "select * from table_that_does_not_exist",
        "Table with name table_that_does_not_exist does not exist",
        id="missing-relation",
    ),
    pytest.param("selec 1 as id", 'syntax error at or near "selec"', id="syntax-error"),
    pytest.param(
        "select * from {{ ref('model_that_does_not_exist') }}",
        "depends on a node named 'model_that_does_not_exist' which was not found",
        id="unresolved-ref",
    ),
]


class _NoCloseSession:
    """The task closes the session it opens; the test's must survive to the assertions."""

    def __init__(self, session):
        self._session = session

    def close(self):
        pass

    def __getattr__(self, name):
        return getattr(self._session, name)


@pytest.fixture
def announced():
    """Record what the run announces, by event and run id."""
    seen: list[tuple[str, int, str]] = []
    registered = []
    for event in ("run.transformation_completed", "run.failed"):

        def handler(_event=event, **kwargs):
            seen.append((_event, kwargs.get("run_id"), kwargs.get("status")))

        hooks.on(event, handler)
        registered.append((event, handler))

    yield lambda event, run_id: [status for e, r, status in seen if e == event and r == run_id]

    for event, handler in registered:
        hooks.off(event, handler)


@pytest.fixture
def real_transformation(db_session, tmp_path, monkeypatch):
    """A transformation whose destination is a real DuckDB file, run through the real task.

    Only the process boundaries are replaced: the task's session source points at the test
    database, and the concurrency gate is open. dbt, the generated project and the warehouse are
    real.
    """
    key = Fernet.generate_key().decode()
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt_projects"))
    monkeypatch.setattr(settings, "credential_encryption_key", key)
    monkeypatch.setattr("datanika.services.concurrency_service.acquire", lambda org_id: True)
    monkeypatch.setattr("datanika.services.concurrency_service.release", lambda org_id: None)
    monkeypatch.setattr("datanika.db.get_sync_session", lambda: _NoCloseSession(db_session))

    def make(sql_body: str):
        org = Organization(name="Acme", slug=f"dbt-outcome-{uuid.uuid4().hex[:8]}")
        db_session.add(org)
        db_session.flush()
        warehouse = Connection(
            org_id=org.id,
            name="warehouse",
            connection_type=ConnectionType.DUCKDB,
            direction=ConnectionDirection.DESTINATION,
            config_encrypted=EncryptionService(key).encrypt(
                {"path": str(tmp_path / "warehouse.duckdb"), "schema": "main"}
            ),
        )
        db_session.add(warehouse)
        db_session.flush()
        transformation = Transformation(
            org_id=org.id,
            name="model_under_test",
            sql_body=sql_body,
            materialization=Materialization.TABLE,
            schema_name="main",
            destination_connection_id=warehouse.id,
            tests_config={},
            tags=[],
        )
        db_session.add(transformation)
        db_session.flush()
        run = ExecutionService().create_run(
            db_session, org.id, NodeType.TRANSFORMATION, transformation.id
        )
        return org, run

    return make


def _observe(db_session, org, run, result, announced, dbt_error: str | None) -> dict:
    db_session.refresh(run)
    return {
        "row status": run.status.value,
        "error_message carries dbt's error": (
            None if dbt_error is None else dbt_error in (run.error_message or "")
        ),
        "run.transformation_completed announced": announced("run.transformation_completed", run.id),
        "run.failed announced": announced("run.failed", run.id),
        "celery state": result.state,
        "task raised RunFailedError": isinstance(result.result, RunFailedError),
        "catalog entries": len(CatalogService.list_entries(db_session, org.id)),
    }


@pytest.mark.parametrize(("sql_body", "dbt_error"), FAILING_MODELS)
def test_a_dbt_model_that_fails_ends_its_run_failed(
    sql_body, dbt_error, real_transformation, announced, db_session
):
    org, run = real_transformation(sql_body)

    result = run_transformation_task.apply(args=[run.id, org.id])

    observed = _observe(db_session, org, run, result, announced, dbt_error)
    assert observed == {
        "row status": "failed",
        "error_message carries dbt's error": True,
        # AC3: a failed run is not a completed one, so nothing is announced for metering.
        # Same as `run_pipeline`'s dbt-failure branch.
        "run.transformation_completed announced": [],
        "run.failed announced": ["failed"],
        # core#1352 already raises for a FAILED row; it only needed the row to be true.
        "celery state": "FAILURE",
        "task raised RunFailedError": True,
        # A model dbt never built gets no catalog entry and no model YML.
        "catalog entries": 0,
    }, (
        f"A dbt model that failed was recorded as {observed['row status']!r} (core#1361).\n"
        f"error_message={run.error_message!r}\nlogs={(run.logs or '')[:600]!r}"
    )


def test_control_a_dbt_model_that_succeeds_still_ends_success(
    real_transformation, announced, db_session
):
    """AC2. The branch every successful transformation takes is unchanged."""
    org, run = real_transformation("select 1 as id")

    result = run_transformation_task.apply(args=[run.id, org.id])

    observed = _observe(db_session, org, run, result, announced, dbt_error=None)
    assert observed == {
        "row status": "success",
        "error_message carries dbt's error": None,
        "run.transformation_completed announced": ["success"],
        "run.failed announced": [],
        "celery state": "SUCCESS",
        "task raised RunFailedError": False,
        "catalog entries": 1,
    }, f"error_message={run.error_message!r}\nlogs={(run.logs or '')[:600]!r}"
    assert run.error_message is None
    # dbt-duckdb does not populate `rows_affected`, so the honest count here is 0, and it is
    # recorded rather than left unmeasured. A non-zero count is covered with a stand-in in
    # test_transformation_tasks.py::test_completes_with_row_count.
    assert run.rows_loaded == 0


def test_a_failure_dbt_gives_no_detail_for_still_names_the_model():
    """The one shape the real cases above do not produce: no node results and no text.

    A failed run must never carry an empty ``error_message``.
    """
    message = describe_dbt_failure({"success": False, "raw_result": None, "logs": ""}, "orders")

    assert message == "dbt run of model orders failed, and dbt gave no detail"

"""A catalog sync that finds zero tables must not be silent (core#883).

`get_table_names(schema=<missing>)` returns `[]` rather than raising for **every**
dialect we support, so a destination dataset that was deleted, renamed, or typo'd
introspects exactly like an empty one. `_sync_catalog_after_upload` then writes
nothing and returns `0` **as a success value**, and core#494's warning is on the
`except` path only — a sync that returns `0` never enters it.

The user is handed core#869's symptom with none of its diagnostics: a green run
with a row count, an empty `/models`, and no warning anywhere.

The discriminator needs no knowledge of *which* of the three causes applies
(genuinely empty load / missing dataset / misconfigured connection): **a load
that reports rows while introspection reports no tables is a contradiction.**
`rows == 0 and table_count == 0` is a legitimately empty load and stays silent —
that is the negative control below, and it is what stops the guard being
always-on.
"""

from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.upload_service import UploadService
from datanika.tasks.upload_tasks import run_upload

#: The destination fixture below is BigQuery with ``dataset: "d"`` — the schema
#: name the warning has to name, because "which schema did you look in" is the
#: one fact that separates a deleted dataset from a mistyped one.
DESTINATION_DATASET = "d"


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture
def setup_upload(db_session, encryption):
    import uuid

    conn_svc = ConnectionService(encryption)
    upload_svc = UploadService(conn_svc)
    exec_svc = ExecutionService()

    org = Organization(name="Acme", slug=f"acme-883-{uuid.uuid4().hex[:8]}")
    db_session.add(org)
    db_session.flush()

    src = conn_svc.create_connection(
        db_session, org.id, "S", ConnectionType.POSTGRES, {"host": "src", "port": 5432}
    )
    dst = conn_svc.create_connection(
        db_session,
        org.id,
        "D",
        ConnectionType.BIGQUERY,
        {"project": "p", "dataset": DESTINATION_DATASET},
    )
    upload = upload_svc.create_upload(
        db_session, org.id, "test", "desc", src.id, dst.id, {"write_disposition": "append"}
    )
    run = exec_svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    return org, upload, run, encryption


def _run(db_session, setup, *, rows_loaded: int, introspect_result: list):
    """Drive the real `run_upload` with a real introspection *result*.

    `introspect_tables` is patched with a return value rather than a
    `MagicMock` — per WORKFLOW_RULES, a `MagicMock` materialises whatever
    attribute is asked of it, and here the claim under test is precisely
    *what an empty result does downstream*. An empty list is the artifact a
    missing BigQuery dataset actually produces (measured on core#883).
    """
    org, upload, run, encryption = setup
    with (
        patch("datanika.tasks.upload_tasks.DltRunnerService") as mock_runner_cls,
        patch.object(CatalogService, "introspect_tables", return_value=introspect_result),
        patch("datanika.tasks.upload_tasks.DbtProjectService", return_value=MagicMock()),
    ):
        mock_runner_cls.return_value.execute.return_value = {
            "rows_loaded": rows_loaded,
            "load_info": "mock_load_info",
        }
        run_upload(run_id=run.id, org_id=org.id, session=db_session, encryption=encryption)
    db_session.refresh(run)
    return org, upload, run


ONE_TABLE = [{"table_name": "users", "columns": [{"name": "id", "data_type": "INTEGER"}]}]


class TestZeroTableCatalogSyncIsNotSilent:
    def test_rows_loaded_but_no_tables_found_warns_and_names_the_schema(
        self, db_session, setup_upload
    ):
        """AC1 — the contradiction is reported on the run, naming the schema.

        Red against unfixed code: `run.logs` holds only the dlt load info.
        """
        _, _, run = _run(db_session, setup_upload, rows_loaded=10, introspect_result=[])

        logs = run.logs or ""
        assert "WARNING" in logs, f"zero-table sync left no warning on the run; logs={logs!r}"
        assert "no tables were found" in logs
        # The schema name is the diagnostic. Without it the message cannot
        # distinguish "wrong dataset configured" from "dataset deleted".
        assert f"'{DESTINATION_DATASET}'" in logs, (
            "the warning must name the schema it introspected, or it cannot "
            f"discriminate the three causes; logs={logs!r}"
        )

    def test_a_sync_that_finds_tables_appends_no_such_warning(self, db_session, setup_upload):
        """AC2 — the negative control. Green today *and* after the fix.

        Without this, a guard that always fires would pass AC1.
        """
        _, _, run = _run(db_session, setup_upload, rows_loaded=10, introspect_result=ONE_TABLE)

        assert "no tables were found" not in (run.logs or "")

    def test_zero_rows_and_zero_tables_stays_silent(self, db_session, setup_upload):
        """AC3 — an empty load is not a contradiction and must not warn."""
        _, _, run = _run(db_session, setup_upload, rows_loaded=0, introspect_result=[])

        assert "no tables were found" not in (run.logs or "")

    def test_the_run_still_succeeds_and_the_load_is_untouched(self, db_session, setup_upload):
        """AC5 — diagnostics only. Status, row count and catalog are unchanged.

        The load genuinely succeeded; turning a reporting gap into a failed run
        would take working data away from the user to make a message appear.
        """
        org, _, run = _run(db_session, setup_upload, rows_loaded=10, introspect_result=[])

        assert run.status == RunStatus.SUCCESS
        assert run.rows_loaded == 10
        assert CatalogService.list_entries(db_session, org.id) == []

    def test_the_existing_sync_failure_warning_still_fires(self, db_session, setup_upload):
        """core#494's `except`-path warning must survive this change.

        The two messages are different signals — "the sync raised" versus "the
        sync succeeded and found nothing" — and collapsing them would put us
        back to one string for two causes, which is the defect core#830 is
        about on a different surface.
        """
        org, upload, run, encryption = setup_upload
        with (
            patch("datanika.tasks.upload_tasks.DltRunnerService") as mock_runner_cls,
            patch.object(
                CatalogService, "introspect_tables", side_effect=RuntimeError("introspect failed")
            ),
        ):
            mock_runner_cls.return_value.execute.return_value = {
                "rows_loaded": 5,
                "load_info": "ok",
            }
            run_upload(run_id=run.id, org_id=org.id, session=db_session, encryption=encryption)
        db_session.refresh(run)

        logs = run.logs or ""
        assert "the catalog sync failed" in logs
        # ...and NOT the zero-table message: `table_count` keeps its fallback of
        # 1 on the exception path, so the contradiction check must not also fire.
        assert "no tables were found" not in logs

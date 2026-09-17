"""A successful upload run records what its catalogue sync found (core#1398).

`/models` has to say, per upload, when the catalogue could not read what the upload loaded, and it
has to say which of two things happened (`SPEC_EARNED_VERDICTS` §4.6). The run's log already tells
them apart in prose. The page cannot rely on prose, so the run carries the verdict as data. Four
outcomes, driven through the real `run_upload` with the harness core#883's tests use:

* ``catalogued``: the sync found tables;
* ``empty``: 0 rows loaded and 0 tables found, a legitimately empty load (core#883's control);
* ``no_tables``: rows loaded and no tables found, recorded with the schema that was asked;
* ``unreadable``: the sync raised.

The status and the row count stay as they are. The load succeeded, and the verdict is about the
catalogue only.
"""

from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

from datanika.models.run import CatalogSyncVerdict, RunStatus
from datanika.services.catalog_service import CatalogService
from datanika.services.encryption import EncryptionService
from datanika.tasks.upload_tasks import run_upload
from tests.test_tasks.test_upload_catalog_sync_silence import (
    DESTINATION_DATASET,
    ONE_TABLE,
    _run,
    make_upload_setup,
)


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture
def setup_upload(db_session, encryption):
    return make_upload_setup(db_session, encryption)


def test_a_sync_that_finds_tables_records_catalogued(db_session, setup_upload):
    _, _, run = _run(db_session, setup_upload, rows_loaded=10, introspect_result=ONE_TABLE)

    assert run.catalog_sync_verdict == CatalogSyncVerdict.CATALOGUED
    assert run.catalog_sync_schema is None


def test_rows_with_no_tables_records_no_tables_and_the_schema_asked(db_session, setup_upload):
    _, _, run = _run(db_session, setup_upload, rows_loaded=10, introspect_result=[])

    assert run.catalog_sync_verdict == CatalogSyncVerdict.NO_TABLES
    assert run.catalog_sync_schema == DESTINATION_DATASET
    assert run.status == RunStatus.SUCCESS and run.rows_loaded == 10


def test_an_empty_load_records_empty(db_session, setup_upload):
    _, _, run = _run(db_session, setup_upload, rows_loaded=0, introspect_result=[])

    assert run.catalog_sync_verdict == CatalogSyncVerdict.EMPTY


def test_a_sync_that_raised_records_unreadable(db_session, setup_upload):
    org, _upload, run, encryption_service = setup_upload
    with (
        patch("datanika.tasks.upload_tasks.DltRunnerService") as mock_runner_cls,
        patch.object(
            CatalogService, "introspect_tables", side_effect=RuntimeError("introspect failed")
        ),
        patch("datanika.tasks.upload_tasks.DbtProjectService", return_value=MagicMock()),
    ):
        mock_runner_cls.return_value.execute.return_value = {"rows_loaded": 5, "load_info": "ok"}
        run_upload(run_id=run.id, org_id=org.id, session=db_session, encryption=encryption_service)
    db_session.refresh(run)

    assert run.catalog_sync_verdict == CatalogSyncVerdict.UNREADABLE
    assert run.status == RunStatus.SUCCESS and run.rows_loaded == 5

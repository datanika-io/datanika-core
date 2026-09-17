"""`/models` names an upload whose tables the catalogue did not read (core#1398).

`SPEC_EARNED_VERDICTS` §4.6. The page used to diagnose only an **entirely** empty catalogue, so
one catalogued upload anywhere in the org hid every uncatalogued one. The diagnosis it did give
("the schema may have been deleted, renamed, or set incorrectly") was wrong when the sync had
raised, because then the connection is correct and the product could not read the destination.

What these tests pin, all on the state the page renders:

* AC1: a notice names the upload, **beside other rows**, with a link to its run;
* AC2: the notice says which of the two things happened, and only ``no_tables`` carries the
  deleted / renamed / misconfigured advice;
* AC3: a later successful run that catalogues clears it;
* AC4: no notice for a catalogued upload, and none for a legitimately empty load.
"""

import json
import pathlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import datanika.ui.state.model_state as model_state_module
from datanika.models.catalog_entry import CatalogEntryType
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import CatalogSyncVerdict
from datanika.models.upload import Upload, UploadStatus
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService
from datanika.services.execution_service import ExecutionService
from datanika.ui.state.model_state import ModelState
from tests.test_ui.test_models_empty_state_after_a_load import _session_patch

_EN = json.loads(
    (pathlib.Path(__file__).resolve().parents[2] / "datanika" / "i18n" / "en.json").read_text(
        encoding="utf-8"
    )
)


class _St:
    """What `load_models` touches, with translations read from the real `en.json`."""

    _uncatalogued_notice = ModelState._uncatalogued_notice

    def __init__(self, org_id: int):
        self._org_id = org_id
        self.models: list = []
        self.loaded_without_catalog = False
        self.uncatalogued_uploads: list = []
        self.error_message = "stale"

    async def _get_org_id(self):
        return self._org_id

    async def _translated(self, key: str, fallback: str) -> str:
        return _EN.get(key, fallback)

    async def get_state(self, _cls):
        return SimpleNamespace(
            current_org=SimpleNamespace(id=self._org_id),
            current_user=SimpleNamespace(id=1),
        )


@pytest.fixture
def org(db_session):
    import uuid

    o = Organization(name="Acme", slug=f"acme-1398-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def connections(db_session, org):
    src = Connection(
        org_id=org.id,
        name="src",
        connection_type=ConnectionType.POSTGRES,
        direction=ConnectionDirection.SOURCE,
        config_encrypted="x",
    )
    dst = Connection(
        org_id=org.id,
        name="dst",
        connection_type=ConnectionType.CLICKHOUSE,
        direction=ConnectionDirection.DESTINATION,
        config_encrypted="x",
    )
    db_session.add_all([src, dst])
    db_session.flush()
    return src, dst


def _upload(db_session, org, connections, name: str) -> Upload:
    src, dst = connections
    upload = Upload(
        org_id=org.id,
        name=name,
        source_connection_id=src.id,
        destination_connection_id=dst.id,
        dlt_config={},
        status=UploadStatus.ACTIVE,
    )
    db_session.add(upload)
    db_session.flush()
    return upload


def _successful_run(db_session, org, upload, verdict: str, *, schema: str | None = None, rows=3):
    svc = ExecutionService()
    run = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    svc.complete_run(db_session, org.id, run.id, rows_loaded=rows, logs="ok")
    run.catalog_sync_verdict = verdict
    run.catalog_sync_schema = schema
    db_session.flush()
    return run


def _catalogue(db_session, org, upload, table: str):
    CatalogService.upsert_entry(
        db_session,
        org.id,
        entry_type=CatalogEntryType.SOURCE_TABLE,
        origin_type=NodeType.UPLOAD,
        origin_id=upload.id,
        table_name=table,
        schema_name="raw",
        dataset_name="raw",
        columns=[{"name": "id", "data_type": "INTEGER"}],
    )


async def _load(db_session, org_id: int) -> _St:
    st = _St(org_id)
    with (
        patch.object(
            model_state_module, "get_sync_session", return_value=_session_patch(db_session)
        ),
        patch.object(model_state_module, "EncryptionService", MagicMock()),
    ):
        await ModelState.load_models.fn(st)
    return st


@pytest.mark.asyncio
async def test_an_unreadable_upload_is_named_beside_other_rows(db_session, org, connections):
    """AC1 and AC2, and the red-first case: one catalogued upload, one whose sync raised."""
    catalogued = _upload(db_session, org, connections, "orders")
    _successful_run(db_session, org, catalogued, CatalogSyncVerdict.CATALOGUED)
    _catalogue(db_session, org, catalogued, "orders")
    unreadable = _upload(db_session, org, connections, "clicks")
    run = _successful_run(db_session, org, unreadable, CatalogSyncVerdict.UNREADABLE)

    st = await _load(db_session, org.id)

    assert [m.table_name for m in st.models] == ["orders"]
    assert [(n.upload_name, n.run_id) for n in st.uncatalogued_uploads] == [("clicks", run.id)]
    (notice,) = st.uncatalogued_uploads
    assert "clicks" in notice.message
    assert notice.run_href == f"/runs?run={run.id}"
    assert "deleted" not in notice.message, "a sync that raised was given the wrong causes"


@pytest.mark.asyncio
async def test_no_tables_names_the_schema_and_carries_the_advice(db_session, org, connections):
    """AC2: only this variant says the schema may have been deleted, renamed or misconfigured."""
    upload = _upload(db_session, org, connections, "events")
    _successful_run(db_session, org, upload, CatalogSyncVerdict.NO_TABLES, schema="raw_events")

    st = await _load(db_session, org.id)

    (notice,) = st.uncatalogued_uploads
    assert "events" in notice.message and "raw_events" in notice.message
    assert "deleted" in notice.message
    assert "{" not in notice.message, f"a placeholder reached the page: {notice.message}"


@pytest.mark.asyncio
async def test_a_later_catalogued_run_clears_the_notice(db_session, org, connections):
    """AC3: a notice that outlives its cause teaches the reader to ignore the next one."""
    upload = _upload(db_session, org, connections, "clicks")
    _successful_run(db_session, org, upload, CatalogSyncVerdict.UNREADABLE)
    _successful_run(db_session, org, upload, CatalogSyncVerdict.CATALOGUED)
    _catalogue(db_session, org, upload, "clicks")

    st = await _load(db_session, org.id)

    assert st.uncatalogued_uploads == []


@pytest.mark.asyncio
async def test_a_failed_run_after_does_not_clear_it(db_session, org, connections):
    """The notice is about the most recent SUCCESSFUL run. A failure loaded nothing new."""
    upload = _upload(db_session, org, connections, "clicks")
    _successful_run(db_session, org, upload, CatalogSyncVerdict.UNREADABLE)
    svc = ExecutionService()
    failed = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    svc.fail_run(db_session, org.id, failed.id, error_message="boom", logs="boom")

    st = await _load(db_session, org.id)

    assert [n.upload_name for n in st.uncatalogued_uploads] == ["clicks"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "verdict", [CatalogSyncVerdict.CATALOGUED, CatalogSyncVerdict.EMPTY, None], ids=str
)
async def test_no_notice_for_a_catalogued_an_empty_or_an_unrecorded_run(
    db_session, org, connections, verdict
):
    """AC4. ``None`` is a run from before the verdict was recorded: nothing is known about it."""
    upload = _upload(db_session, org, connections, "orders")
    _successful_run(db_session, org, upload, verdict, rows=0 if verdict == "empty" else 3)

    st = await _load(db_session, org.id)

    assert st.uncatalogued_uploads == []


def test_the_notice_sentences_are_keys_not_fallbacks():
    """The UI tests above read `en.json`; this is what stops a missing key passing as English."""
    for key in ("models.catalog_unreadable", "models.catalog_no_tables", "models.open_run"):
        assert _EN.get(key), key

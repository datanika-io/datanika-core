"""core#910 + core#912 — the two metering dimensions core computes and discards.

Core knows both numbers at the moment an upload finishes and keeps neither:

* **the mode** (`UploadMode.ETL` / `ELT`) is on the `Upload` row and `run_upload`
  branches on it, but `announce("run.upload_completed", ...)` never passed it —
  so `datanika_cloud_bytes_processed_total` ships as `{org_id}` while three
  specs and a Grafana panel are written against `{org_id, mode}`;
* **the byte count** is computed on both paths and handed to a hook, after
  which nothing records it. `datanika_bytes_processed_by_run` therefore has
  zero call sites, and core#907's fix for the cloud counter cannot carry over —
  core must never import cloud, and `usage_ledger` is cloud's.

Both are fixed by making the number durable rather than by adding a label to a
counter in a process that cannot see it.
"""

import os
import uuid
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

from datanika import hooks
from datanika.models.connection import ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import Run
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.upload_service import UploadService


@pytest.fixture(autouse=True)
def _clean_hooks():
    hooks.clear()
    yield
    hooks.clear()


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def upload_svc(conn_svc):
    return UploadService(conn_svc)


@pytest.fixture
def setup_upload(db_session, upload_svc, conn_svc, encryption):
    org = Organization(name="Acme", slug=f"acme-dims-{uuid.uuid4().hex[:8]}")
    db_session.add(org)
    db_session.flush()
    src = conn_svc.create_connection(
        db_session, org.id, "S", ConnectionType.POSTGRES, {"host": "src", "port": 5432}
    )
    dst = conn_svc.create_connection(
        db_session, org.id, "D", ConnectionType.BIGQUERY, {"project": "p", "dataset": "d"}
    )
    upload = upload_svc.create_upload(
        db_session, org.id, "test", "desc", src.id, dst.id, {"write_disposition": "append"}
    )
    run = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    return org, upload, run, encryption


class _FakeJob:
    def __init__(self, path: str) -> None:
        self.file_path = path


class _FakePackage:
    def __init__(self, jobs: list[_FakeJob]) -> None:
        self.jobs = {"completed_jobs": jobs}


class _FakeLoadInfo:
    """A LoadInfo shaped exactly the way `_extract_bytes_from_load_info` walks it.

    ⚠️ Deliberately **not** a `MagicMock`. A mock materialises whatever attribute
    is asked of it, so `load_info.load_packages` would be truthy and iterating it
    would yield mocks whose `file_path` is also truthy — the extractor would
    then be measured against a structure it manufactured. The byte total here
    comes from `os.path.getsize` over a real file on disk.
    """

    def __init__(self, packages: list[_FakePackage]) -> None:
        self.load_packages = packages

    def __str__(self) -> str:
        return "fake load info"


def _run_upload(db_session, org, run, encryption, load_info):
    from datanika.services.catalog_service import CatalogService
    from datanika.tasks.upload_tasks import run_upload

    with (
        patch("datanika.tasks.upload_tasks.DltRunnerService") as runner_cls,
        patch.object(CatalogService, "introspect_tables", return_value=[]),
        patch("datanika.tasks.upload_tasks.DbtProjectService", return_value=MagicMock()),
    ):
        runner_cls.return_value.execute.return_value = {
            "rows_loaded": 30,
            "load_info": load_info,
        }
        run_upload(run_id=run.id, org_id=org.id, session=db_session, encryption=encryption)


class TestTheUploadRunCarriesItsMode:
    def test_upload_completed_announces_the_ingestion_mode(self, db_session, setup_upload):
        org, upload, run, encryption = setup_upload
        spy = MagicMock()
        hooks.on("run.upload_completed", spy)

        _run_upload(db_session, org, run, encryption, "not-a-load-info")

        assert spy.call_count == 1
        kw = spy.call_args.kwargs
        assert kw["mode"] == "etl", (
            "cloud's bytes counter is specified as {org_id, mode} and the label cannot "
            "exist until core passes the dimension it already branches on (core#910)"
        )

    def test_the_mode_is_the_string_value_not_the_enum(self, db_session, setup_upload):
        """It crosses a process boundary into a ledger column, so it must be a str.

        `UploadMode` is a `StrEnum`, which makes an enum member compare equal to
        its value and hides this everywhere except where the value is written to
        a database or rendered into a Prometheus label.
        """
        org, upload, run, encryption = setup_upload
        spy = MagicMock()
        hooks.on("run.upload_completed", spy)

        _run_upload(db_session, org, run, encryption, "not-a-load-info")

        assert type(spy.call_args.kwargs["mode"]) is str


class TestModeIsNotFabricatedWhereItDoesNotExist:
    """core#910 step 2 asks for "the equivalent" from pipeline/transformation.

    There is no equivalent. `UploadMode` is a property of an *ingestion* run; a
    dbt model run and a transformation run do not read a source and have no
    mode. More decisively, neither event passes `bytes_processed` at all, so
    `handle_bytes_processed` returns immediately on both and writes no ledger
    row — a `mode` on them would label rows that do not exist.

    ⚠️ If a pipeline ever gains a real mode, change this test deliberately. It
    exists to stop the dimension being *invented* to satisfy a spec sentence,
    which is the failure this whole issue is about.
    """

    def _announce_kwargs(self, module_name: str, event: str) -> set[str]:
        import ast
        import importlib
        import pathlib

        path = pathlib.Path(importlib.import_module(module_name).__file__)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        found: set[str] = set()
        hits = 0
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
                continue
            if node.func.id != "announce" or not node.args:
                continue
            first = node.args[0]
            if isinstance(first, ast.Constant) and first.value == event:
                hits += 1
                found |= {kw.arg for kw in node.keywords if kw.arg}
        assert hits == 1, (
            f"expected exactly one announce({event!r}) in {module_name}, found {hits} — "
            f"the extractor is not reading what this test claims to check"
        )
        return found

    def test_model_completion_announces_no_mode(self):
        kwargs = self._announce_kwargs("datanika.tasks.pipeline_tasks", "run.models_completed")
        assert "bytes_processed" not in kwargs, "premise changed: this event now carries bytes"
        assert "mode" not in kwargs

    def test_transformation_completion_announces_no_mode(self):
        kwargs = self._announce_kwargs(
            "datanika.tasks.transformation_tasks", "run.transformation_completed"
        )
        assert "bytes_processed" not in kwargs, "premise changed: this event now carries bytes"
        assert "mode" not in kwargs


class TestTheRunRowRecordsItsByteCount:
    def test_the_byte_count_lands_on_the_run_row(self, db_session, setup_upload, tmp_path):
        """core#912 option (a): make the number durable, then it is observable.

        Core computes this on every upload and keeps no record — `Run` has
        `rows_loaded` and no byte count, so open-source core has no volume
        visibility at all and the histogram has nothing to read.
        """
        org, upload, run, encryption = setup_upload

        blob = tmp_path / "load.jsonl"
        blob.write_bytes(b"x" * 4096)
        expected = os.path.getsize(blob)
        load_info = _FakeLoadInfo([_FakePackage([_FakeJob(str(blob))])])

        _run_upload(db_session, org, run, encryption, load_info)

        row = db_session.get(Run, run.id)
        assert row.bytes_processed == expected, (
            "the byte count reached the hook and was then discarded; nothing in core "
            "records how large a load was (core#912)"
        )

    def test_a_run_with_no_extractable_bytes_records_null(self, db_session, setup_upload):
        """The negative control, and it is what makes NULL readable.

        A run that produced no measurable bytes must be distinguishable from one
        that predates the column. Writing 0 here would erase that distinction and
        put a fake floor in the histogram.
        """
        org, upload, run, encryption = setup_upload

        _run_upload(db_session, org, run, encryption, "not-a-load-info")

        row = db_session.get(Run, run.id)
        assert row.bytes_processed is None

    def test_the_column_is_nullable_and_wide_enough(self):
        """`BigInteger`, for the reason core#283 gave for `rows_loaded`.

        Bytes overflow int32 four thousand times sooner than rows do — 2 GiB in
        a single load is unremarkable, and `NumericValueOutOfRange` on insert
        would fail the run *after* the data moved.
        """
        from sqlalchemy import BigInteger

        col = Run.__table__.columns["bytes_processed"]
        assert col.nullable is True
        assert isinstance(col.type, BigInteger)

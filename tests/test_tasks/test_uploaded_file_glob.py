"""An uploaded file is read without a hand-written file pattern (core#1615).

The runner's default for a ``json`` connection is ``*.json``. The extract
directory holds exactly the uploaded file, so ``events.jsonl`` matches nothing
and the run loads zero rows. The witness is the rows in the destination, not
the absence of an error: a green run that matched nothing is the bug.
"""

from __future__ import annotations

import glob
import io
import os
from pathlib import Path

import duckdb
import pytest
from cryptography.fernet import Fernet

from datanika.config import settings
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.upload import Upload, UploadStatus
from datanika.models.user import Organization
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.file_formats import FILE_EXTENSIONS_BY_TYPE
from datanika.services.file_upload_service import FileUploadService
from datanika.tasks.upload_tasks import file_glob_for_uploaded_file, run_upload

_EXPECTED_ROWS = [(1, "alpha"), (2, "beta")]


def _content_for(file_type: str) -> bytes:
    if file_type == "csv":
        return b"id,value\n1,alpha\n2,beta\n"
    if file_type == "json":
        return b'{"id":1,"value":"alpha"}\n{"id":2,"value":"beta"}\n'

    import pyarrow
    import pyarrow.parquet

    buffer = io.BytesIO()
    pyarrow.parquet.write_table(pyarrow.table({"id": [1, 2], "value": ["alpha", "beta"]}), buffer)
    return buffer.getvalue()


def _uploaded_file_run(db_session, tmp_path, monkeypatch, file_type: str, filename: str):
    monkeypatch.setattr(settings, "file_uploads_dir", str(tmp_path / "uploads"))
    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt"))
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt"))

    encryption = EncryptionService(Fernet.generate_key().decode())
    org = Organization(name="Acme", slug=f"acme-1615-{tmp_path.name[-16:]}")
    db_session.add(org)
    db_session.flush()

    source = Connection(
        org_id=org.id,
        name="uploaded file",
        connection_type=ConnectionType(file_type),
        direction=ConnectionDirection.SOURCE,
        config_encrypted=encryption.encrypt({}),
    )
    destination_path = tmp_path / "warehouse.duckdb"
    destination = Connection(
        org_id=org.id,
        name="warehouse",
        connection_type=ConnectionType.DUCKDB,
        direction=ConnectionDirection.DESTINATION,
        config_encrypted=encryption.encrypt({"path": str(destination_path)}),
    )
    db_session.add_all([source, destination])
    db_session.flush()

    content = _content_for(file_type)
    uploaded = FileUploadService(settings.file_uploads_dir).save_file(
        db_session, org.id, filename, content
    )
    upload = Upload(
        org_id=org.id,
        name="uploaded file",
        source_connection_id=source.id,
        destination_connection_id=destination.id,
        dlt_config={"uploaded_file_id": uploaded.id},
        status=UploadStatus.DRAFT,
    )
    db_session.add(upload)
    db_session.flush()

    run = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    run_upload(run.id, org.id, session=db_session, encryption=encryption)
    db_session.refresh(run)
    return run, destination_path


def _loaded_rows(destination_path):
    if not destination_path.exists():
        return []

    with duckdb.connect(str(destination_path), read_only=True) as connection:
        tables = [
            (row[0], row[1])
            for row in connection.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE'"
            ).fetchall()
            if not str(row[1]).startswith("_dlt")
        ]
        rows = []
        for schema, table in tables:
            columns = {
                row[0]
                for row in connection.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = ? AND table_name = ?",
                    [schema, table],
                ).fetchall()
            }
            if "id" not in columns or "value" not in columns:
                continue
            rows.extend(
                connection.execute(
                    f'SELECT id, value FROM "{schema}"."{table}" ORDER BY id'
                ).fetchall()
            )
        return sorted(rows)


def _assert_rows_arrived(run, destination_path, filename: str) -> None:
    rows = _loaded_rows(destination_path)
    assert rows == _EXPECTED_ROWS, (
        f"{filename} was accepted but its rows did not arrive. "
        f"status={run.status} error={run.error_message}"
    )


@pytest.mark.parametrize(
    ("file_type", "extension"),
    [
        (file_type, extension)
        for file_type, extensions in FILE_EXTENSIONS_BY_TYPE.items()
        for extension in extensions
    ],
)
def test_uploaded_file_extensions_declared_for_the_connection_are_loaded(
    db_session, tmp_path, monkeypatch, file_type, extension
):
    filename = f"events{extension}"
    run, destination_path = _uploaded_file_run(
        db_session, tmp_path, monkeypatch, file_type, filename
    )
    _assert_rows_arrived(run, destination_path, filename)


@pytest.mark.parametrize(
    "filename",
    [
        pytest.param(
            "events*.jsonl",
            marks=pytest.mark.skipif(os.name == "nt", reason="Windows forbids '*' in a filename"),
        ),
        pytest.param(
            "ev?nts.jsonl",
            marks=pytest.mark.skipif(os.name == "nt", reason="Windows forbids '?' in a filename"),
        ),
        # A closed class, not an unclosed '['. ``events[part.jsonl`` is a literal
        # to fnmatch; ``events[a].jsonl`` matches ``eventsa.jsonl`` and not itself.
        "events[a].jsonl",
    ],
)
def test_uploaded_filename_glob_metacharacters_still_load_the_file(
    db_session, tmp_path, monkeypatch, filename
):
    run, destination_path = _uploaded_file_run(db_session, tmp_path, monkeypatch, "json", filename)
    _assert_rows_arrived(run, destination_path, filename)


def _listed_names(root: Path, pattern: str) -> set[str]:
    """Names dlt's local lister would return for this pattern.

    ``fsspec_filesystem.glob_files`` lists a local directory with
    ``glob.glob(str(Path(root).joinpath(file_glob)), recursive=True)``.
    """
    return {Path(match).name for match in glob.glob(str(root.joinpath(pattern)), recursive=True)}


def test_literal_glob_escapes_fnmatch_metacharacters():
    assert file_glob_for_uploaded_file("events.jsonl", "json", None) == "events.jsonl"
    assert file_glob_for_uploaded_file("events[a].jsonl", "json", None) == "events[[]a].jsonl"
    assert file_glob_for_uploaded_file("events*.jsonl", "json", None) == "events[*].jsonl"
    assert file_glob_for_uploaded_file("ev?nts.jsonl", "json", None) == "ev[?]nts.jsonl"
    # The glob is matched on a case-sensitive filesystem. The extension check
    # is case-insensitive; the pattern keeps the name the archive stored.
    assert file_glob_for_uploaded_file("Events.JSONL", "json", None) == "Events.JSONL"
    assert file_glob_for_uploaded_file("inbox/events.jsonl", "json", None) == "events.jsonl"


def test_literal_glob_matches_the_uploaded_name_and_not_a_decoy(tmp_path):
    real = tmp_path / "events[a].jsonl"
    decoy = tmp_path / "eventsa.jsonl"
    real.write_text("{}", encoding="utf-8")
    decoy.write_text("{}", encoding="utf-8")

    pattern = file_glob_for_uploaded_file(real.name, "json", None)
    assert _listed_names(tmp_path, pattern) == {real.name}
    # The two shortcuts this must refuse. The raw name is a character class,
    # so it reads the decoy. ``*`` reads both, and the extract directory is not
    # cleared between identical uploads (core#1615).
    assert _listed_names(tmp_path, real.name) == {decoy.name}
    assert _listed_names(tmp_path, "*") == {real.name, decoy.name}


def test_explicit_file_glob_is_kept():
    assert file_glob_for_uploaded_file("events.jsonl", "json", "part-*.jsonl") == "part-*.jsonl"


def test_extension_this_connection_does_not_read_keeps_the_runner_default():
    assert file_glob_for_uploaded_file("events.jsonl", "csv", None) is None


@pytest.mark.parametrize(
    ("file_type", "extension"),
    [
        (file_type, extension)
        for file_type, extensions in FILE_EXTENSIONS_BY_TYPE.items()
        for extension in extensions
    ],
)
def test_each_declared_extension_is_named_as_its_own_file(file_type, extension):
    filename = f"events{extension}"
    assert file_glob_for_uploaded_file(filename, file_type, None) == filename

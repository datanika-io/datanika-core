"""A run's stored error text and logs never carry its own connection secrets (core#1460).

The witness is the stored run row, read back after the real ``run_upload``. Two connectors, one on
each side of an upload:

* **a source**: Pipedrive, refused with ``401`` by a real loopback server, so the failure text is
  the HTTP client's own;
* **a destination**: PostgreSQL, whose driver is made to fail with a message that quotes the
  credential it was handed. The credential reaches the driver exactly as the product builds it
  from the stored connection. The password carries ``@`` and ``/``, so it travels
  percent-encoded, and the message holds that spelling rather than the stored one.

Each arm also asserts the stored text still says what went wrong: the secret goes, the diagnosis
stays.
"""

from __future__ import annotations

import http.server
import json
import sqlite3
import threading
import urllib.parse

import pytest
from cryptography.fernet import Fernet

from datanika.config import settings
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.upload import Upload, UploadStatus
from datanika.models.user import Organization
from datanika.services.dlt_runner import DltRunnerService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.upload_tasks import run_upload

MARKER = "Marker-Key-1460-q7Zp"
PASSWORD = "Marker@1460/pw-Zq"


class _Refuses(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        body = json.dumps({"error": "unauthorized"}).encode()
        self.send_response(401)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def refusing_server(monkeypatch):
    """A real loopback server that answers 401; the egress guard is opened for it (#441's shape)."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Refuses)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr("datanika.services.dlt_runner.validate_egress_host", lambda url: None)
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
    )
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture(autouse=True)
def _dirs_in_tmp(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt"))
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt"))


def _run(db_session, tmp_path, source, destination, dlt_config) -> dict:
    """``source`` and ``destination`` are ``(ConnectionType, config)``."""
    encryption = EncryptionService(Fernet.generate_key().decode())
    org = Organization(name="Acme", slug=f"acme-1460-{tmp_path.name[-16:]}")
    db_session.add(org)
    db_session.flush()
    src, dst = (
        Connection(
            org_id=org.id,
            name=name,
            connection_type=ctype,
            direction=direction,
            config_encrypted=encryption.encrypt(config),
        )
        for name, (ctype, config), direction in (
            ("source", source, ConnectionDirection.SOURCE),
            ("warehouse", destination, ConnectionDirection.DESTINATION),
        )
    )
    db_session.add_all([src, dst])
    db_session.flush()
    upload = Upload(
        org_id=org.id,
        name="secret probe",
        source_connection_id=src.id,
        destination_connection_id=dst.id,
        dlt_config=dlt_config,
        status=UploadStatus.DRAFT,
    )
    db_session.add(upload)
    db_session.flush()
    run = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    run_upload(run.id, org.id, session=db_session, encryption=encryption)
    db_session.refresh(run)
    return {"status": run.status, "error": run.error_message or "", "logs": run.logs or ""}


def _spellings(secret: str) -> list[str]:
    return [secret, urllib.parse.quote(secret, safe=""), urllib.parse.quote_plus(secret)]


def _assert_stored_without(reading: dict, secret: str, diagnosis: str) -> None:
    assert reading["status"] == RunStatus.FAILED, reading["status"]
    assert diagnosis in reading["error"] + reading["logs"], (
        f"the stored text no longer says {diagnosis!r}, so this arm measures nothing"
    )
    for field in ("error", "logs"):
        for spelling in _spellings(secret):
            assert spelling not in reading[field], (
                f"the run's stored {field} carries the connection's secret, spelled {spelling!r}"
            )


def _duckdb(tmp_path):
    return ConnectionType.DUCKDB, {"path": str(tmp_path / "destination.duckdb")}


def test_a_refused_source_stores_no_key(db_session, tmp_path, refusing_server, monkeypatch):
    original = DltRunnerService._rest_api_fallback.__func__

    def to_the_server(cls, base_url, auth, resources, *, paginator, headers=None):
        rewritten = refusing_server + urllib.parse.urlsplit(base_url).path
        return original(cls, rewritten, auth, resources, paginator=paginator, headers=headers)

    monkeypatch.setattr(DltRunnerService, "_rest_api_fallback", classmethod(to_the_server))

    reading = _run(
        db_session,
        tmp_path,
        (ConnectionType.PIPEDRIVE, {"api_key": MARKER}),
        _duckdb(tmp_path),
        {},
    )

    _assert_stored_without(reading, MARKER, "401")


def test_a_destination_driver_that_quotes_its_credential_stores_no_password(
    db_session, tmp_path, monkeypatch
):
    import psycopg2

    def quotes_the_credential_it_was_given(dsn=None, **_kwargs):
        credential = dsn.split("://", 1)[1].rsplit("@", 1)[0].split(":", 1)[1]
        raise psycopg2.OperationalError(f"server refused the login, credential {credential}")

    monkeypatch.setattr(psycopg2, "connect", quotes_the_credential_it_was_given)
    source_file = tmp_path / "source.sqlite"
    con = sqlite3.connect(source_file)
    con.execute("CREATE TABLE events (id INTEGER PRIMARY KEY)")
    con.execute("INSERT INTO events VALUES (1)")
    con.commit()
    con.close()

    reading = _run(
        db_session,
        tmp_path,
        (ConnectionType.SQLITE, {"path": str(source_file)}),
        (
            ConnectionType.POSTGRES,
            {
                "host": "warehouse.invalid",
                "port": 5432,
                "database": "dw",
                "user": "loader",
                "password": PASSWORD,
            },
        ),
        {"mode": "full_database"},
    )

    _assert_stored_without(reading, PASSWORD, "server refused the login")

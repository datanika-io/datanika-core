"""The stored run row of a SaaS **source**, read back after the real ``run_upload`` (core#1460).

``test_run_text_never_carries_connection_secrets.py`` witnesses a Pipedrive source refused by a real
loopback server. That arm proves the run fails and stores a diagnosis; it cannot prove the *auth*
the product built would be removed, because a ``401`` body does not contain it.

This arm supplies that half, on a connector type that file does not cover, with the technique its
own destination arm uses: **a client made to quote the credential it was handed.** What is under
test is the store, not the client — the whole point of redacting where run text is written is that
the guarantee does not depend on which call site produced the text.

Fourteen SaaS connector types reach ``_rest_api_fallback``; Notion is one of them, and the auth dict
it hands over is built by the product from the stored connection.
"""

from __future__ import annotations

import urllib.parse

from cryptography.fernet import Fernet

from datanika.config import settings
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.upload import Upload, UploadStatus
from datanika.models.user import Organization
from datanika.services.dlt_runner import DltRunnerError, DltRunnerService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.upload_tasks import run_upload

#: Carries ``@``, ``/`` and a space so the value travels in more than one spelling.
MARKER = "Notion@Marker/1460 Zq7Kx"

DIAGNOSIS = "the API rejected the credential"


def _spellings(secret: str) -> list[str]:
    return [secret, urllib.parse.quote(secret, safe=""), urllib.parse.quote_plus(secret)]


def test_a_saas_source_whose_client_quotes_its_auth_stores_no_token(
    db_session, tmp_path, monkeypatch
):
    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt"))
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt"))

    def quotes_the_auth_it_was_given(cls, base_url, auth, resources, *, paginator, headers=None):
        raise DltRunnerError(f"{DIAGNOSIS}: {auth}")

    monkeypatch.setattr(
        DltRunnerService, "_rest_api_fallback", classmethod(quotes_the_auth_it_was_given)
    )

    encryption = EncryptionService(Fernet.generate_key().decode())
    org = Organization(name="Acme", slug=f"acme-saas-{tmp_path.name[-16:]}")
    db_session.add(org)
    db_session.flush()
    source = Connection(
        org_id=org.id,
        name="notion",
        connection_type=ConnectionType.NOTION,
        direction=ConnectionDirection.SOURCE,
        config_encrypted=encryption.encrypt({"api_key": MARKER}),
    )
    destination = Connection(
        org_id=org.id,
        name="warehouse",
        connection_type=ConnectionType.DUCKDB,
        direction=ConnectionDirection.DESTINATION,
        config_encrypted=encryption.encrypt({"path": str(tmp_path / "destination.duckdb")}),
    )
    db_session.add_all([source, destination])
    db_session.flush()
    upload = Upload(
        org_id=org.id,
        name="saas secret witness",
        source_connection_id=source.id,
        destination_connection_id=destination.id,
        dlt_config={},
        status=UploadStatus.DRAFT,
    )
    db_session.add(upload)
    db_session.flush()
    run = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, upload.id)

    run_upload(run.id, org.id, session=db_session, encryption=encryption)
    db_session.refresh(run)

    stored = {"error_message": run.error_message or "", "logs": run.logs or ""}
    assert run.status == RunStatus.FAILED, run.status
    assert DIAGNOSIS in stored["error_message"] + stored["logs"], (
        f"the client was never reached, so this arm measures nothing: {stored}"
    )
    for field, text in stored.items():
        for spelling in _spellings(MARKER):
            assert spelling not in text, (
                f"the run's stored {field} carries the connection's token, spelled {spelling!r}"
            )

"""Every surface that accepts a cursor resumes it, or refuses it at save (core#1404).

``SPEC_INCREMENTAL_UPLOADS`` §2.5: *the product never calls something incremental that does not
resume.* A cursor resumes only on the single-table SQL path, whose builder passes it to dlt's
``sql_table``. MongoDB applied the initial value as a fixed filter that never advanced, and every
other source ignored the cursor, so an upload saved with ``incremental`` for one of them read the
same rows on every run while calling itself incremental. ``UploadService`` is where the form, the
REST API, the MCP tools and the YAML import all save an upload, so the refusal lives there.
"""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionType
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.upload_service import UploadConfigError, UploadService
from tests.factories import make_org_admin

CURSOR = {
    "mode": "single_table",
    "table": "events",
    "incremental": {"cursor_path": "updated_at", "initial_value": "30"},
}


@pytest.fixture
def setup(db_session):
    encryption = EncryptionService(Fernet.generate_key().decode())
    conn_svc = ConnectionService(encryption)
    org = Organization(name="Acme", slug="acme-incremental-surfaces")
    db_session.add(org)
    db_session.flush()
    actor = make_org_admin(db_session, org.id)

    def connection(name, ctype, config):
        return conn_svc.create_connection(
            db_session, org.id, name, ctype, config, actor_user_id=actor
        )

    destination = connection("Warehouse", ConnectionType.POSTGRES, {"host": "warehouse"})
    return UploadService(conn_svc), org, actor, connection, destination


def _create(setup, db_session, source, dlt_config):
    svc, org, actor, _connection, destination = setup
    return svc.create_upload(
        db_session,
        org.id,
        "Events",
        None,
        source.id,
        destination.id,
        dlt_config,
        actor_user_id=actor,
    )


def test_a_sql_source_saves_its_cursor(setup, db_session):
    _svc, _org, _actor, connection, _destination = setup
    source = connection("Events DB", ConnectionType.POSTGRES, {"host": "source"})

    upload = _create(setup, db_session, source, dict(CURSOR))

    assert upload.dlt_config["incremental"]["cursor_path"] == "updated_at"


@pytest.mark.parametrize(
    "ctype, config",
    [
        pytest.param(ConnectionType.MONGODB, {"host": "mongo", "database": "app"}, id="mongodb"),
        pytest.param(ConnectionType.STRIPE, {"api_key": "sk_test_x"}, id="saas"),
        pytest.param(ConnectionType.CSV, {"bucket_url": "s3://bucket/events/"}, id="file"),
    ],
)
def test_a_source_whose_cursor_would_not_advance_is_refused_at_save(
    setup, db_session, ctype, config
):
    _svc, _org, _actor, connection, _destination = setup
    source = connection("Other source", ctype, config)

    with pytest.raises(UploadConfigError) as refused:
        _create(setup, db_session, source, dict(CURSOR))

    assert ctype.value in str(refused.value) and "incremental" in str(refused.value)


def test_control_the_same_source_saves_without_a_cursor(setup, db_session):
    _svc, _org, _actor, connection, _destination = setup
    source = connection("Mongo", ConnectionType.MONGODB, {"host": "mongo", "database": "app"})
    config = {k: v for k, v in CURSOR.items() if k != "incremental"}

    upload = _create(setup, db_session, source, config)

    assert "incremental" not in upload.dlt_config


def test_an_edit_that_adds_a_cursor_to_such_a_source_is_refused(setup, db_session):
    svc, org, actor, connection, _destination = setup
    source = connection("Mongo", ConnectionType.MONGODB, {"host": "mongo", "database": "app"})
    upload = _create(
        setup, db_session, source, {k: v for k, v in CURSOR.items() if k != "incremental"}
    )

    with pytest.raises(UploadConfigError):
        svc.update_upload(
            db_session, org.id, upload.id, actor_user_id=actor, dlt_config=dict(CURSOR)
        )


def test_control_an_edit_to_a_sql_uploads_cursor_saves(setup, db_session):
    svc, org, actor, connection, _destination = setup
    source = connection("Events DB", ConnectionType.POSTGRES, {"host": "source"})
    upload = _create(setup, db_session, source, dict(CURSOR))
    edited = {**CURSOR, "incremental": {"cursor_path": "id"}}

    saved = svc.update_upload(db_session, org.id, upload.id, actor_user_id=actor, dlt_config=edited)

    assert saved.dlt_config["incremental"]["cursor_path"] == "id"

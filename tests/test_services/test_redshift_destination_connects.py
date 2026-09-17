"""The Redshift destination reaches a server with the connection string it builds (core#1456).

Redshift speaks PostgreSQL's wire protocol, and dlt's Redshift client connects through psycopg2, so
a real PostgreSQL is a faithful stand-in for the question here: *can the destination connect at
all?* Before the fix it could not. Its DSN began with the source map's
``redshift+redshift_connector``, and psycopg2 refused it (``invalid dsn``) before any network
activity, while Test Connection, which builds its own ``postgresql+psycopg2`` URL, reported success
against the same server.

The PostgreSQL destination is the control: the same server, the same builder, a DSN that parsed
before and after.

⚠️ Only the connection is asserted here. A load through the Redshift destination cannot run on a
PostgreSQL stand-in: dlt writes Redshift's own DDL (``varchar(max)``), which PostgreSQL rejects.
That was measured when a load arm was tried, and it is a property of the stand-in, not of the fix.
"""

from __future__ import annotations

import pytest

from datanika.services.dlt_runner import DltRunnerService
from tests.test_services.test_source_builders_move_rows import await_setup, requires_docker


def _config(container) -> dict:
    import sqlalchemy

    engine = sqlalchemy.create_engine(container.get_connection_url())
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text("SELECT 1"))
    engine.dispose()
    return {
        "host": container.get_container_host_ip(),
        "port": int(container.get_exposed_port(5432)),
        "user": container.username,
        "password": container.password,
        "database": container.dbname,
    }


@pytest.fixture(scope="module")
def server():
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as container:
        yield await_setup(
            "postgres accepting connections", lambda: _config(container), container=container
        )


@requires_docker
@pytest.mark.parametrize("destination", ["redshift", "postgres"])
def test_the_destination_opens_its_own_client_against_the_server(tmp_path, server, destination):
    pipeline = DltRunnerService(pipelines_dir=str(tmp_path / "dlt")).build_pipeline(
        1, destination, dict(server), dataset_name="probe", run_id=1
    )

    with pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT 1")

    assert [tuple(row) for row in rows] == [(1,)]

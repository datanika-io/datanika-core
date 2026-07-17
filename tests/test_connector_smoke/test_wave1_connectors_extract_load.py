"""Nightly live-connector E2E — Wave-1 drop (core#311).

The *extract → load → assert* layer for the Wave-1 connectors, complementing
``test_wave1_connectors_smoke.py`` (which only does auth + metadata). Each test
drives the **real** Datanika connector path — ``DltRunnerService.execute()``,
the same synchronous entrypoint the upload task calls
(``datanika/tasks/upload_tasks.py`` → ``runner.execute(...)``) — to extract a
single resource from the live vendor into a scratch DuckDB file, then asserts on
the loaded rows + schema by reading the DuckDB back. No Celery / DB / session.

Coverage split (deliberate):
- Auth + metadata reachability → ``test_wave1_connectors_smoke.py``.
- Offline correctness (URL/auth/IR) → ``tests/test_services/test_wave1_connectors.py``.
- **This suite** → the full live ELT round-trip (extract → load → assert) per connector.

Each test overrides ``dlt_config["resources"]`` to ONE low-volume, no-params
endpoint so the load is fast and deterministic (``write_disposition="replace"``
makes reruns idempotent). Endpoints are chosen to always return >= 1 row for the
QA account (its own user / agent / workspace) and to need no query params — e.g.
Asana's default ``tasks`` resource 400s without a project/assignee, so we use
``workspaces``.

Env vars (identical to the smoke; in CI materialized from ``QA_WAVE1_CREDENTIALS``
+ the Oracle XE service container — see ``.github/workflows/nightly-connector-smoke.yml``):

| Connector | Env vars |
|-----------|----------|
| Oracle    | ``ORACLE_{HOST,PORT,SERVICE,USER,PASSWORD}`` |
| Pipedrive | ``PIPEDRIVE_API_TOKEN`` |
| Freshdesk | ``FRESHDESK_API_KEY`` ``FRESHDESK_DOMAIN`` |
| Asana     | ``ASANA_ACCESS_TOKEN`` |

Skipped unless ``DATANIKA_CONNECTOR_SMOKE=1`` (see conftest.py). Locally:

    set -a && source secrets/pipedrive.env secrets/freshdesk.env secrets/asana.env && set +a
    DATANIKA_CONNECTOR_SMOKE=1 uv run pytest \\
        tests/test_connector_smoke/test_wave1_connectors_extract_load.py -v
"""

from __future__ import annotations

import contextlib

import duckdb
import pytest

from datanika.services.dlt_runner import DltRunnerService


def _log(msg: str) -> None:
    """Print with a stable prefix so the nightly log is scannable."""
    print(f"[e2e] {msg}")


def _saas_extract_load(source_type: str, source_config: dict, resource: str, tmp_path) -> tuple:
    """Extract one ``resource`` from a live SaaS source into a scratch DuckDB via the
    real ``DltRunnerService.execute()`` path.

    Returns ``(db_path, dataset, rows_loaded)``. ``dataset`` is the DuckDB schema
    dlt writes the resource table into.
    """
    db_path = str(tmp_path / "scratch.duckdb")
    dataset = f"{source_type}_e2e"
    runner = DltRunnerService(pipelines_dir=str(tmp_path / "dlt"))
    result = runner.execute(
        pipeline_id=1,
        source_type=source_type,
        source_config=source_config,
        destination_type="duckdb",
        destination_config={"path": db_path},
        dlt_config={
            # Override the connector's default resource list down to one safe endpoint.
            "resources": [{"name": resource, "endpoint": {"path": resource}}],
            "write_disposition": "replace",
        },
        dataset_name=dataset,
        run_id=1,
    )
    return db_path, dataset, result["rows_loaded"]


def _read_back(db_path: str, dataset: str, table: str) -> tuple[int, set[str]]:
    """Reopen the scratch DuckDB and return (row_count, column_names) for a loaded table."""
    con = duckdb.connect(db_path)
    try:
        n = con.execute(f'SELECT count(*) FROM "{dataset}"."{table}"').fetchone()[0]
        cols = {
            row[0]
            for row in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [dataset, table],
            ).fetchall()
        }
    finally:
        con.close()
    return n, cols


# --------------------------------------------------------------------------- #
# Pipedrive (SaaS REST — api_token query auth)
# --------------------------------------------------------------------------- #


def test_pipedrive_extract_load_assert(require_env, tmp_path):
    """Extract Pipedrive ``users`` → DuckDB → assert rows + ``id`` column landed."""
    env = require_env("PIPEDRIVE_API_TOKEN")
    db_path, dataset, rows = _saas_extract_load(
        "pipedrive", {"api_key": env["PIPEDRIVE_API_TOKEN"]}, "users", tmp_path
    )
    assert rows >= 1, f"Pipedrive users: dlt reported {rows} rows loaded"
    n, cols = _read_back(db_path, dataset, "users")
    _log(f"pipedrive: users rows={n} cols={len(cols)}")
    assert n >= 1, f"Pipedrive users: {n} rows in DuckDB {dataset}.users"
    assert "id" in cols, f"Pipedrive users: no 'id' column; got {sorted(cols)}"


# --------------------------------------------------------------------------- #
# Freshdesk (SaaS REST — HTTP basic auth)
# --------------------------------------------------------------------------- #


def test_freshdesk_extract_load_assert(require_env, tmp_path):
    """Extract Freshdesk ``agents`` → DuckDB → assert rows + ``id`` column landed."""
    env = require_env("FRESHDESK_API_KEY", "FRESHDESK_DOMAIN")
    subdomain = env["FRESHDESK_DOMAIN"].strip().removesuffix(".freshdesk.com")
    db_path, dataset, rows = _saas_extract_load(
        "freshdesk",
        {"api_key": env["FRESHDESK_API_KEY"], "domain": subdomain},
        "agents",
        tmp_path,
    )
    assert rows >= 1, f"Freshdesk agents: dlt reported {rows} rows loaded"
    n, cols = _read_back(db_path, dataset, "agents")
    _log(f"freshdesk: agents rows={n} cols={len(cols)}")
    assert n >= 1, f"Freshdesk agents: {n} rows in DuckDB {dataset}.agents"
    assert "id" in cols, f"Freshdesk agents: no 'id' column; got {sorted(cols)}"


# --------------------------------------------------------------------------- #
# Asana (SaaS REST — bearer PAT)
# --------------------------------------------------------------------------- #


def test_asana_extract_load_assert(require_env, tmp_path):
    """Extract Asana ``workspaces`` → DuckDB → assert rows + ``gid`` column landed.

    ``workspaces`` (not the default ``tasks``, which 400s without a project/assignee)
    always returns >= 1 for a valid PAT.
    """
    env = require_env("ASANA_ACCESS_TOKEN")
    db_path, dataset, rows = _saas_extract_load(
        "asana", {"api_key": env["ASANA_ACCESS_TOKEN"]}, "workspaces", tmp_path
    )
    assert rows >= 1, f"Asana workspaces: dlt reported {rows} rows loaded"
    n, cols = _read_back(db_path, dataset, "workspaces")
    _log(f"asana: workspaces rows={n} cols={len(cols)}")
    assert n >= 1, f"Asana workspaces: {n} rows in DuckDB {dataset}.workspaces"
    assert "gid" in cols, f"Asana workspaces: no 'gid' column; got {sorted(cols)}"


# --------------------------------------------------------------------------- #
# Oracle (SQL, source-only) — xfails on core#329 until the DSN fix lands
# --------------------------------------------------------------------------- #


@pytest.mark.xfail(
    reason="core#329: the Oracle connector emits the service name as a SID in the DSN "
    "(connection_service._build_sa_url / dlt_runner._to_dlt_credentials), so extract via "
    "execute() raises ORA-12505 against a service-name listener (the XE PDB). Flips to "
    "XPASS when #329 lands — delete this marker then.",
    strict=False,
    raises=Exception,
)
def test_oracle_extract_load_assert(require_env, tmp_path):
    """Seed a tiny table in Oracle XE (correct service-name form = positive control),
    then extract it through the Datanika connector → DuckDB → assert the rows land.

    The seed proves the driver/container/creds are healthy; the ``execute()`` call
    exercises the connector's own DSN path, which currently mishandles service names
    (core#329) and raises ORA-12505 — caught by the xfail above.
    """
    env = require_env(
        "ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "ORACLE_USER", "ORACLE_PASSWORD"
    )

    from sqlalchemy import create_engine, text

    host, port, service = env["ORACLE_HOST"], int(env["ORACLE_PORT"]), env["ORACLE_SERVICE"]
    user, password = env["ORACLE_USER"], env["ORACLE_PASSWORD"]
    table = "QA_E2E_WAVE1"

    # Seed via the correct service-name easy-connect form (the smoke's positive control).
    seed = create_engine(
        f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service}",
        connect_args={"tcp_connect_timeout": 5},
    )
    try:
        with seed.connect() as conn:
            with contextlib.suppress(Exception):  # first run: table doesn't exist yet
                conn.execute(text(f"DROP TABLE {table}"))  # DDL auto-commits in Oracle
            conn.execute(text(f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(50))"))
            conn.execute(text(f"INSERT INTO {table} (id, name) VALUES (1, 'alpha')"))
            conn.execute(text(f"INSERT INTO {table} (id, name) VALUES (2, 'beta')"))
            conn.commit()
    finally:
        seed.dispose()

    # Extract through the REAL connector path (single_table mode) into a scratch DuckDB.
    db_path = str(tmp_path / "scratch.duckdb")
    dataset = "oracle_e2e"
    runner = DltRunnerService(pipelines_dir=str(tmp_path / "dlt"))
    result = runner.execute(
        pipeline_id=1,
        source_type="oracle",
        source_config={
            "host": host,
            "port": port,
            "database": service,  # Oracle service name — core#329 mis-treats this as a SID
            "user": user,
            "password": password,
        },
        destination_type="duckdb",
        destination_config={"path": db_path},
        dlt_config={"mode": "single_table", "table": table, "write_disposition": "replace"},
        dataset_name=dataset,
        run_id=1,
    )
    assert result["rows_loaded"] >= 2, f"Oracle {table}: dlt reported {result['rows_loaded']} rows"
    n, _cols = _read_back(db_path, dataset, table.lower())  # dlt lowercases identifiers
    _log(f"oracle: {table} rows={n}")
    assert n >= 2, f"Oracle {table}: {n} rows in DuckDB {dataset}.{table.lower()}"

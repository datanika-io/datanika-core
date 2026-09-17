"""An upload into ClickHouse is catalogued under the name that queries it (core#1397).

The catalogue is meant to cover every destination. A ClickHouse upload finished `success` with
its rows in the destination and **zero** catalogue entries. Two defects were stacked, and fixing
only the visible one still catalogues nothing:

1. **The dialect cannot list tables.** clickhouse-connect 0.11's SQLAlchemy dialect passes a plain
   string to ``connection.execute``, which SQLAlchemy 2 refuses before anything reaches the server
   (``ObjectNotExecutableError``). That happens for every schema, including one that exists.
2. **Under it, the sync asks the wrong level.** dlt's ClickHouse destination has no schema level. A
   dataset becomes a table-name **prefix** (``<dataset>___<table>``) inside the connection's
   database. So ``SHOW TABLES FROM <dataset>`` is ``UNKNOWN_DATABASE``, and dlt's bookkeeping tables
   begin with the prefix rather than with ``_dlt_``.

Every assertion reads a real ClickHouse server through the product's own ``run_upload``, with no
patch on the load or on the catalogue sync.
"""

from pathlib import Path

import pytest
import yaml
from cryptography.fernet import Fernet
from sqlalchemy import select

from datanika.config import settings
from datanika.models.catalog_entry import CatalogEntry
from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus
from datanika.models.upload import Upload, UploadStatus
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.upload_tasks import run_upload
from tests.test_services.test_clickhouse_destination_ports import (
    CLICKHOUSE_IMAGE,
    PASSWORD,
    USER,
    stored_connection,
)
from tests.test_services.test_rerun_lands_each_record_once import WIDGETS as CSV_WIDGETS
from tests.test_services.test_rerun_lands_each_record_once import _write_csv, form_config
from tests.test_services.test_source_builders_move_rows import requires_docker

pytestmark = requires_docker

#: The connection's database. dlt writes every dataset into it as a table-name prefix.
DATABASE = "raw_data"
UPLOAD_NAME = "widgets load"
DATASET = "widgets_load"  # to_dataset_name(UPLOAD_NAME)
LOADED_TABLE = f"{DATASET}___widgets"
WIDGETS = sorted((w["id"], w["name"], w["price"]) for w in CSV_WIDGETS)


@pytest.fixture(scope="module")
def clickhouse():
    """A real server on ClickHouse's own ports. The native port is derived, not stored (#1341)."""
    from testcontainers.clickhouse import ClickHouseContainer

    container = ClickHouseContainer(
        CLICKHOUSE_IMAGE, username=USER, password=PASSWORD, dbname=DATABASE
    )
    container.with_bind_ports(9000, 9000)
    container.with_bind_ports(8123, 8123)
    with container:
        yield container


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture(autouse=True)
def _dirs_in_tmp(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "dlt_pipelines_dir", str(tmp_path / "dlt"))
    monkeypatch.setattr(settings, "dbt_projects_dir", str(tmp_path / "dbt"))


def _destination_config(clickhouse) -> dict:
    config = stored_connection(clickhouse.get_container_host_ip())
    config["database"] = DATABASE
    return config


@pytest.fixture
def loaded(db_session, encryption, tmp_path, clickhouse):
    """One upload of a three-row CSV into ClickHouse, run through ``run_upload``.

    A CSV source, as in the landing#604 walk. It declares no primary key, so ClickHouse's sorting
    key is dlt's default and the load itself is not what is being tested.
    """
    drop = tmp_path / "drop"
    drop.mkdir()
    _write_csv(drop)

    org = Organization(name="Acme", slug=f"acme-1397-{tmp_path.name[-8:]}")
    db_session.add(org)
    db_session.flush()
    destination_config = _destination_config(clickhouse)
    src = Connection(
        org_id=org.id,
        name="widget-files",
        connection_type=ConnectionType.CSV,
        direction=ConnectionDirection.SOURCE,
        config_encrypted=encryption.encrypt({"bucket_url": str(drop)}),
    )
    dst = Connection(
        org_id=org.id,
        name="Click Warehouse",
        connection_type=ConnectionType.CLICKHOUSE,
        direction=ConnectionDirection.DESTINATION,
        config_encrypted=encryption.encrypt(destination_config),
    )
    db_session.add_all([src, dst])
    db_session.flush()
    upload = Upload(
        org_id=org.id,
        name=UPLOAD_NAME,
        source_connection_id=src.id,
        destination_connection_id=dst.id,
        dlt_config=form_config("csv", form_file_glob="widgets.csv"),
        status=UploadStatus.DRAFT,
    )
    db_session.add(upload)
    db_session.flush()

    run = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    run_upload(run.id, org.id, session=db_session, encryption=encryption)
    db_session.refresh(run)
    if run.status != RunStatus.SUCCESS or run.rows_loaded != len(WIDGETS):
        raise RuntimeError(
            f"the load itself did not succeed, so nothing below is about the catalogue: "
            f"{run.status} {run.rows_loaded} {run.error_message}"
        )
    entries = (
        db_session.execute(
            select(CatalogEntry)
            .where(CatalogEntry.org_id == org.id)
            .order_by(CatalogEntry.table_name)
        )
        .scalars()
        .all()
    )
    return {
        "org": org,
        "run": db_session.get(Run, run.id),
        "entries": entries,
        "destination_config": destination_config,
        "source_yml": Path(settings.dbt_projects_dir)
        / f"tenant_{org.id}"
        / "models"
        / "click_warehouse_src.yml",
    }


def test_an_upload_into_clickhouse_is_catalogued_under_the_name_that_queries_it(loaded):
    """Every acceptance criterion on one load, each judged separately and reported together.

    One load rather than one per criterion: the server is real, and each load costs a run. Every
    criterion is evaluated even when an earlier one fails, so a red names all of them.
    """
    failures: list[str] = []

    # AC1 and AC3: the user's table only, under the database and the prefixed name.
    named = [(e.schema_name, e.table_name, e.dataset_name) for e in loaded["entries"]]
    if named != [(DATABASE, LOADED_TABLE, DATASET)]:
        failures.append(f"AC1/AC3 catalogue entries: {named}")

    logs = loaded["run"].logs or ""
    if "catalog sync failed" in logs or "no tables were found" in logs:
        failures.append("the run log carries a catalogue warning: " + logs[-400:])

    entry = next((e for e in loaded["entries"] if e.table_name == LOADED_TABLE), None)
    if entry is not None:
        # AC2: its columns, and a preview through the entry's own names.
        names = {c["name"] for c in entry.columns or []}
        if not {"id", "name", "price"} <= names:
            failures.append(f"AC2 columns: {entry.columns}")
        try:
            columns, rows = ConnectionService.preview_table(
                loaded["destination_config"],
                ConnectionType.CLICKHOUSE,
                entry.table_name,
                schema=entry.schema_name,
            )
            got = sorted(
                (r["id"], r["name"], r["price"])
                for r in (dict(zip(columns, row, strict=True)) for row in rows)
            )
            if got != WIDGETS:
                failures.append(f"AC2 preview rows: {got}")
        except Exception as exc:  # noqa: BLE001 - a raised preview is a failed criterion
            failures.append(f"AC2 preview raised {type(exc).__name__}: {exc}")

    # AC5: what GET /api/v1/connections/{id}/tables returns.
    try:
        tables = ConnectionService.list_tables(
            loaded["destination_config"], ConnectionType.CLICKHOUSE
        )
        if {"schema": DATABASE, "name": LOADED_TABLE} not in tables:
            failures.append(f"AC5 list_tables: {tables}")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"AC5 list_tables raised {type(exc).__name__}: {exc}")

    # AC6: the generated dbt source must name the database and the prefixed table, never a
    # database called after the dataset.
    source_yml = loaded["source_yml"]
    if not source_yml.exists():
        failures.append(f"AC6 no source file at {source_yml.name}")
    else:
        sources = yaml.safe_load(source_yml.read_text())["sources"]
        shaped = [(s["name"], s["schema"], [t["name"] for t in s["tables"]]) for s in sources]
        if shaped != [(DATASET, DATABASE, [LOADED_TABLE])]:
            failures.append(f"AC6 dbt sources: {shaped}")
        else:
            # And dbt itself, the consumer of that file: the source must compile to the table the
            # load wrote, and a model selecting from it must read its rows.
            failures.extend(_dbt_reads_the_source(loaded))

    assert not failures, chr(10).join(failures)


def _dbt_reads_the_source(loaded) -> list[str]:
    from datanika.services.dbt_project import DbtProjectService

    org_id = loaded["org"].id
    dbt = DbtProjectService(settings.dbt_projects_dir)
    dbt.generate_profiles_yml(
        org_id, "clickhouse", loaded["destination_config"], default_schema=DATABASE
    )
    dbt.write_model(
        org_id,
        "widgets_through_source",
        f"select id, name, price from {{{{ source('{DATASET}', '{LOADED_TABLE}') }}}}",
        schema_name=DATABASE,
        materialization="view",
    )
    compiled = dbt.compile_model(org_id, "widgets_through_source")
    if not compiled["success"]:
        return [f"AC6 dbt compile failed: {compiled['logs'][-600:]}"]
    sql = compiled["compiled_sql"] or ""
    if LOADED_TABLE not in sql or DATABASE not in sql:
        return [f"AC6 the source compiled to something else: {sql}"]
    run = dbt.run_model(org_id, "widgets_through_source")
    if not run["success"]:
        return [f"AC6 dbt run failed: {run['logs'][-600:]}"]
    return []

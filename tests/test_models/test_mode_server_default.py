"""``uploads.mode`` / ``pipelines.mode``: the column default must be a value the ORM can load.

core#1391. ``Enum(UploadMode, native_enum=False)`` **without** ``values_callable`` persists a
member's **name**, so the only spellings the mapper accepts are ``ETL`` and ``ELT``. Both columns
declared ``server_default=<Mode>.ETL.value``, which writes the member's **value**, ``etl``.

Only an INSERT that does not name the column ever reaches that default — every application write
goes through the ORM, whose Python-side ``default=<Mode>.ETL`` writes the name. So the rows it
produces come from SQL: a migration, a backfill, a data repair, a support fix. The insert succeeds,
nothing is logged, and the row raises ``LookupError`` the next time anything loads it — in the
worker, before ``run_upload`` can reach ``fail_run``, stranding the run in ``RUNNING``.

These tests build the schema from the **model** (``Base.metadata.create_all``), so they pin the
model's declaration. What the *database* column carries is a migration's business, and is asserted
against real Postgres in ``tests/test_migrations/test_h8i9_mode_server_default.py``.

Each case has a control that names the column explicitly and loads cleanly, so the regression
cannot pass by every insert failing, and cannot fail for a reason other than the default.
"""

import pytest
from sqlalchemy import text

from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.pipeline import Pipeline, PipelineMode
from datanika.models.upload import Upload, UploadMode
from datanika.models.user import Organization


@pytest.fixture
def org_and_connections(db_session):
    org = Organization(name="Acme", slug="acme-1391")
    db_session.add(org)
    db_session.flush()
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
        connection_type=ConnectionType.POSTGRES,
        direction=ConnectionDirection.DESTINATION,
        config_encrypted="x",
    )
    db_session.add_all([src, dst])
    db_session.flush()
    return org.id, src.id, dst.id


def _insert_upload(db_session, ids, *, mode: str | None) -> int:
    org_id, src_id, dst_id = ids
    columns = "org_id, name, source_connection_id, destination_connection_id, dlt_config, status"
    values = ":org, :name, :src, :dst, '{}', 'DRAFT'"
    if mode is not None:
        columns += ", mode"
        values += ", :mode"
    db_session.execute(
        text(f"INSERT INTO uploads ({columns}) VALUES ({values})"),  # noqa: S608 - fixed literals
        {"org": org_id, "name": f"upload-{mode}", "src": src_id, "dst": dst_id, "mode": mode},
    )
    return db_session.execute(text("SELECT max(id) FROM uploads")).scalar_one()


def _insert_pipeline(db_session, ids, *, mode: str | None) -> int:
    org_id, _src_id, dst_id = ids
    columns = "org_id, name, destination_connection_id, command, full_refresh, models, status"
    values = ":org, :name, :dst, 'RUN', 0, '[]', 'DRAFT'"
    if mode is not None:
        columns += ", mode"
        values += ", :mode"
    db_session.execute(
        text(f"INSERT INTO pipelines ({columns}) VALUES ({values})"),  # noqa: S608 - fixed literals
        {"org": org_id, "name": f"pipeline-{mode}", "dst": dst_id, "mode": mode},
    )
    return db_session.execute(text("SELECT max(id) FROM pipelines")).scalar_one()


CASES = [
    pytest.param(Upload, UploadMode, "uploads", _insert_upload, id="uploads.mode"),
    pytest.param(Pipeline, PipelineMode, "pipelines", _insert_pipeline, id="pipelines.mode"),
]


@pytest.mark.parametrize("model,mode_enum,table,insert", CASES)
def test_a_row_that_omits_mode_loads_through_the_orm(
    model, mode_enum, table, insert, db_session, org_and_connections
):
    """The regression. Red before core#1391: ``LookupError: 'etl' is not among the defined enum
    values`` on the load — the exact error QA reproduced from the worker on staging."""
    row_id = insert(db_session, org_and_connections, mode=None)
    db_session.expire_all()

    loaded = db_session.get(model, row_id)

    assert loaded.mode is mode_enum.ETL


@pytest.mark.parametrize("model,mode_enum,table,insert", CASES)
def test_the_default_stores_the_spelling_the_orm_itself_writes(
    model, mode_enum, table, insert, db_session, org_and_connections
):
    """Derived, not restated: whatever the ORM stores for ``ETL`` is what the default must store.

    A default that loads but is spelled differently from what the ORM writes would split one
    value into two spellings in the same column — the ``runs.status`` family, where a lowercase
    SQL filter silently reads 0.
    """
    omitted_id = insert(db_session, org_and_connections, mode=None)
    org_id, src_id, dst_id = org_and_connections
    kwargs = {"org_id": org_id, "name": "written-by-the-orm", "destination_connection_id": dst_id}
    if model is Upload:
        kwargs["source_connection_id"] = src_id
    orm_row = model(**kwargs)
    db_session.add(orm_row)
    db_session.flush()

    stored = dict(
        db_session.execute(
            text(f"SELECT id, mode FROM {table} WHERE id IN (:a, :b)"),  # noqa: S608 - literal
            {"a": omitted_id, "b": orm_row.id},
        ).all()
    )
    assert stored[omitted_id] == stored[orm_row.id], (
        f"{table}.mode: the column default stored {stored[omitted_id]!r} while the ORM stores "
        f"{stored[orm_row.id]!r} for the same member"
    )


@pytest.mark.parametrize("model,mode_enum,table,insert", CASES)
def test_control_a_row_that_names_mode_loads(
    model, mode_enum, table, insert, db_session, org_and_connections
):
    """Control: the same insert with the column named loads, so the fixture is sound and a red
    in the regression above is about the default and nothing else."""
    row_id = insert(db_session, org_and_connections, mode="ELT")
    db_session.expire_all()

    assert db_session.get(model, row_id).mode is mode_enum.ELT

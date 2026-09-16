"""``uploads.mode`` / ``pipelines.mode`` column defaults must be loadable by the ORM (core#1391).

``z6a1b2c3d4e5`` added both columns with ``server_default="etl"``. The mapper is
``Enum(<Mode>, native_enum=False)`` with no ``values_callable``, which stores and loads member
**names** — ``ETL`` / ``ELT`` — so a row that takes the database default is written successfully
and cannot be read back: ``LookupError: 'etl' is not among the defined enum values``. QA reproduced
it on staging from the worker, and measured the default as ``'etl'::character varying`` in
``information_schema`` on **both** staging and production, with **zero** such rows on either.

So the trap is armed and has not fired. Every application write goes through the ORM, whose
Python-side default writes the name; only an INSERT that does not name the column meets the
database default — a migration, a backfill, a data repair, a support fix.

Why this file rather than the model test
----------------------------------------
``tests/test_models/test_mode_server_default.py`` builds the schema with
``Base.metadata.create_all``, which reads the **model**, so it goes green the moment the model
changes whatever the database holds. Only alembic against a real Postgres can say what the
*column* does, and the column is what an out-of-band INSERT meets.

DDL only
--------
The migration changes the two defaults and rewrites no row. QA's census is the evidence that there
is nothing to repair on either deployment we run, and a row holding a lowercase spelling is
unreadable by every deployed version alike, so leaving rows alone cannot make any of them worse.
``test_existing_rows_are_left_alone`` is what stops a DML "tidy-up" arriving later.
"""

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from datanika.models.pipeline import Pipeline, PipelineMode
from datanika.models.upload import Upload, UploadMode
from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "g7h8i9j0k1l2"
THIS_REVISION = "h8i9j0k1l2m3"

TABLES = ("uploads", "pipelines")
MODELS = {"uploads": (Upload, UploadMode), "pipelines": (Pipeline, PipelineMode)}


@pytest.fixture
def at_parent(roundtrip_db_url):
    """A database migrated to the revision immediately before this one."""
    engine = create_engine(roundtrip_db_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    result = _run_alembic(["upgrade", PARENT_REVISION], roundtrip_db_url)
    assert result.returncode == 0, f"could not reach {PARENT_REVISION}: {result.stderr}"
    return engine


def _upgrade(db_url: str) -> None:
    result = _run_alembic(["upgrade", THIS_REVISION], db_url)
    assert result.returncode == 0, f"upgrade to {THIS_REVISION} failed: {result.stderr}"


def _column_default(engine, table: str) -> str | None:
    with engine.begin() as conn:
        return conn.execute(
            text(
                "SELECT column_default FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :t AND column_name = 'mode'"
            ),
            {"t": table},
        ).scalar_one()


def _seed_parents(engine) -> tuple[int, int, int]:
    with engine.begin() as conn:
        org = conn.execute(
            text(
                "INSERT INTO organizations (name, slug) VALUES ('Probe', 'probe-1391') RETURNING id"
            )
        ).scalar_one()
        conns = [
            conn.execute(
                text(
                    "INSERT INTO connections (org_id, name, connection_type, direction, "
                    "config_encrypted) VALUES (:org, :name, 'POSTGRES', :dir, 'x') RETURNING id"
                ),
                {"org": org, "name": name, "dir": direction},
            ).scalar_one()
            for name, direction in (("src", "SOURCE"), ("dst", "DESTINATION"))
        ]
    return org, conns[0], conns[1]


def _insert(engine, table: str, parents, *, name: str, mode: str | None = None) -> int:
    """INSERT at the SQL level. ``mode=None`` OMITS the column, so the database default applies."""
    org, src, dst = parents
    if table == "uploads":
        columns = (
            "org_id, name, source_connection_id, destination_connection_id, dlt_config, status"
        )
        values = ":org, :name, :src, :dst, '{}', 'DRAFT'"
    else:
        columns = "org_id, name, destination_connection_id, command, full_refresh, models, status"
        values = ":org, :name, :dst, 'RUN', false, '[]', 'DRAFT'"
    if mode is not None:
        columns += ", mode"
        values += ", :mode"
    with engine.begin() as conn:
        return conn.execute(
            text(f"INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING id"),  # noqa: S608
            {"org": org, "name": name, "src": src, "dst": dst, "mode": mode},
        ).scalar_one()


def _stored(engine, table: str, row_id: int) -> str:
    with engine.begin() as conn:
        return conn.execute(
            text(f"SELECT mode FROM {table} WHERE id = :id"),  # noqa: S608 - literal table names
            {"id": row_id},
        ).scalar_one()


def _load_through_the_mapper(engine, table: str, row_id: int):
    """Read ``mode`` through the ORM column type — the conversion that raises in the worker."""
    model, _enum = MODELS[table]
    with Session(engine) as session:
        return session.execute(select(model.mode).where(model.id == row_id)).scalar_one()


# ---------------------------------------------------------------------------
# Controls — they attribute the reds below. Without them, "the default loads" and "this file never
# reached the defect" are the same observation.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table", TABLES)
def test_control_the_default_is_lowercase_at_the_parent_revision(at_parent, table):
    default = _column_default(at_parent, table)
    assert default is not None and "'etl'" in default, (
        f"{table}.mode already defaulted to {default!r} at {PARENT_REVISION}, so every assertion "
        "below would pass without this migration doing anything"
    )


@pytest.mark.parametrize("table", TABLES)
def test_control_an_omitting_insert_is_unloadable_at_the_parent_revision(at_parent, table):
    """The defect itself, reproduced against the real column and the real mapper."""
    row_id = _insert(at_parent, table, _seed_parents(at_parent), name="omits-at-parent")

    assert _stored(at_parent, table, row_id) == "etl"
    with pytest.raises(LookupError, match="etl"):
        _load_through_the_mapper(at_parent, table, row_id)


# ---------------------------------------------------------------------------
# Regressions — red before this migration exists.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table", TABLES)
def test_an_omitting_insert_loads_through_the_mapper_after_the_migration(
    at_parent, roundtrip_db_url, table
):
    """The criterion, asserted by writing a row and reading it back — not by reading a catalogue."""
    _upgrade(roundtrip_db_url)
    row_id = _insert(at_parent, table, _seed_parents(at_parent), name="omits-after")

    _model, mode_enum = MODELS[table]
    assert _load_through_the_mapper(at_parent, table, row_id) is mode_enum.ETL


@pytest.mark.parametrize("table", TABLES)
def test_the_column_default_is_the_models_server_default(at_parent, roundtrip_db_url, table):
    """Model and migration must agree, derived from the model rather than restated here.

    Two declarations of one default — ``server_default=`` on the column and ``SET DEFAULT`` in a
    migration — are one edit away from disagreeing, and nothing else compares them: the model
    suite builds its schema with ``create_all`` and never runs alembic.
    """
    _upgrade(roundtrip_db_url)
    model, _enum = MODELS[table]
    declared = model.__table__.c.mode.server_default.arg

    default = _column_default(at_parent, table)
    assert default is not None and f"'{declared}'" in default, (
        f"{table}.mode: the migrated column defaults to {default!r} while the model declares "
        f"server_default={declared!r}"
    )


@pytest.mark.parametrize("table", TABLES)
def test_existing_rows_are_left_alone(at_parent, roundtrip_db_url, table):
    """DDL only. A row the ORM wrote, a row carrying ``ELT``, and a row already holding the
    unloadable spelling all keep exactly what they held — this migration alters a column, never a
    row."""
    parents = _seed_parents(at_parent)
    rows = {
        "ETL": _insert(at_parent, table, parents, name="explicit-etl", mode="ETL"),
        "ELT": _insert(at_parent, table, parents, name="explicit-elt", mode="ELT"),
        "etl": _insert(at_parent, table, parents, name="took-the-old-default"),
    }

    _upgrade(roundtrip_db_url)

    assert {held: _stored(at_parent, table, rid) for held, rid in rows.items()} == {
        "ETL": "ETL",
        "ELT": "ELT",
        "etl": "etl",
    }


def test_the_downgrade_restores_the_old_default_without_rewriting_rows(at_parent, roundtrip_db_url):
    """A rollback re-arms the old default for NEW rows and leaves existing rows alone."""
    _upgrade(roundtrip_db_url)
    parents = _seed_parents(at_parent)
    kept = {table: _insert(at_parent, table, parents, name="before-down") for table in TABLES}

    down = _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url)
    assert down.returncode == 0, f"downgrade failed: {down.stderr}"

    for table in TABLES:
        default = _column_default(at_parent, table)
        assert default is not None and "'etl'" in default, (
            f"{table}.mode default not restored: {default!r}"
        )
        assert _stored(at_parent, table, kept[table]) == "ETL", (
            f"the downgrade rewrote an existing {table} row"
        )

    # And back up again, because a rollback is rarely the last thing that happens.
    _upgrade(roundtrip_db_url)
    for table in TABLES:
        default = _column_default(at_parent, table)
        assert default is not None and "'ETL'" in default

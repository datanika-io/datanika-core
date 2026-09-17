"""An Enum column's default must load through the column's own type — the class of core#1391.

core#1394 is this guard. core#1391 is ``uploads.mode`` and ``pipelines.mode``; core#1393 is
``invitations.status``.

The class
---------
``Enum(SomeEnum, native_enum=False)`` without ``values_callable`` persists member **names**
(``ETL``). A default spelled as the member **value** (``etl``) is accepted by the database — the
column is a ``VARCHAR`` — and raises ``LookupError`` the first time the ORM loads the row. In a
Celery worker that happens while the row is being loaded, before ``fail_run`` can run, so the run
is stranded in ``RUNNING`` (measured on staging, core#1391).

Why two layers
--------------
The default can live in two places, and a guard over one of them is blind to the other:

* **model** — ``mapped_column(server_default=...)``. Read by ``create_all`` and by alembic
  autogenerate, which is how ``z6a1b2c3d4e5`` inherited ``"etl"``.
* **schema** — the column default in the database after ``alembic upgrade head``. Read by every
  INSERT that does not name the column: a migration backfill, a data repair, a support fix.

``invitations.status`` is why the second layer is not redundant: its model declares **no**
``server_default``, and migration ``m2i9j0k1l3f4`` gave the column ``'pending'``. A model-only
guard reads that column as clean.

How "loads" is decided
----------------------
Nothing about the ORM is re-implemented. The default expression is **evaluated by Postgres**
(``SELECT <default>``) and the value is **loaded through the column's own SQLAlchemy type**
(``TextClause.columns``) — the result processor the ORM runs when it reads the row. The model-layer
expression is the one SQLAlchemy's DDL compiler emits, so a bare string, ``sa.text(...)`` and an
enum member are read exactly as the DDL reads them. (core#1048: a guard that understood one
spelling of ``server_default`` passed a new instance written in another.)

The ratchet
-----------
``KNOWN_UNLOADABLE`` lists today's offenders, one entry per ``(layer, table, column)``, so this file
lands green on its own. Two tests hold it:

* a **new** offender fails — that is the guard;
* a **known** entry that now loads also fails, naming the entry to delete — so the change that
  fixes a column must shrink the list in the same PR.

The fix for core#1391 and this guard were built in parallel. **Whichever lands second reconciles
the list**; neither waits for the other. The list only ever shrinks: adding an entry to make a new
offender pass is the one edit this file exists to make visible in review.
"""

from __future__ import annotations

import ast
import enum
import importlib
import pkgutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Column, MetaData, Table, TypeDecorator, create_engine, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

import datanika.models as models_pkg
from datanika.models.base import Base
from datanika.models.invitation import Invitation
from tests.test_migrations.conftest import _run_alembic

MODEL = "model"
SCHEMA = "schema"

#: Today's offenders. **Shrink only.** Each entry names the issue that owns the fix; delete the
#: entry in the PR that makes the column's default load (the stale-entry test will say which).
KNOWN_UNLOADABLE: dict[tuple[str, str, str], str] = {
    (SCHEMA, "invitations", "status"): "core#1393",
}

#: Not a member of any enum in this codebase, and short enough for the narrowest Enum column
#: (``length=10``), so an INSERT storing it fails on the enum and never on the column width.
NOT_A_MEMBER = "qa-no-such"

_PG = postgresql.dialect()

_CATALOGUE_DEFAULT = text(
    "SELECT column_default FROM information_schema.columns "
    "WHERE table_schema = 'public' AND table_name = :table AND column_name = :column"
)

_FIX_HINT = (
    "Store and default the SAME spelling. Either make the default the member NAME (e.g. 'ETL', "
    "which touches no existing row), or give the Enum values_callable=lambda e: [i.value for i "
    "in e] (which changes what is stored, so it needs expand/contract). A schema-layer default "
    "is corrected by a migration — changing the model alone leaves the database armed. Do NOT "
    "add the column to KNOWN_UNLOADABLE to make this pass."
)


# ---------------------------------------------------------------------------
# The census
# ---------------------------------------------------------------------------


def _is_enum(type_) -> bool:
    """An Enum, or a TypeDecorator whose storage type is one (it loads through the same path)."""
    return isinstance(type_, SAEnum) or (
        isinstance(type_, TypeDecorator) and isinstance(type_.impl_instance, SAEnum)
    )


def _enum_columns(metadata: MetaData) -> list[Column]:
    return [
        column
        for table in metadata.sorted_tables
        for column in table.columns
        if _is_enum(column.type)
    ]


def _model_default_sql(column: Column) -> str | None:
    """The DEFAULT expression SQLAlchemy's DDL compiler emits for this column, or None.

    None also covers a server default with no literal to read (``FetchedValue``, ``Identity``,
    ``Computed``): nothing about it can be evaluated, and the schema layer still reads what the
    database actually holds.
    """
    if column.server_default is None:
        return None
    return _PG.ddl_compiler(_PG, None).get_column_default_string(column)


def _load_error(conn: Connection, default_sql: str, column: Column) -> str | None:
    """Evaluate ``default_sql`` in Postgres, then load the value through ``column``'s own type.

    Returns None when it loads, else the text of the ``LookupError`` the ORM would raise on a row
    holding that value. Any other exception propagates: an expression Postgres refuses is a
    finding in its own right, never a pass.

    🚨 **The statement cache is switched off here, and the guard is wrong without that.**
    SQLAlchemy 2.0 keys an ``Enum`` type in its compiled-statement cache as
    ``(Enum, ('length', 20))`` — **no enum class, no members, no ``values_callable``**. So two
    columns whose defaults render to the same SQL (``uploads.mode`` and ``pipelines.mode`` both
    read ``'etl'::character varying``) share one compiled statement, and the second is loaded
    through the **first** column's result processor. Measured: with the cache on, a names-Enum
    then a ``values_callable``-Enum over ``'alpha'`` both RAISE; with it off, RAISE then LOADS.
    Today the two ``mode`` columns agree, so the verdict would happen to be right — until one of
    them is fixed and the other is not, which is exactly the reading this file exists to make.
    ``test_control_the_loader_is_immune_to_the_statement_cache`` pins it.
    """
    statement = text(f"SELECT {default_sql} AS value").columns(value=column.type)
    try:
        conn.execute(statement, execution_options={"compiled_cache": None}).scalar_one()
    except LookupError as exc:
        return str(exc)
    return None


def _model_offenders(conn: Connection, metadata: MetaData) -> dict[tuple[str, str, str], str]:
    offenders = {}
    for column in _enum_columns(metadata):
        default_sql = _model_default_sql(column)
        if default_sql is None:
            continue
        error = _load_error(conn, default_sql, column)
        if error is not None:
            offenders[(MODEL, column.table.name, column.name)] = f"{default_sql} -> {error}"
    return offenders


def _schema_census(
    conn: Connection, metadata: MetaData
) -> tuple[dict[tuple[str, str, str], str], list[str]]:
    """``(offenders, columns absent from the catalogue)`` for every mapped Enum column."""
    offenders, absent = {}, []
    for column in _enum_columns(metadata):
        row = conn.execute(
            _CATALOGUE_DEFAULT, {"table": column.table.name, "column": column.name}
        ).one_or_none()
        if row is None:
            absent.append(f"{column.table.name}.{column.name}")
            continue
        if row.column_default is None:
            continue
        error = _load_error(conn, row.column_default, column)
        if error is not None:
            offenders[(SCHEMA, column.table.name, column.name)] = f"{row.column_default} -> {error}"
    return offenders, absent


def _format(entries) -> str:
    return "\n".join(f"  {entry}" for entry in sorted(entries))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def metadata() -> MetaData:
    """``Base.metadata`` with **every** module under ``datanika/models/`` imported.

    Walked with ``pkgutil`` rather than trusting ``datanika/models/__init__.py``: a hand-written
    import list once omitted four models (see ``test_env_metadata_completeness.py``), and a column
    that is never imported is a column this guard would never read.
    """
    package = Path(models_pkg.__file__).parent
    for info in pkgutil.iter_modules([str(package)]):
        importlib.import_module(f"datanika.models.{info.name}")
    return Base.metadata


@pytest.fixture(scope="module")
def pg(roundtrip_db_url) -> Iterator[Engine]:
    """Any Postgres connection — the model layer only evaluates expressions."""
    engine = create_engine(roundtrip_db_url)
    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def migrated(pg, roundtrip_db_url) -> Engine:
    """A database built by ``alembic upgrade head``: the schema production runs, not create_all."""
    with pg.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    result = _run_alembic(["upgrade", "head"], roundtrip_db_url)
    assert result.returncode == 0, f"alembic upgrade head failed:\n{result.stderr[-3000:]}"
    return pg


# ---------------------------------------------------------------------------
# 1. The guard
# ---------------------------------------------------------------------------


def test_no_new_model_default_fails_to_load(pg, metadata):
    with pg.connect() as conn:
        offenders = _model_offenders(conn, metadata)
    new = {key: detail for key, detail in offenders.items() if key not in KNOWN_UNLOADABLE}
    assert not new, (
        f"{len(new)} model server_default(s) cannot be loaded by their own column type:\n"
        + "\n".join(f"  {key}: {detail}" for key, detail in sorted(new.items()))
        + f"\n\n{_FIX_HINT}"
    )


def test_no_new_schema_default_fails_to_load(migrated, metadata):
    with migrated.connect() as conn:
        offenders, _ = _schema_census(conn, metadata)
    new = {key: detail for key, detail in offenders.items() if key not in KNOWN_UNLOADABLE}
    assert not new, (
        f"{len(new)} column default(s) in the MIGRATED schema cannot be loaded by their own "
        "column type — every INSERT that does not name the column stores a row the ORM "
        "cannot read:\n"
        + "\n".join(f"  {key}: {detail}" for key, detail in sorted(new.items()))
        + f"\n\n{_FIX_HINT}"
    )


def test_no_known_entry_has_gone_stale(pg, migrated, metadata):
    """The ratchet's other half: a fixed column must leave the list in the PR that fixed it."""
    with pg.connect() as conn:
        measured = set(_model_offenders(conn, metadata))
    with migrated.connect() as conn:
        measured |= set(_schema_census(conn, metadata)[0])
    stale = set(KNOWN_UNLOADABLE) - measured
    assert not stale, (
        "these KNOWN_UNLOADABLE entries now LOAD — delete them from the dict at the top of this "
        f"file, in the PR that fixed them:\n{_format(stale)}\n\n"
        "If this PR is the guard or the fix and the other one merged first, this is the "
        "reconciliation the ratchet exists to force: the list only ever shrinks. A misspelled "
        "table or column also lands here, because it can never be measured."
    )


# ---------------------------------------------------------------------------
# 2. Arming — the census must be reading something
# ---------------------------------------------------------------------------


def test_the_census_reads_every_mapped_enum_column(migrated, metadata):
    columns = _enum_columns(metadata)
    assert len(columns) >= 15, (
        f"only {len(columns)} mapped Enum columns (22 on 2026-09-16) — the model import is "
        "broken, and every assertion above would pass by reading nothing"
    )
    with migrated.connect() as conn:
        _, absent = _schema_census(conn, metadata)
    assert not absent, (
        f"{len(absent)} of {len(columns)} mapped Enum columns are not in the migrated catalogue:\n"
        f"{_format(absent)}\n\nEither the catalogue query is reading the wrong schema — in which "
        "case the schema-layer guard reads nothing — or a model has no migration "
        "(test_migration_coverage.py)."
    )


def _enum_columns_by_source() -> set[tuple[str, str]]:
    """``(table, column)`` for every ``mapped_column(Enum(...))`` in ``datanika/models/*.py``,
    read from SOURCE: a second derivation that no import order, registry or ``_is_enum`` rule
    can narrow.
    """
    found: set[tuple[str, str]] = set()
    for path in sorted(Path(models_pkg.__file__).parent.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for cls in (node for node in tree.body if isinstance(node, ast.ClassDef)):
            table = next(
                (
                    stmt.value.value
                    for stmt in cls.body
                    if isinstance(stmt, ast.Assign)
                    and isinstance(stmt.value, ast.Constant)
                    and any(
                        isinstance(t, ast.Name) and t.id == "__tablename__" for t in stmt.targets
                    )
                ),
                None,
            )
            if table is None:
                continue
            for stmt in cls.body:
                if not (isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)):
                    continue
                call = stmt.value
                if not (
                    isinstance(call, ast.Call) and getattr(call.func, "id", None) == "mapped_column"
                ):
                    continue
                first = call.args[0] if call.args else None
                if isinstance(first, ast.Call) and getattr(first.func, "id", None) == "Enum":
                    found.add((table, stmt.target.id))
    return found


def test_the_census_population_agrees_with_the_source(metadata):
    """QA_RULES §2a: two independent derivations of "every Enum column core maps", as sets.

    The census reads ``Base.metadata`` through ``_is_enum`` — a population set by what was imported
    and by what the type check recognises. The floor above catches an EMPTY census; only a second
    derivation catches a SUBSET, which is non-empty and looks healthy. Metadata columns on tables no
    core model file declares (a cloud model imported elsewhere in a session) are left out of the
    comparison; they are still censused.
    """
    by_source = _enum_columns_by_source()
    # The filter must NOT come from the source walk: filtering one side by the other side's output
    # makes a subset on that side agree with itself. Measured: narrowing the walk to [a-r]*.py
    # passed until the filter was taken from the mapper registry instead.
    core_tables = {
        mapper.local_table.name
        for mapper in Base.registry.mappers
        if mapper.class_.__module__.startswith("datanika.models.")
    }
    by_metadata = {
        (column.table.name, column.name)
        for column in _enum_columns(metadata)
        if column.table.name in core_tables
    }
    assert by_source, "the source walk found no Enum columns — it has stopped working"
    assert by_metadata == by_source, (
        f"only in the source: {sorted(by_source - by_metadata)}\n"
        f"only in the metadata: {sorted(by_metadata - by_source)}\n"
        "The census reads the metadata, so a column only in the source is one this guard never "
        "examines — most likely a column type _is_enum does not recognise."
    )


# ---------------------------------------------------------------------------
# 3. Controls that can fail in the direction of the conclusion
# ---------------------------------------------------------------------------


class _Planted(enum.StrEnum):
    ALPHA = "alpha"
    BETA = "beta"


class _PlantedDecorated(TypeDecorator):
    impl = SAEnum(_Planted, native_enum=False, length=20)
    cache_ok = True


_BY_NAME = SAEnum(_Planted, native_enum=False, length=20)
_BY_VALUE = SAEnum(
    _Planted, native_enum=False, length=20, values_callable=lambda e: [i.value for i in e]
)


#: ``names-bare-value-string`` is core#1391's shape; ``names-text-clause`` is core#1048's lesson
#: (a guard that read one spelling of ``server_default`` passed an instance written in another).
@pytest.mark.parametrize(
    ("column_type", "server_default", "loads"),
    [
        pytest.param(_BY_NAME, "alpha", False, id="names-bare-value-string"),
        pytest.param(_BY_NAME, text("'alpha'"), False, id="names-text-clause"),
        pytest.param(_BY_NAME, _Planted.ALPHA, False, id="names-enum-member"),
        pytest.param(_PlantedDecorated(), "alpha", False, id="typedecorator-over-enum"),
        pytest.param(_BY_VALUE, "ALPHA", False, id="values_callable-name-string"),
        pytest.param(_BY_NAME, "ALPHA", True, id="names-name-string"),
        pytest.param(_BY_VALUE, "alpha", True, id="values_callable-value-string"),
    ],
)
def test_control_a_planted_model_column_is_judged_correctly(pg, column_type, server_default, loads):
    """A third column, planted in a throwaway MetaData, in every spelling this guard must read.

    The two ``loads=True`` cases are the half that can fail in the direction of this file's
    verdict: a detector that flagged every Enum default would pass the other five.
    """
    planted = MetaData()
    Table("qa_planted", planted, Column("mode", column_type, server_default=server_default))
    with pg.connect() as conn:
        offenders = _model_offenders(conn, planted)
    assert ((MODEL, "qa_planted", "mode") not in offenders) is loads, offenders


def test_control_the_loader_is_immune_to_the_statement_cache(pg):
    """Same SQL, two Enum types of equal length, opposite stored spellings, one engine.

    SQLAlchemy 2.0.46 caches both under one key (see ``_load_error``), and the cache belongs to
    the engine, so a loader that let it in would return the first type's verdict for the second.
    Both orders are run, so this cannot pass by happening to evaluate the right type first. It
    asserts the loader's verdicts, not the cache key: if a later SQLAlchemy keys the types apart
    this stays green, as it should.
    """
    by_name, by_value = Column("v", _BY_NAME), Column("v", _BY_VALUE)
    with pg.connect() as conn:
        name_first = (_load_error(conn, "'alpha'", by_name), _load_error(conn, "'alpha'", by_value))
    with pg.connect() as conn:
        value_first = (
            _load_error(conn, "'alpha'", by_value),
            _load_error(conn, "'alpha'", by_name),
        )
    assert name_first[0] is not None and name_first[1] is None, name_first
    assert value_first[0] is None and value_first[1] is not None, value_first


def test_control_the_schema_census_reads_the_database_not_the_model(migrated, metadata):
    """Plant an offender in the migrated database, then correct it, inside one rolled-back
    transaction. The model is untouched throughout, so the census can only follow the change
    by reading the catalogue — and the corrected reading is what proves a KNOWN entry can go
    stale, i.e. that the stale-entry test above is live rather than decorative."""
    runs_status = metadata.tables["runs"].c.status
    good_spelling = runs_status.type.enums[0]
    key = (SCHEMA, "runs", "status")
    with migrated.connect() as conn, conn.begin() as outer:
        conn.execute(text(f"ALTER TABLE runs ALTER COLUMN status SET DEFAULT '{NOT_A_MEMBER}'"))
        planted, _ = _schema_census(conn, metadata)
        conn.execute(text(f"ALTER TABLE runs ALTER COLUMN status SET DEFAULT '{good_spelling}'"))
        corrected, _ = _schema_census(conn, metadata)
        outer.rollback()
    assert key in planted, f"a planted unloadable default on runs.status was not caught: {planted}"
    assert key not in corrected, f"a default of {good_spelling!r} was reported unloadable"


@pytest.mark.parametrize("spelling", ["unloadable", "loadable"])
def test_control_the_census_agrees_with_the_orm_on_an_insert_that_omits_the_column(
    migrated, metadata, spelling
):
    """The census is a proxy for "the ORM cannot read a row this default wrote". Check the proxy
    against the real thing, in both directions, independently of today's defaults: set the
    column default, INSERT an invitation without naming ``status``, and load it through the ORM.
    """
    status = metadata.tables["invitations"].c.status
    role = metadata.tables["invitations"].c.role
    default = NOT_A_MEMBER if spelling == "unloadable" else status.type.enums[0]
    with migrated.connect() as conn, conn.begin() as outer:
        conn.execute(text(f"ALTER TABLE invitations ALTER COLUMN status SET DEFAULT '{default}'"))
        offenders, _ = _schema_census(conn, metadata)
        census_says_loads = (SCHEMA, "invitations", "status") not in offenders

        org = conn.execute(
            text("INSERT INTO organizations (name, slug) VALUES ('qa', 'qa-1394') RETURNING id")
        ).scalar_one()
        user = conn.execute(
            text("INSERT INTO users (password_hash, is_active) VALUES ('x', true) RETURNING id")
        ).scalar_one()
        invitation = conn.execute(
            text(
                "INSERT INTO invitations (org_id, role, invited_by_user_id, expires_at) "
                "VALUES (:org, :role, :user, now()) RETURNING id"
            ),
            {"org": org, "role": role.type.enums[0], "user": user},
        ).scalar_one()
        stored = conn.execute(
            text("SELECT status FROM invitations WHERE id = :id"), {"id": invitation}
        ).scalar_one()
        try:
            with Session(bind=conn) as session:
                session.get(Invitation, invitation)
            orm_loads = True
        except LookupError:
            orm_loads = False
        outer.rollback()

    assert stored == default, f"the INSERT stored {stored!r}, not the default {default!r}"
    assert orm_loads is (spelling == "loadable"), (
        f"the ORM {'loaded' if orm_loads else 'refused'} a row holding {stored!r}"
    )
    assert census_says_loads is orm_loads, (
        f"the census says the default {'loads' if census_says_loads else 'does not load'}, and "
        f"the ORM {'loaded' if orm_loads else 'refused'} the row it wrote"
    )

"""MySQL after ``dbt-mysql`` is dropped: what goes, and what must NOT go (core#825).

``dbt-mysql`` was removed because it is abandoned — last release 1.7.0, published
2024-04-26 from a personal repository, declaring ``dbt-core~=1.7.0`` — and that one
package held the whole dbt stack on 1.7, which in turn blocked six packages'
CRITICAL/HIGH advisories including ``cryptography`` (the library
``EncryptionService`` uses on customer warehouse credentials) and a CRITICAL RCE in
the Redshift driver. Founder decision, 2026-08-31.

**The removal is narrow, and this file exists because the narrowness is the risk.**
MySQL has three independent roles here:

===========================  ==============  =================================
role                         status          path
===========================  ==============  =================================
extract source               **KEPT**        dlt ``sql_database``/``sql_table``
                                             → SQLAlchemy → ``pymysql``
dbt transformation target    **GONE**        was ``dbt-mysql``; no replacement
dlt load destination         **WITHDRAWN**   never worked; advertised until
                                             core#862 removed it (core#865)
===========================  ==============  =================================

🚨 **That third row is a correction to core#825, to this file's first draft, and
to the landing docs.** All three said MySQL "stays a load destination". It does
not, and it never did — ``build_destination`` does
``getattr(dlt.destinations, connection_type)`` and dlt has no such attribute, so
every MySQL load has always raised ``AttributeError``. Pre-existing, confirmed
against the pre-change image, filed as core#865.

**It was found by refusing to assert it cheaply.** ``"mysql" in
SUPPORTED_DESTINATION_TYPES`` is true, and a test asserting that would have
passed, "confirmed" the claim, and shipped it onto the marketing site. Only
attempting a real load found it.

A dependency change is exactly the shape of edit that removes more than intended
and reports success, because the roles it does NOT touch are exercised nowhere in
the diff. So this file asserts all three, and the surviving one is asserted by
**moving real rows through a real MySQL server**.

⚠️ **Why set membership is not sufficient for the kept roles.** ``mysql`` appearing
in ``SUPPORTED_SOURCE_TYPES`` proves the dispatch table still lists it, not that a
row can travel. This module's sibling ``test_source_builders_move_rows.py`` exists
because of core#492, where `csv`/`json`/`parquet`/`s3` loaded a **file listing**
instead of file contents while 2,500 tests stayed green — every one of them
asserting the kwargs passed in rather than what dlt yielded. The same trap is
available here: ``pymysql`` is still installed, the dialect string is unchanged,
and the set still contains ``"mysql"`` — so every cheap check passes whether or not
extraction works.
"""

from __future__ import annotations

import pytest

from datanika.services.dbt_project import SUPPORTED_ADAPTERS, DbtProjectError, DbtProjectService
from datanika.services.dlt_runner import DltRunnerError, DltRunnerService
from tests.test_services.test_source_builders_move_rows import (
    _extract_load,
    _rows,
    await_setup,
    requires_docker,
)

MYSQL_IMAGE = "mysql:8.4"


# ── What is GONE ────────────────────────────────────────────────────────────────


class TestMysqlIsNoLongerADbtTransformTarget:
    """The half core#825 actually removes.

    These are the assertions that were RED before the change: ``mysql`` was in
    ``SUPPORTED_ADAPTERS`` and ``_build_profile_output`` emitted a ``type: mysql``
    profile for it.
    """

    def test_mysql_is_not_a_supported_dbt_adapter(self):
        assert "mysql" not in SUPPORTED_ADAPTERS, (
            "mysql is still advertised as a dbt adapter, but no dbt-mysql package "
            "is installed. generate_profiles_yml would write a profiles.yml naming "
            "an adapter dbt cannot load, and the failure would surface at `dbt run` "
            "time inside a Celery task rather than when the user picked the "
            "destination."
        )

    def test_generating_a_mysql_profile_refuses_instead_of_writing_a_dead_profile(self, tmp_path):
        """Refusing is the point: a written-but-unloadable profile fails later and worse."""
        svc = DbtProjectService(str(tmp_path))
        svc.ensure_project(1)
        with pytest.raises(DbtProjectError, match="mysql"):
            svc.generate_profiles_yml(
                1,
                "mysql",
                {"host": "h", "port": 3306, "user": "u", "password": "p", "database": "d"},
            )
        assert not (tmp_path / "tenant_1" / "profiles.yml").exists(), (
            "profiles.yml was written despite the refusal. A dead profile on the "
            "persistent dbt_projects volume outlives the deploy that created it."
        )

    def test_the_dbt_mysql_package_is_genuinely_absent(self):
        """Not just unlisted — uninstalled.

        Asserted by import rather than by reading metadata: a distribution can be
        registered and hollow (WORKFLOW_RULES §3 — ``metadata.version()`` returned
        the correct version for a package missing 69 of its 374 files). Here the
        direction that matters is the opposite one, but the lesson is the same:
        ask the thing you actually depend on.
        """
        with pytest.raises(ImportError):
            __import__("dbt.adapters.mysql")


# ── What must NOT go: MySQL as a SOURCE ─────────────────────────────────────────


def _mysql_url(container) -> str:
    """A SQLAlchemy URL naming the driver WE actually install.

    🚨 Do not use ``container.get_connection_url()``. testcontainers returns a
    bare ``mysql://`` URL, which SQLAlchemy resolves to ``MySQLDialect_mysqldb``
    and therefore to ``MySQLdb`` (the ``mysqlclient`` C extension) — a package
    this project does not install and never has. The test then dies in its own
    seeding helper with ``ModuleNotFoundError``, which reads exactly like "MySQL
    extraction is broken" and is nothing of the kind.

    Our production mapping is correct and unaffected:
    ``dlt_runner.SOURCE_DRIVERNAME_MAP["mysql"] == "mysql+pymysql"``. That is
    also why this must be spelled out rather than fixed by installing
    ``mysqlclient`` — the harness has to exercise the driver that ships.
    """
    return (
        f"mysql+pymysql://{container.username}:{container.password}"
        f"@{container.get_container_host_ip()}:{container.get_exposed_port(3306)}"
        f"/{container.dbname}"
    )


def _seed_mysql(container) -> dict:
    """Create the fixture table and return the config shape ConnectionState saves."""
    import sqlalchemy

    engine = sqlalchemy.create_engine(_mysql_url(container))
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("CREATE TABLE widgets (id int, name varchar(32), price int)"))
        conn.execute(
            sqlalchemy.text(
                "INSERT INTO widgets VALUES (1,'alpha',100),(2,'beta',200),(3,'gamma',300)"
            )
        )
    engine.dispose()
    # Note `user`, not `username` — `_to_dlt_credentials` performs that rename and
    # mysql is in `_RENAME_USER_TYPES`. Passing the stored shape is what exercises it.
    return {
        "host": container.get_container_host_ip(),
        "port": int(container.get_exposed_port(3306)),
        "user": container.username,
        "password": container.password,
        "database": container.dbname,
    }


@requires_docker
class TestMysqlStillMovesRowsAsASource:
    """The founder's explicit condition on core#825, asserted by measurement.

    Both dlt modes are covered because they call different functions —
    ``full_database`` → ``sql_database()``, ``single_table`` → ``sql_table()`` —
    exactly as ``TestSqlDatabaseSourceMovesRows`` does for postgres.

    The assertion reads the **destination** back. Never ``result["rows_loaded"]``,
    which is the pipeline's own report of its own work.
    """

    def test_mysql_is_still_a_declared_source_type(self):
        assert "mysql" in DltRunnerService.SUPPORTED_SOURCE_TYPES

    def test_full_database_mode_moves_rows(self, tmp_path):
        from testcontainers.mysql import MySqlContainer

        with MySqlContainer(MYSQL_IMAGE) as mysql:
            config = await_setup(
                "mysql accepting writes",
                lambda: _seed_mysql(mysql),
                container=mysql,
            )
            db_path = _extract_load(
                tmp_path,
                "mysql",
                config,
                {"mode": "full_database", "table_names": ["widgets"]},
            )
            rows = _rows(db_path, "widgets")

        assert rows == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "sql_database() did not deliver MySQL row CONTENTS into DuckDB. "
            "core#825 dropped the dbt-mysql adapter and must not have touched "
            "extraction, which goes through SQLAlchemy/pymysql and never through dbt."
        )

    def test_single_table_mode_moves_rows(self, tmp_path):
        from testcontainers.mysql import MySqlContainer

        with MySqlContainer(MYSQL_IMAGE) as mysql:
            config = await_setup(
                "mysql accepting writes",
                lambda: _seed_mysql(mysql),
                container=mysql,
            )
            db_path = _extract_load(
                tmp_path,
                "mysql",
                config,
                {"mode": "single_table", "table": "widgets"},
            )
            rows = _rows(db_path, "widgets")

        assert rows == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "sql_table() did not deliver MySQL row contents into DuckDB."
        )


# ── What must NOT go: MySQL as a LOAD DESTINATION ───────────────────────────────


class TestMysqlIsNotALoadDestinationAndIsNoLongerAdvertisedAsOne:
    """🚨 MySQL as a dlt LOAD destination has NEVER worked. Found by core#825,
    tracked as core#865, **withdrawn from the product in core#862**.

    This class was written to prove the "kept" role and instead disproved it,
    which is why it exists rather than a set-membership assertion:
    ``"mysql" in SUPPORTED_DESTINATION_TYPES`` was true, and it meant nothing.

    ``build_destination`` did ``getattr(dlt.destinations, connection_type)``.
    **dlt has no ``mysql`` attribute** — measured against dlt 1.21.0, and equally
    absent from the pre-change image, so core#825 neither caused this nor was it
    a regression from the dbt move. dlt's generic ``sqlalchemy`` destination is
    what would serve MySQL, and nothing maps to it.

    ``sqlite`` had the identical defect. Same shape as core#845, where Redshift's
    ``SOURCE_DRIVERNAME_MAP`` named a SQLAlchemy dialect we do not ship.

    🔑 **These tests were written asserting the DEFECT, with the instruction that
    the day it was fixed they should go red and be rewritten into the positive
    form.** core#862 is that day and this is that rewrite — kept as one class
    rather than deleted, because the history of a capability that was advertised
    for years and never worked is the most useful thing in the file. A broken
    capability with no failing test is one nobody is told about; a fixed one with
    no test is one that comes back.

    The SOURCE half is unaffected and IS verified, by
    ``TestMysqlStillMovesRowsAsASource`` above, against a real MySQL server.
    """

    def test_mysql_is_no_longer_advertised_as_a_destination(self):
        """The advertisement is gone (core#862)."""
        assert "mysql" not in DltRunnerService.SUPPORTED_DESTINATION_TYPES

    def test_and_dlt_still_has_no_mysql_destination(self):
        """The reason. Needs no container — it fails before a socket is opened.

        Kept pointing at dlt rather than at our own set, because the day dlt
        ships a MySQL destination this is the assertion that should go red and
        tell somebody the capability can be restored. A test of our set alone
        would stay green forever and nobody would ever look again.
        """
        import dlt.destinations

        assert not hasattr(dlt.destinations, "mysql"), (
            "dlt.destinations now HAS a `mysql` attribute, so the defect behind "
            "core#865 may be fixed upstream. Run a REAL load into MySQL, and if "
            "rows land, re-add `mysql` to SUPPORTED_DESTINATION_TYPES and to "
            "DESTINATION_TYPES, restore the positive test, and update the "
            "landing connector entry back to direction: both."
        )

        runner = DltRunnerService(pipelines_dir="unused")
        with pytest.raises(DltRunnerError, match="Unsupported destination type: mysql"):
            runner.build_destination(
                "mysql",
                {"host": "h", "port": 3306, "user": "u", "password": "p", "database": "d"},
            )

    def test_sqlite_had_the_identical_defect_and_the_identical_fix(self):
        """Recorded here rather than filed separately — one cause, one fix."""
        import dlt.destinations

        assert "sqlite" not in DltRunnerService.SUPPORTED_DESTINATION_TYPES
        assert not hasattr(dlt.destinations, "sqlite")

    def test_every_advertised_destination_now_really_exists(self):
        """The discriminating control, and the assertion this class exists for now.

        Before core#862 this read ``broken == ["mysql", "sqlite"]`` — the defect
        asserted, so that fixing it would go red and force this rewrite. It did.
        Nine of eleven resolving was what made those two failures a fact about
        those connectors rather than about this test; ``broken == []`` over a set
        of nine is the same control pointing the other way.
        """
        import dlt.destinations

        broken = sorted(
            t
            for t in DltRunnerService.SUPPORTED_DESTINATION_TYPES
            if not hasattr(dlt.destinations, t)
        )
        assert broken == [], (
            f"advertised dlt destinations with no factory: {broken}. Every load "
            "into one of these raises AttributeError inside a Celery task, after "
            "the run row and the quota charge already exist."
        )
        assert len(DltRunnerService.SUPPORTED_DESTINATION_TYPES) == 9, (
            "the destination set changed size. Nine is what dlt 1.21.0 provides; "
            "re-measure with `hasattr(dlt.destinations, t)` rather than adjusting "
            "this number to match."
        )

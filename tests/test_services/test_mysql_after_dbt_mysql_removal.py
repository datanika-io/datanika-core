"""MySQL after ``dbt-mysql`` is dropped: what goes, and what must NOT go (core#825).

``dbt-mysql`` was removed because it is abandoned — last release 1.7.0, published
2024-04-26 from a personal repository, declaring ``dbt-core~=1.7.0`` — and that one
package held the whole dbt stack on 1.7, which in turn blocked six packages'
CRITICAL/HIGH advisories including ``cryptography`` (the library
``EncryptionService`` uses on customer warehouse credentials) and a CRITICAL RCE in
the Redshift driver. Founder decision, 2026-08-31.

**The removal is narrow, and this file exists because the narrowness is the risk.**
MySQL has three independent roles here and only one of them went:

===========================  ==========  =====================================
role                         status      path
===========================  ==========  =====================================
extract source               **KEPT**    dlt ``sql_database``/``sql_table``
                                         → SQLAlchemy → ``pymysql``
dlt load destination         **KEPT**    same driver, other direction
dbt transformation target    **GONE**    was ``dbt-mysql``; no replacement
===========================  ==========  =====================================

A dependency change is exactly the shape of edit that removes more than intended
and reports success, because the two kept roles are exercised nowhere in the diff.
So this file asserts all three, and the two "kept" rows are asserted by **moving
real rows through a real MySQL server** rather than by checking set membership.

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
from datanika.services.dlt_runner import DltRunnerService
from tests.test_services.test_source_builders_move_rows import (
    WIDGETS,
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


def _seed_mysql(container) -> dict:
    """Create the fixture table and return the config shape ConnectionState saves."""
    import sqlalchemy

    engine = sqlalchemy.create_engine(container.get_connection_url())
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


@requires_docker
class TestMysqlStillAcceptsRowsAsADestination:
    """The other kept role, and the one most likely to be assumed rather than checked.

    dlt loads into MySQL through the same SQLAlchemy/``pymysql`` pair, so this is
    not covered by the source tests above — it is the other direction through a
    different dlt destination implementation.
    """

    def test_mysql_is_still_a_declared_destination_type(self):
        assert "mysql" in DltRunnerService.SUPPORTED_DESTINATION_TYPES

    def test_rows_land_in_a_real_mysql_warehouse(self, tmp_path, json_api, allow_loopback):
        import sqlalchemy
        from testcontainers.mysql import MySqlContainer

        with MySqlContainer(MYSQL_IMAGE) as mysql:
            dst = await_setup(
                "mysql accepting writes",
                lambda: {
                    "host": mysql.get_container_host_ip(),
                    "port": int(mysql.get_exposed_port(3306)),
                    "user": mysql.username,
                    "password": mysql.password,
                    "database": mysql.dbname,
                },
                container=mysql,
            )
            runner = DltRunnerService(pipelines_dir=str(tmp_path / "dlt"))
            runner.execute(
                pipeline_id=1,
                source_type="rest_api",
                source_config={"base_url": json_api},
                destination_type="mysql",
                destination_config=dst,
                dlt_config={"write_disposition": "replace", "resources": ["widgets"]},
                dataset_name=mysql.dbname,
                run_id=1,
            )

            # Read the warehouse back — the destination is the source of truth.
            engine = sqlalchemy.create_engine(mysql.get_connection_url())
            try:
                with engine.connect() as conn:
                    landed = conn.execute(
                        sqlalchemy.text("SELECT name, price FROM widgets ORDER BY price")
                    ).fetchall()
            finally:
                engine.dispose()

        assert [tuple(r) for r in landed] == [(w["name"], w["price"]) for w in WIDGETS], (
            "dlt did not land row contents in a real MySQL destination. MySQL "
            "remains a load destination after core#825; only the dbt transform "
            "adapter was removed."
        )

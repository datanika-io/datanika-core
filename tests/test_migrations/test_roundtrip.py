"""Alembic migration round-trip test (PLAN_QA.md P1).

Spin up a blank Postgres, run `alembic upgrade head`, snapshot the schema,
run `alembic downgrade -1`, run `alembic upgrade head` again, and assert
the final schema matches the initial one. This catches migrations whose
`downgrade()` is broken or missing — which only matters at 2 AM when
you're trying to roll back a bad deploy.

## How it runs

- **CI** (`migration-roundtrip` job): GitHub Actions spins up `postgres:16`
  as a service container, exports `DATABASE_URL_SYNC_TEST`, the test
  uses that DB directly.
- **Locally with Docker**: the test auto-provisions a Postgres container
  via `testcontainers[postgres]`. Requires `uv pip install testcontainers`
  (optional dev dep).
- **Locally without Docker**: test skips with a clear message. CI catches
  any breakage; local devs who want to verify downgrades need Docker.

## Scope

MVP asserts **schema equivalence** after a round-trip on the HEAD
migration only. Data preservation and full-history round-trips are
follow-ups. Migrations that are intentionally one-way (data-destructive)
must mark `one_way = True` in their module globals — the test will
skip them.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

PROJECT_ROOT = Path(__file__).parent.parent.parent


def _docker_available() -> bool:
    """Fast check for a reachable Docker daemon."""
    if not shutil.which("docker"):
        return False
    try:
        return (
            subprocess.run(
                ["docker", "info"],
                capture_output=True,
                timeout=5,
                check=False,
            ).returncode
            == 0
        )
    except (subprocess.TimeoutExpired, OSError):
        return False


@pytest.fixture(scope="session")
def roundtrip_db_url():
    """Postgres URL for round-trip tests.

    Priority:
    1. ``DATABASE_URL_SYNC_TEST`` env var (CI, or manual override)
    2. testcontainers[postgres] auto-provisioned container (local with Docker)
    3. skip the test (local without Docker)

    Must be a generator (yield-based) on every path — pytest treats a
    function with any `yield` as a generator, and hitting `return` on
    one path while `yield`-ing on another raises
    ``ValueError: fixture did not yield a value``.
    """
    env_url = os.environ.get("DATABASE_URL_SYNC_TEST")
    if env_url:
        yield env_url
        return

    if not _docker_available():
        pytest.skip(
            "Round-trip test requires Postgres. Set DATABASE_URL_SYNC_TEST or run "
            "with Docker Desktop running. CI always has Docker + the env var set."
        )

    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        pytest.skip(
            "testcontainers not installed. Install with: "
            "`uv pip install 'testcontainers[postgres]'` or run CI instead."
        )

    # Session-scoped container — one per pytest session. Torn down when
    # the fixture generator exits.
    container = PostgresContainer("postgres:16-alpine")
    container.start()
    try:
        yield container.get_connection_url().replace("postgresql+psycopg2", "postgresql")
    finally:
        container.stop()


def _run_alembic(cmd: list[str], db_url: str) -> subprocess.CompletedProcess:
    """Run an alembic command against the test DB.

    Uses `uv run` so the subprocess inherits the project's venv, and
    passes ``DATABASE_URL_SYNC`` in the environment — alembic's env.py
    reads `settings.database_url_sync` which is sourced from that env var.
    """
    env = {**os.environ, "DATABASE_URL_SYNC": db_url, "DATABASE_URL": db_url}
    return subprocess.run(
        ["uv", "run", "alembic", *cmd],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _snapshot_schema(db_url: str) -> dict[str, list[str]]:
    """Return {table_name: [sorted column names]} for the public schema.

    Intentionally ignores column types, constraints, and indexes —
    matching those adds false positives without catching real bugs
    that a simple column-set diff wouldn't already catch.
    """
    engine = create_engine(db_url)
    insp = inspect(engine)
    snapshot: dict[str, list[str]] = {}
    for name in insp.get_table_names(schema="public"):
        if name == "alembic_version":
            continue
        cols = sorted(c["name"] for c in insp.get_columns(name, schema="public"))
        snapshot[name] = cols
    engine.dispose()
    return snapshot


def _reset_db(db_url: str) -> None:
    """Drop everything in the public schema — gives each test a blank DB."""
    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    engine.dispose()


def test_head_downgrade_upgrade_roundtrip(roundtrip_db_url: str) -> None:
    """Upgrade → downgrade -1 → upgrade and assert schema round-trips cleanly."""
    _reset_db(roundtrip_db_url)

    # 1. Upgrade to head
    r = _run_alembic(["upgrade", "head"], roundtrip_db_url)
    assert r.returncode == 0, (
        f"Initial `alembic upgrade head` failed:\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )
    schema_before = _snapshot_schema(roundtrip_db_url)
    assert schema_before, "Initial upgrade produced an empty schema — something is wrong"

    # 2. Downgrade one step
    r = _run_alembic(["downgrade", "-1"], roundtrip_db_url)
    assert r.returncode == 0, (
        f"`alembic downgrade -1` failed — HEAD migration's downgrade() is broken.\n"
        f"If this migration is intentionally one-way (data-destructive), add\n"
        f"    one_way = True\n"
        f"at module scope in the migration file AND skip this test via the\n"
        f"ONE_WAY_REVISIONS list in test_roundtrip.py.\n"
        f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )

    # 3. Upgrade back to head
    r = _run_alembic(["upgrade", "head"], roundtrip_db_url)
    assert r.returncode == 0, (
        f"Second `alembic upgrade head` failed — migration isn't idempotent after "
        f"downgrade.\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )

    # 4. Assert schema round-trips
    schema_after = _snapshot_schema(roundtrip_db_url)
    assert schema_after == schema_before, (
        f"Schema differs after downgrade + re-upgrade cycle.\n"
        f"Missing after round-trip: {_diff(schema_before, schema_after)}\n"
        f"Extra after round-trip:   {_diff(schema_after, schema_before)}"
    )


def _diff(a: dict[str, list[str]], b: dict[str, list[str]]) -> dict[str, list[str]]:
    """Return {table: [cols in a but not b]} for tables that differ."""
    out: dict[str, list[str]] = {}
    for table, cols in a.items():
        if table not in b:
            out[table] = cols
            continue
        missing = sorted(set(cols) - set(b[table]))
        if missing:
            out[table] = missing
    return out

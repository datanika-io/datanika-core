"""Shared Postgres harness for the migration tests.

**Why this is a conftest and not an import.** ``test_roundtrip.py`` owned
``roundtrip_db_url``, and ``test_expand_contract.py`` reached for it with
``from ...test_roundtrip import roundtrip_db_url``. That looks like sharing and
isn't: pytest registers an imported fixture as a **separate FixtureDef in the
importing module**, so ``scope="session"`` caches once per module rather than
once per session. ``pytest --setup-plan`` showed it plainly::

    SETUP    S roundtrip_db_url      <- for test_expand_contract.py
    SETUP    S roundtrip_db_url      <- for test_roundtrip.py
    TEARDOWN S roundtrip_db_url
    TEARDOWN S roundtrip_db_url

Two setups meant **two Postgres containers per run**, each with its own ryuk and
its own teardown — and on Windows the second teardown timed out against the
Docker named pipe, failing an otherwise green suite in ``TEARDOWN`` (session
``exitstatus=OK, testsfailed=0``) and leaving containers running.

A fixture defined here is discovered by every module in this directory as **one**
FixtureDef: one container, one teardown. Helpers live here too, so neither test
module has to import from the other.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

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


def _no_postgres(reason: str) -> None:
    """Skip locally when Postgres is unavailable — but FAIL in CI.

    The ``migration-roundtrip`` job exists to run these files, so if they skip,
    the job exits 0 and reports green having verified nothing — while the thing
    it guards (a downgrade that only matters at 2 AM during a prod rollback)
    goes unchecked. CI always has both Docker and ``DATABASE_URL_SYNC_TEST``, so
    reaching here in CI means the workflow broke, and that must be loud.

    Locally, skipping stays correct: devs without Docker shouldn't be blocked.
    Same reasoning as the connector smoke suite's strict mode (core#407).
    """
    if os.environ.get("CI"):
        pytest.fail(
            f"{reason}\n\n"
            "This is a hard failure because CI is expected to provide Postgres "
            "(DATABASE_URL_SYNC_TEST, or Docker for testcontainers). Skipping here "
            "would let the migration-roundtrip job pass without testing a single "
            "migration. Check the job's env block and service container."
        )
    pytest.skip(reason)


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


@pytest.fixture(scope="session")
def roundtrip_db_url():
    """Postgres URL for the migration tests.

    Priority:
    1. ``DATABASE_URL_SYNC_TEST`` env var (CI, or manual override)
    2. testcontainers[postgres] auto-provisioned container (local with Docker)
    3. skip the test locally — but **fail** in CI (see ``_no_postgres``)

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
        _no_postgres(
            "Round-trip test requires Postgres. Set DATABASE_URL_SYNC_TEST or run "
            "with Docker Desktop running."
        )

    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        _no_postgres(
            "testcontainers not installed. Install with: "
            "`uv pip install 'testcontainers[postgres]'` or run CI instead."
        )

    # One container per pytest session, shared by every module in this
    # directory. Torn down when the fixture generator exits.
    container = PostgresContainer("postgres:16-alpine")
    container.start()
    try:
        yield container.get_connection_url().replace("postgresql+psycopg2", "postgresql")
    finally:
        container.stop()

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


def _no_postgres(reason: str) -> None:
    """Skip locally when Postgres is unavailable — but FAIL in CI.

    The ``migration-roundtrip`` job runs *only* this file, so if these tests
    skip, the job exits 0 and reports green having verified nothing — while
    the thing it guards (a downgrade that only matters at 2 AM during a prod
    rollback) goes unchecked. CI always has both Docker and
    ``DATABASE_URL_SYNC_TEST``, so reaching here in CI means the workflow
    broke, and that must be loud.

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


@pytest.fixture(scope="session")
def roundtrip_db_url():
    """Postgres URL for round-trip tests.

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


# Probe values chosen to fit int64 and bust int32 (max 2_147_483_647 ≈ 2 GB).
# Each value is a realistic real-world size for the column's semantic:
#   - plans.bytes_included: 100 GB (current Pro tier spec — core#249)
#   - usage_ledger.quantity: 3 GB (single day of Pro-scale ingestion)
#   - charges.overage_quantity: 50 GB (one month of mild Pro overage)
#   - uploaded_files.file_size: 5 GB (realistic Enterprise single upload)
#   - runs.rows_loaded: 3 * 10**9 rows (Enterprise backfill — currently
#     int32, tracked by core#283; probe is xfailed until the widening migration ships)
_GB = 1024**3
_PROBE_PLAN_BYTES = 100 * _GB  # 107_374_182_400
_PROBE_USAGE_BYTES = 3 * _GB  # 3_221_225_472
_PROBE_OVERAGE_BYTES = 50 * _GB  # 53_687_091_200
_PROBE_FILE_SIZE_BYTES = 5 * _GB  # 5_368_709_120
_PROBE_ROWS_LOADED = 3_000_000_000  # Busts int32.max (2_147_483_647)


def test_realistic_byte_size_roundtrip(roundtrip_db_url: str) -> None:
    """Insert real-world byte sizes into bigint columns; catch int32 regressions.

    Would-have-caught: **core#272**. The existing schema-equivalence test
    only compares column names — type-width regressions are invisible
    because SQLite maps both Integer and BigInteger to a dynamic-width
    INTEGER. Postgres enforces fixed widths, so inserting a value above
    `int32.max` (≈ 2 GB) surfaces any silent regression immediately.

    Columns probed:
      - `plans.bytes_included` at 100 GB — the Pro tier's included
        allotment; every Pro plan row stores this value.
      - `usage_ledger.quantity` at 3 GB — a single day of Pro-scale
        ingestion. This was the column core#272 fixed; this test locks
        that fix in.
      - `charges.overage_quantity` at 50 GB — a month of mild Pro
        overage. V2 P5 Option B writes this column on every cycle close.

    If any of these columns ever silently reverts to int32, this test
    fails with `NumericValueOutOfRange: integer out of range` and the
    PR is blocked.
    """
    _reset_db(roundtrip_db_url)
    r = _run_alembic(["upgrade", "head"], roundtrip_db_url)
    assert r.returncode == 0, (
        f"alembic upgrade head failed:\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )

    engine = create_engine(roundtrip_db_url)
    try:
        with engine.begin() as conn:
            org_id = conn.execute(
                text("INSERT INTO organizations (name, slug) VALUES (:n, :s) RETURNING id"),
                {"n": "QA bigint probe", "s": "qa-bigint-probe"},
            ).scalar_one()

            plan_id = conn.execute(
                text(
                    "INSERT INTO plans (name, slug, paddle_price_id, "
                    "paddle_product_id, price_cents, interval, bytes_included) "
                    "VALUES (:n, :s, :ppi, :ppp, :pc, :i, :bi) RETURNING id"
                ),
                {
                    "n": "QA Probe Plan",
                    "s": "qa-probe",
                    "ppi": "pri_qa_probe_001",
                    "ppp": "pro_qa_probe_001",
                    "pc": 0,
                    "i": "month",
                    "bi": _PROBE_PLAN_BYTES,
                },
            ).scalar_one()

            sub_id = conn.execute(
                text(
                    "INSERT INTO subscriptions (org_id, plan_id, "
                    "paddle_customer_id, paddle_subscription_id) "
                    "VALUES (:o, :p, :c, :s) RETURNING id"
                ),
                {
                    "o": org_id,
                    "p": plan_id,
                    "c": "ctm_qa_probe",
                    "s": "sub_qa_probe_001",
                },
            ).scalar_one()

            ul_id = conn.execute(
                text(
                    "INSERT INTO usage_ledger "
                    "(org_id, period_start, period_end, metric, quantity) "
                    "VALUES (:o, :ps, :pe, 'bytes_processed', :q) RETURNING id"
                ),
                {
                    "o": org_id,
                    "ps": "2026-04-01 00:00:00+00",
                    "pe": "2026-05-01 00:00:00+00",
                    "q": _PROBE_USAGE_BYTES,
                },
            ).scalar_one()

            charge_id = conn.execute(
                text(
                    "INSERT INTO charges "
                    "(org_id, idempotency_key, subscription_id, period_start, "
                    "period_end, metric, overage_quantity, amount_cents) "
                    "VALUES (:o, :k, :s, :ps, :pe, 'bytes_processed', :oq, :ac) "
                    "RETURNING id"
                ),
                {
                    "o": org_id,
                    "k": "sub_qa_probe_001:2026-04-01:bytes_processed",
                    "s": sub_id,
                    "ps": "2026-04-01 00:00:00+00",
                    "pe": "2026-05-01 00:00:00+00",
                    "oq": _PROBE_OVERAGE_BYTES,
                    "ac": 50000,
                },
            ).scalar_one()

            # uploaded_files.file_size — 5 GB. Bigint since u0q7r8s9t1n2;
            # this probe locks that guarantee in place for Enterprise uploads.
            file_id = conn.execute(
                text(
                    "INSERT INTO uploaded_files "
                    "(original_name, content_type, file_size, file_hash, "
                    "archive_path, org_id) "
                    "VALUES (:n, :t, :sz, :h, :p, :o) RETURNING id"
                ),
                {
                    "n": "qa_probe_5gb.csv",
                    "t": "text/csv",
                    "sz": _PROBE_FILE_SIZE_BYTES,
                    "h": "0" * 64,
                    "p": "/tmp/qa-probe",
                    "o": org_id,
                },
            ).scalar_one()

            # Assert round-trip — if the column is int32, the INSERT would
            # have already raised NumericValueOutOfRange. Assertions below
            # guard against less-obvious regressions (e.g., a migration that
            # silently truncates, or future ORM drift).
            assert (
                conn.execute(
                    text("SELECT bytes_included FROM plans WHERE id = :i"),
                    {"i": plan_id},
                ).scalar()
                == _PROBE_PLAN_BYTES
            )
            assert (
                conn.execute(
                    text("SELECT quantity FROM usage_ledger WHERE id = :i"),
                    {"i": ul_id},
                ).scalar()
                == _PROBE_USAGE_BYTES
            )
            assert (
                conn.execute(
                    text("SELECT overage_quantity FROM charges WHERE id = :i"),
                    {"i": charge_id},
                ).scalar()
                == _PROBE_OVERAGE_BYTES
            )
            assert (
                conn.execute(
                    text("SELECT file_size FROM uploaded_files WHERE id = :i"),
                    {"i": file_id},
                ).scalar()
                == _PROBE_FILE_SIZE_BYTES
            )

            # Delete in FK-order so the DB is clean for any following test.
            conn.execute(text("DELETE FROM uploaded_files"))
            conn.execute(text("DELETE FROM charges"))
            conn.execute(text("DELETE FROM usage_ledger"))
            conn.execute(text("DELETE FROM subscriptions"))
            conn.execute(text("DELETE FROM plans"))
            conn.execute(text("DELETE FROM organizations"))
    finally:
        engine.dispose()


def test_runs_rows_loaded_bigint_roundtrip(roundtrip_db_url: str) -> None:
    """3-billion-row probe for `runs.rows_loaded` (core#283 — shipped).

    Enterprise customers run one-shot backfills of clickstream / event /
    log data that routinely exceed 2^31 rows. dlt extracts the row count
    and we persist it into `runs.rows_loaded`; before core#283 the
    column was int32 and any ingestion over 2.14B rows raised
    `NumericValueOutOfRange`.

    Migration `d0e5f6g7h8i9` widens to bigint. This test was authored
    xfailed in core#282 as a red-test pointing at the gap; the xfail
    marker is removed now that the widening ships — a passing probe
    that regressions back to int32 would trip.
    """
    _reset_db(roundtrip_db_url)
    r = _run_alembic(["upgrade", "head"], roundtrip_db_url)
    assert r.returncode == 0, (
        f"alembic upgrade head failed:\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )

    engine = create_engine(roundtrip_db_url)
    try:
        with engine.begin() as conn:
            org_id = conn.execute(
                text("INSERT INTO organizations (name, slug) VALUES (:n, :s) RETURNING id"),
                {"n": "QA rows_loaded probe", "s": "qa-rows-loaded-probe"},
            ).scalar_one()

            run_id = conn.execute(
                text(
                    "INSERT INTO runs "
                    "(target_type, target_id, status, rows_loaded, org_id) "
                    "VALUES ('pipeline', 1, 'success', :r, :o) RETURNING id"
                ),
                {"r": _PROBE_ROWS_LOADED, "o": org_id},
            ).scalar_one()

            assert (
                conn.execute(
                    text("SELECT rows_loaded FROM runs WHERE id = :i"),
                    {"i": run_id},
                ).scalar()
                == _PROBE_ROWS_LOADED
            )

            conn.execute(text("DELETE FROM runs"))
            conn.execute(text("DELETE FROM organizations"))
    finally:
        engine.dispose()

"""core#726 — the migration round-trip with rows in the database.

``test_roundtrip.py`` runs ``upgrade head → downgrade -1 → upgrade head`` on a
real Postgres and compares the **schema**. It never puts a row in a table. Three
things follow from that, and all three are the kind that read as green:

1. **A schema comparison cannot see data loss.** Measured on restored production
   data, migration ``a7b8c9d0e1f2`` round-tripped *schema-identically and
   data-lossily*: ``downgrade()`` dropped ``users.password_changed_at`` and the
   re-``upgrade()`` re-backfilled every row from ``created_at``. Same columns,
   same types, same nullability, same indexes, same row count, **no new NULLs**.
   Every count-based assertion and the schema round-trip stayed green. Only
   comparing the *values* catches it — and that column is the
   session-revocation baseline, so the rollback direction fails **open**.
   (``password_reset_tokens`` went 1 → 0 in the same trip; row-loss is the other
   half of the same class.)
2. **A downgrade is only exercised against an empty database.** "Can this
   migration be rolled back" and "can this migration be rolled back *with
   customer rows in it*" are different questions, and only the second one is
   ever asked at 2 a.m. A ``DROP COLUMN`` never fails on an empty table.
3. **The escape hatch for migrations that legitimately destroy data did not
   exist.** It is implemented in ``conftest.py`` for this issue; see the header
   comment there. Without it, the first legitimately one-way migration turns
   this file into something a colleague disables under time pressure.

## Why the assertion is "by value", not "by count" or "by shape"

The case to design against is a round trip that preserves the row count, leaves
no NULLs, and produces an identical ``information_schema`` — **while replacing
every value in a column**. Both obvious harness shapes miss it. So this snapshots
every column of every row keyed by primary key, and compares the values.

The fixture is what makes that sensitive, and it is asserted rather than assumed
(``TestTheFixtureIsArmed``):

* **Every seeded timestamp is distinct**, so a column re-derived from a sibling
  column is a visible change. In ``a7b8c9d0e1f2`` the defect *is* the gap between
  ``created_at`` and ``password_changed_at``; a fixture that seeds them close
  together makes a wrong restore look almost right.
* **Every seeded timestamp is in the past**, so a column re-derived from ``now()``
  is visible too.
* **Every table in ``PUBLIC_TABLES`` holds a row**, asserted by set equality
  rather than by a lower bound. A table nobody seeded is a table this guard is
  blind to, and the blindness would arrive silently with the table.

## Controls

Per ``WORKFLOW_RULES`` §13: a forced red on a synthetic case is necessary and not
sufficient, because a synthetic control is written from the same mental model as
the check and agrees with it including where the check is wrong. So the differ
has cheap synthetic controls **and** the round trip is run end to end against the
**real migration tree with one synthetic head appended** — real alembic, real
Postgres, real ``env.py``, on a copy of the actual ``migrations/`` directory.

That appended head is ``a7b8c9d0e1f2``'s mechanism, deliberately: add a nullable
column, backfill it from ``created_at``. The same tree is then run a second time
with ``one_way = True`` added and nothing else changed. Unmarked it must **fail**;
marked it must **skip, and say so**. Two runs of one artifact differing only by
the marker is the only thing that distinguishes a skip that engaged from a skip
that never engaged — both are otherwise green.

## Cost

One extra ``upgrade head`` per test on the shared session container. Kept to four
DB-backed tests for that reason; the differ's own behaviour is covered without a
database.

⚠️ Running by hand on Windows: ``export UV_NO_SYNC=1`` first (see ``_run_alembic``).
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, inspect, text

from datanika.migrations.helpers import PUBLIC_TABLES
from tests.test_migrations.conftest import (
    ONE_WAY_MARKER,
    ONE_WAY_REASON_MARKER,
    PROJECT_ROOT,
    _run_alembic,
    head_revision,
    one_way_skip_reason,
    read_module_marker,
)

# `roundtrip_db_url` is deliberately NOT imported — see conftest.py's docstring.

# Deliberately a fixed instant in the past, and deliberately not round: a column
# re-derived from `now()` differs from all of these, and a column re-derived from
# a sibling differs because `_ts` never repeats.
_BASE = datetime(2025, 3, 4, 5, 6, 7)


def _ts(n: int) -> str:
    """The n-th distinguishable timestamp. Distinct for every n."""
    return (_BASE + timedelta(days=n, minutes=n, seconds=n)).strftime("%Y-%m-%d %H:%M:%S")


def _insert(conn, table: str, **cols: Any) -> int:
    keys = ", ".join(cols)
    binds = ", ".join(f":{k}" for k in cols)
    return conn.execute(
        text(f"INSERT INTO {table} ({keys}) VALUES ({binds}) RETURNING id"), cols
    ).scalar_one()


def _seed_identifiable_rows(conn) -> dict[str, int]:
    """One row in every table in ``PUBLIC_TABLES``, with distinguishable values.

    Not ``INSERT ... DEFAULT``: a row made of defaults is a row whose values a
    backfill can reproduce by accident, which is the one thing this file exists
    to detect. Every timestamp comes from ``_ts`` and no two share a value.
    """
    n = iter(range(1, 500))
    ids: dict[str, int] = {}

    ids["organizations"] = org = _insert(
        conn,
        "organizations",
        name="QA preservation org",
        slug="qa-preservation-org",
        default_dbt_schema="qa_dds",
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["users"] = user = _insert(
        conn,
        "users",
        email="preservation@qa.example.com",
        password_hash="$2b$12$qapreservationprobehash",
        full_name="Preservation Probe",
        is_active=True,
        email_verified=True,
        oauth_provider="google",
        oauth_provider_id="g-preservation-1",
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
        onboarding_checklist_dismissed_at=_ts(next(n)),
        # 🚨 The a7b8c9d0e1f2 shape. Eight months after created_at, because the
        # defect *is* the gap: a rollback that re-derives this from created_at
        # moves the session-revocation baseline backwards and fails open.
        password_changed_at=_ts(next(n) + 240),
    )
    ids["memberships"] = _insert(
        conn,
        "memberships",
        user_id=user,
        org_id=org,
        role="owner",
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["plans"] = plan = _insert(
        conn,
        "plans",
        name="QA Preservation Plan",
        slug="qa-preservation",
        paddle_price_id="pri_qa_preservation",
        paddle_product_id="pro_qa_preservation",
        price_cents=7900,
        interval="month",
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["subscriptions"] = sub = _insert(
        conn,
        "subscriptions",
        org_id=org,
        plan_id=plan,
        paddle_customer_id="ctm_qa_preservation",
        paddle_subscription_id="sub_qa_preservation",
        renews_at=_ts(next(n)),
        card_last_four="4242",
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["usage_ledger"] = _insert(
        conn,
        "usage_ledger",
        org_id=org,
        period_start=_ts(next(n)),
        period_end=_ts(next(n)),
        metric="bytes_processed",
        quantity=3_221_225_472,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["charges"] = _insert(
        conn,
        "charges",
        org_id=org,
        idempotency_key="sub_qa_preservation:2026-04-01:bytes_processed",
        subscription_id=sub,
        period_start=_ts(next(n)),
        period_end=_ts(next(n)),
        metric="bytes_processed",
        overage_quantity=53_687_091_200,
        amount_cents=4200,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["connections"] = conn_id = _insert(
        conn,
        "connections",
        name="QA preservation source",
        connection_type="postgres",
        direction="both",
        config_encrypted="gAAAAABqa-preservation-ciphertext",
        org_id=org,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["uploads"] = _insert(
        conn,
        "uploads",
        name="QA preservation upload",
        source_connection_id=conn_id,
        destination_connection_id=conn_id,
        dlt_config=json.dumps({"qa": "preservation", "tables": ["orders"]}),
        status="active",
        mode="etl",
        org_id=org,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["pipelines"] = _insert(
        conn,
        "pipelines",
        name="QA preservation pipeline",
        destination_connection_id=conn_id,
        command="build",
        full_refresh=False,
        status="active",
        mode="elt",
        org_id=org,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["transformations"] = _insert(
        conn,
        "transformations",
        name="QA preservation transformation",
        sql_body="select 1 as qa_preservation",
        materialization="table",
        schema_name="qa_dds",
        tests_config=json.dumps({"not_null": ["qa_preservation"]}),
        destination_connection_id=conn_id,
        org_id=org,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["dependencies"] = _insert(
        conn,
        "dependencies",
        upstream_type="upload",
        upstream_id=ids["uploads"],
        downstream_type="transformation",
        downstream_id=ids["transformations"],
        org_id=org,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["runs"] = run = _insert(
        conn,
        "runs",
        target_type="pipeline",
        target_id=ids["pipelines"],
        status="success",
        started_at=_ts(next(n)),
        finished_at=_ts(next(n)),
        rows_loaded=3_000_000_000,
        logs="qa preservation run log",
        org_id=org,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["schedules"] = _insert(
        conn,
        "schedules",
        target_type="upload",
        target_id=ids["uploads"],
        cron_expression="17 3 * * *",
        timezone="Europe/Athens",
        is_active=True,
        org_id=org,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["api_keys"] = api_key = _insert(
        conn,
        "api_keys",
        user_id=user,
        name="QA preservation key",
        key_hash="a" * 64,
        expires_at=_ts(next(n)),
        last_used_at=_ts(next(n)),
        org_id=org,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["audit_logs"] = _insert(
        conn,
        "audit_logs",
        user_id=user,
        action="login",
        resource_type="session",
        resource_id=run,
        old_values=json.dumps({"email": "preservation@qa.example.com"}),
        new_values=json.dumps({"email": "preservation2@qa.example.com"}),
        # PII inside audit rows — core#655's design does not reach these two
        # JSON columns, which is exactly why they are seeded with addresses.
        ip_address="203.0.113.42",
        org_id=org,
        created_at=_ts(next(n)),
    )
    ids["catalog_entries"] = _insert(
        conn,
        "catalog_entries",
        entry_type="source_table",
        origin_type="upload",
        origin_id=ids["uploads"],
        table_name="qa_preservation_orders",
        schema_name="qa_raw",
        dataset_name="qa_preservation",
        connection_id=conn_id,
        description="seeded by the data-preservation round-trip",
        org_id=org,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["uploaded_files"] = _insert(
        conn,
        "uploaded_files",
        original_name="qa_preservation.csv",
        content_type="text/csv",
        file_size=5_368_709_120,
        file_hash="b" * 64,
        archive_path="/archive/qa-preservation.csv",
        org_id=org,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["invitations"] = _insert(
        conn,
        "invitations",
        org_id=org,
        email="invited-preservation@qa.example.com",
        role="editor",
        invited_by_user_id=user,
        token="qa-preservation-invite-token",
        status="pending",
        expires_at=_ts(next(n)),
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["sso_configs"] = _insert(
        conn,
        "sso_configs",
        org_id=org,
        protocol="oidc",
        display_name="QA preservation IdP",
        oidc_issuer_url="https://idp.qa.example.com/",
        oidc_client_id="qa-preservation-client",
        oidc_client_secret_encrypted="gAAAAABqa-preservation-oidc-secret",
        is_active=True,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["notification_channels"] = _insert(
        conn,
        "notification_channels",
        name="QA preservation channel",
        channel_type="email",
        config=json.dumps({"to": "ops-preservation@qa.example.com"}),
        events=json.dumps(["run_failed"]),
        is_active=True,
        org_id=org,
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["notifications"] = _insert(
        conn,
        "notifications",
        org_id=org,
        user_id=user,
        type="run_failed",
        title="QA preservation notification",
        message="seeded by the data-preservation round-trip",
        resource_type="run",
        resource_id=run,
        read_at=_ts(next(n)),
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["oauth_clients"] = _insert(
        conn,
        "oauth_clients",
        client_id="qa-preservation-oauth-client",
        client_name="QA Preservation MCP Client",
        redirect_uris=json.dumps(["https://client.qa.example.com/callback"]),
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["oauth_grants"] = grant = _insert(
        conn,
        "oauth_grants",
        org_id=org,
        client_id="qa-preservation-oauth-client",
        user_id=user,
        api_key_id=api_key,
        encrypted_api_key="gAAAAABqa-preservation-apikey",
        code_hash="c" * 64,
        code_challenge="d" * 43,
        redirect_uri="https://client.qa.example.com/callback",
        scope="mcp:read",
        resource="https://app.datanika.io/mcp",
        code_expires_at=_ts(next(n)),
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["oauth_tokens"] = _insert(
        conn,
        "oauth_tokens",
        org_id=org,
        grant_id=grant,
        access_token_hash="e" * 64,
        refresh_token_hash="f" * 64,
        expires_at=_ts(next(n)),
        refresh_expires_at=_ts(next(n)),
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    ids["password_reset_tokens"] = _insert(
        conn,
        "password_reset_tokens",
        user_id=user,
        token_hash="0" * 64,
        expires_at=_ts(next(n)),
        created_at=_ts(next(n)),
        updated_at=_ts(next(n)),
    )
    return ids


# ---------------------------------------------------------------------------
# Snapshot + diff
# ---------------------------------------------------------------------------

Snapshot = dict[str, dict[Any, dict[str, str]]]


def _norm(value: Any) -> str:
    """A stable text form, so both snapshots are compared on the same terms.

    Normalises *representation*, never *content*: a datetime that came back with
    a different tzinfo, or a JSON column whose key order moved, is a real
    difference in what the database holds and would be reported.
    """
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, memoryview | bytes | bytearray):
        return bytes(value).hex()
    if isinstance(value, dict | list):
        return json.dumps(value, sort_keys=True, default=str)
    return f"{type(value).__name__}:{value}"


def _snapshot_data(db_url: str) -> Snapshot:
    """``{table: {pk: {column: normalised value}}}`` for the public schema."""
    engine = create_engine(db_url)
    try:
        insp = inspect(engine)
        snapshot: Snapshot = {}
        with engine.connect() as conn:
            for table in sorted(insp.get_table_names(schema="public")):
                if table == "alembic_version":
                    continue
                pk_cols = insp.get_pk_constraint(table, schema="public")["constrained_columns"]
                assert pk_cols, (
                    f"table {table} has no primary key, so its rows cannot be matched "
                    "across the round trip; this guard would silently compare nothing"
                )
                order = ", ".join(pk_cols)
                rows = (
                    conn.execute(text(f"SELECT * FROM {table} ORDER BY {order}")).mappings().all()
                )
                snapshot[table] = {
                    tuple(row[c] for c in pk_cols): {k: _norm(v) for k, v in row.items()}
                    for row in rows
                }
        return snapshot
    finally:
        engine.dispose()


def _diff_data(before: Snapshot, after: Snapshot) -> list[str]:
    """Human-readable findings. Empty list means the data survived."""
    findings: list[str] = []
    for table in sorted(set(before) | set(after)):
        rows_before = before.get(table, {})
        rows_after = after.get(table, {})
        for pk in sorted(set(rows_before) - set(rows_after), key=repr):
            findings.append(f"{table}: row {pk} was LOST by the round trip")
        for pk in sorted(set(rows_after) - set(rows_before), key=repr):
            findings.append(f"{table}: row {pk} APPEARED during the round trip")
        for pk in sorted(set(rows_before) & set(rows_after), key=repr):
            b, a = rows_before[pk], rows_after[pk]
            for column in sorted(set(b) & set(a)):
                if b[column] != a[column]:
                    findings.append(
                        f"{table}.{column} on row {pk} CHANGED: {b[column]!r} -> {a[column]!r}"
                    )
    return findings


def _reset_db(db_url: str) -> None:
    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    engine.dispose()


def _roundtrip_findings(
    db_url: str,
    config_path: Path | None = None,
    after_seed: str | None = None,
) -> tuple[list[str], Snapshot, Snapshot]:
    """upgrade head → seed → snapshot → downgrade -1 → upgrade head → snapshot.

    ``after_seed`` is one extra SQL statement run before the first snapshot. It
    exists for the mutated-tree control: the defect being reproduced is *the
    application having written a real value after the migration ran*, and a
    column that is still NULL when the round trip starts reproduces a weaker
    case than the one that matters.

    Returns the findings plus both snapshots, so a caller can show that the
    round trip was count-preserving and NULL-free while still being wrong.
    """
    _reset_db(db_url)

    r = _run_alembic(["upgrade", "head"], db_url, config_path)
    assert r.returncode == 0, f"initial upgrade failed:\n{r.stdout}\n{r.stderr}"

    engine = create_engine(db_url)
    try:
        with engine.begin() as conn:
            _seed_identifiable_rows(conn)
            if after_seed:
                conn.execute(text(after_seed))
    finally:
        engine.dispose()

    before = _snapshot_data(db_url)

    r = _run_alembic(["downgrade", "-1"], db_url, config_path)
    assert r.returncode == 0, (
        "`alembic downgrade -1` failed WITH ROWS PRESENT.\n\n"
        "The schema-only round-trip downgrades an empty database, so this is a "
        "class it cannot reach: a DROP COLUMN never fails on an empty table, and "
        "a FK or NOT NULL that the downgrade cannot satisfy only shows up once "
        "there is data. If this migration genuinely cannot be rolled back, "
        f'declare `{ONE_WAY_MARKER} = True` and `{ONE_WAY_REASON_MARKER} = "..."` '
        "at module scope in the migration file — the marker is read by "
        "tests/test_migrations/conftest.py and skips both round-trip guards.\n\n"
        f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )

    r = _run_alembic(["upgrade", "head"], db_url, config_path)
    assert r.returncode == 0, f"re-upgrade failed:\n{r.stdout}\n{r.stderr}"

    after = _snapshot_data(db_url)
    return _diff_data(before, after), before, after


# ---------------------------------------------------------------------------
# Guard-the-guard: the differ, without a database
# ---------------------------------------------------------------------------


class TestTheDifferCanActuallyFail:
    """Cheap controls. Necessary, and — per WORKFLOW_RULES §13 — not sufficient:
    every case here is written from the same mental model as ``_diff_data``, so
    it agrees with the differ including where the differ is wrong. The
    sufficient control is ``TestAgainstAMutatedMigrationTree`` below, which runs
    the real tree through real alembic.
    """

    def test_an_unchanged_snapshot_reports_nothing(self):
        snap: Snapshot = {"users": {(1,): {"id": "int:1", "email": "str:a@b.c"}}}
        assert _diff_data(snap, snap) == []

    def test_it_reports_a_changed_value(self):
        before: Snapshot = {"users": {(1,): {"password_changed_at": "2026-08-30T12:00:00"}}}
        after: Snapshot = {"users": {(1,): {"password_changed_at": "2026-01-01T00:00:00"}}}
        findings = _diff_data(before, after)
        assert len(findings) == 1
        assert "users.password_changed_at" in findings[0]
        assert "CHANGED" in findings[0]

    def test_it_reports_a_lost_row(self):
        before: Snapshot = {"password_reset_tokens": {(1,): {"token_hash": "abc"}}}
        after: Snapshot = {"password_reset_tokens": {}}
        findings = _diff_data(before, after)
        assert len(findings) == 1 and "LOST" in findings[0]

    def test_it_reports_an_appearing_row(self):
        """A re-upgrade that re-creates rows is as wrong as one that drops them,
        and a row count would call the pair of them clean."""
        before: Snapshot = {"runs": {(1,): {"status": "success"}}}
        after: Snapshot = {"runs": {(1,): {"status": "success"}, (2,): {"status": "success"}}}
        findings = _diff_data(before, after)
        assert len(findings) == 1 and "APPEARED" in findings[0]

    def test_a_count_preserving_null_free_change_is_still_caught(self):
        """The shape the whole file is designed against — ``a7b8c9d0e1f2``.

        Same row count, no NULLs introduced, identical column set. Every
        count-based assertion and the schema round-trip stay green.
        """
        before: Snapshot = {
            "users": {
                (1,): {"created_at": "2026-01-01T00:00:00", "pwc": "2026-08-30T12:00:00"},
                (2,): {"created_at": "2026-02-01T00:00:00", "pwc": "2026-08-31T09:00:00"},
            }
        }
        after: Snapshot = {
            "users": {
                (1,): {"created_at": "2026-01-01T00:00:00", "pwc": "2026-01-01T00:00:00"},
                (2,): {"created_at": "2026-02-01T00:00:00", "pwc": "2026-02-01T00:00:00"},
            }
        }
        assert len(before["users"]) == len(after["users"])
        assert not any("NULL" in v for row in after["users"].values() for v in row.values())
        findings = _diff_data(before, after)
        assert len(findings) == 2, findings
        assert all("CHANGED" in f for f in findings)

    def test_it_ignores_a_column_that_exists_on_only_one_side(self):
        """Columns appearing or disappearing is the *schema* round-trip's job.
        Reporting it here too would make this file fail for a reason it does not
        name, and the reader would go looking in the wrong place."""
        before: Snapshot = {"users": {(1,): {"email": "a@b.c", "gone": "x"}}}
        after: Snapshot = {"users": {(1,): {"email": "a@b.c"}}}
        assert _diff_data(before, after) == []


# ---------------------------------------------------------------------------
# Guard-the-guard: the one_way marker, without a database
# ---------------------------------------------------------------------------


def _write_migration(
    directory: Path, name: str, body: str = "", revision: str = "qa000000test"
) -> Path:
    """A syntactically real migration. ``down_revision = None`` makes it a root,
    so a directory holding one is a valid single-revision graph."""
    path = directory / f"{name}.py"
    path.write_text(
        "from alembic import op\n\n"
        f'revision = "{revision}"\n'
        "down_revision = None\n"
        f"{body}\n\n"
        "def upgrade() -> None:\n    pass\n\n"
        "def downgrade() -> None:\n    pass\n",
        encoding="utf-8",
    )
    return path


class TestTheOneWayMarkerReaderCanActuallyFail:
    def test_an_unmarked_migration_reads_as_absent(self, tmp_path):
        path = _write_migration(tmp_path, "plain")
        assert read_module_marker(path, ONE_WAY_MARKER) is None

    def test_a_marked_migration_reads_as_true(self, tmp_path):
        path = _write_migration(tmp_path, "marked", body=f"{ONE_WAY_MARKER} = True")
        assert read_module_marker(path, ONE_WAY_MARKER) is True

    def test_an_explicit_false_reads_as_false(self, tmp_path):
        path = _write_migration(tmp_path, "unmarked", body=f"{ONE_WAY_MARKER} = False")
        assert read_module_marker(path, ONE_WAY_MARKER) is False

    def test_an_annotated_assignment_is_read(self, tmp_path):
        path = _write_migration(tmp_path, "annotated", body=f"{ONE_WAY_MARKER}: bool = True")
        assert read_module_marker(path, ONE_WAY_MARKER) is True

    def test_a_marker_inside_a_function_is_not_a_declaration(self, tmp_path):
        path = _write_migration(
            tmp_path, "nested", body=f"def helper():\n    {ONE_WAY_MARKER} = True"
        )
        assert read_module_marker(path, ONE_WAY_MARKER) is None

    def test_a_non_literal_marker_raises_rather_than_reading_as_absent(self, tmp_path):
        """🚨 The whole point. A classifier that returns something falsy for a
        shape it does not recognise produces a SKIP, and a skip is the same
        colour as a pass. Here it must be loud instead."""
        path = _write_migration(
            tmp_path,
            "computed",
            body=f'import os\n{ONE_WAY_MARKER} = os.environ.get("QA") == "1"',
        )
        with pytest.raises(ValueError, match="non-literal"):
            read_module_marker(path, ONE_WAY_MARKER)


class TestTheEscapeHatchIsVisible:
    def test_the_real_head_is_resolved_and_is_not_currently_one_way(self):
        """Arming check. If this ever skips silently, every round-trip guard in
        this directory is switched off and nothing says so."""
        revision, path = head_revision()
        assert revision and path.exists(), "alembic reported no usable head revision"
        assert one_way_skip_reason() is None, (
            f"the head migration {path.name} is marked one-way, so BOTH round-trip "
            "guards are skipped for this release. That may be correct — but it "
            "must be a decision someone made, not something discovered later."
        )

    def test_marking_one_way_without_a_reason_is_refused(self, tmp_path, monkeypatch):
        """An escape hatch whose use is invisible cannot be told apart from one
        that never engaged. The reason is what the skip line prints."""
        versions = tmp_path / "migrations" / "versions"
        versions.mkdir(parents=True)
        _write_migration(versions, "qa_head", body=f"{ONE_WAY_MARKER} = True", revision="qahead1")
        ini = _write_alembic_ini(tmp_path)
        with pytest.raises(ValueError, match=ONE_WAY_REASON_MARKER):
            one_way_skip_reason(ini)

    def test_a_reasoned_marker_produces_a_skip_line_naming_the_revision(self, tmp_path):
        versions = tmp_path / "migrations" / "versions"
        versions.mkdir(parents=True)
        _write_migration(
            versions,
            "qa_head",
            body=f'{ONE_WAY_MARKER} = True\n{ONE_WAY_REASON_MARKER} = "drops the legacy blob"',
            revision="qahead2",
        )
        reason = one_way_skip_reason(_write_alembic_ini(tmp_path))
        assert reason is not None
        assert "qahead2" in reason and "drops the legacy blob" in reason


def _write_alembic_ini(root: Path, script_location: Path | None = None) -> Path:
    """A minimal alembic.ini pointing at ``root/migrations`` (or elsewhere).

    Only ``script_location`` is needed to build a ``ScriptDirectory``; the
    logging sections are only read by ``env.py``, which these tests do not run.
    """
    ini = root / "alembic.ini"
    location = script_location or (root / "migrations")
    ini.write_text(f"[alembic]\nscript_location = {location}\n", encoding="utf-8")
    return ini


# ---------------------------------------------------------------------------
# The real thing
# ---------------------------------------------------------------------------


def _fresh_seeded(db_url: str) -> tuple[dict[str, int], Snapshot]:
    """Blank DB → ``upgrade head`` → seed. Returns the seeded ids and a snapshot.

    The ids matter: ``plans`` already holds rows **inserted by the migrations
    themselves** (``c1d2e3f4a5b6`` seeds the priced tiers), and those carry
    ``now()`` timestamps. Assertions about the fixture must look at the fixture's
    own rows, or they end up asserting things about migration seed data and fail
    for a reason that has nothing to do with what they are checking.
    """
    _reset_db(db_url)
    r = _run_alembic(["upgrade", "head"], db_url)
    assert r.returncode == 0, f"{r.stdout}\n{r.stderr}"
    engine = create_engine(db_url)
    try:
        with engine.begin() as conn:
            ids = _seed_identifiable_rows(conn)
    finally:
        engine.dispose()
    return ids, _snapshot_data(db_url)


def _seeded_rows(ids: dict[str, int], snapshot: Snapshot) -> list[tuple[str, Any, dict[str, str]]]:
    """Only the rows ``_seed_identifiable_rows`` created."""
    return [
        (table, (pk,), snapshot[table][(pk,)])
        for table, pk in ids.items()
        if (pk,) in snapshot.get(table, {})
    ]


class TestTheFixtureIsArmed:
    """A fixture that does not cover a table makes this guard blind to it, and
    an under-populated run must fail rather than read as clean."""

    def test_every_public_table_is_seeded(self, roundtrip_db_url):
        _, snapshot = _fresh_seeded(roundtrip_db_url)
        populated = {table for table, rows in snapshot.items() if rows}
        assert populated == PUBLIC_TABLES, (
            "the seeder and PUBLIC_TABLES disagree, so this guard is blind to "
            "some tables while reading as full coverage.\n"
            f"  in PUBLIC_TABLES but never seeded: {sorted(PUBLIC_TABLES - populated)}\n"
            f"  seeded but not in PUBLIC_TABLES:   {sorted(populated - PUBLIC_TABLES)}\n"
            "Asserted by set equality on purpose: a lower bound would let a "
            "newly added table arrive uncovered and silently."
        )

    def test_no_seeded_row_holds_two_equal_timestamps(self, roundtrip_db_url):
        """The sensitivity of the whole file.

        ``a7b8c9d0e1f2`` re-derived ``password_changed_at`` from ``created_at``.
        If a fixture seeds those two close together — or equal — the wrong
        restore produces the right value and the guard passes over a live
        defect. Every seeded timestamp must therefore be distinct within its
        row, and this asserts it against the database rather than against the
        seeding code.
        """
        ids, snapshot = _fresh_seeded(roundtrip_db_url)
        offenders: list[str] = []
        for table, pk, row in _seeded_rows(ids, snapshot):
            seen: dict[str, str] = {}
            for col, val in row.items():
                if val == "NULL" or not _looks_like_a_timestamp(val):
                    continue
                if val in seen:
                    offenders.append(f"{table}.{seen[val]} == {table}.{col} on row {pk}")
                seen[val] = col
        assert not offenders, (
            "seeded timestamps collide, so a column re-derived from a sibling "
            "would restore to the right value and this guard would pass over "
            "it:\n  " + "\n  ".join(offenders)
        )

    def test_every_seeded_timestamp_is_in_the_past(self, roundtrip_db_url):
        """So a column re-derived from ``now()`` is visible too."""
        ids, snapshot = _fresh_seeded(roundtrip_db_url)
        cutoff = datetime(2026, 1, 1).isoformat()
        late = [
            f"{table}.{col} on row {pk} = {val}"
            for table, pk, row in _seeded_rows(ids, snapshot)
            for col, val in row.items()
            if val != "NULL" and _looks_like_a_timestamp(val) and val > cutoff
        ]
        assert not late, "seeded timestamps must predate any plausible now():\n  " + "\n  ".join(
            late
        )


def _looks_like_a_timestamp(normalised: str) -> bool:
    """``_norm`` renders datetimes as bare ISO-8601; everything else is prefixed
    with its type name, so the absence of a prefix is the discriminator."""
    return len(normalised) >= 19 and normalised[4] == "-" and normalised[10] == "T"


class TestDataSurvivesTheHeadRoundTrip:
    def test_the_head_migration_preserves_every_seeded_value(self, roundtrip_db_url):
        """The issue's negative control, and the ordinary case.

        A correct expand/contract migration must pass this **unchanged**. A
        guard that demanded every migration preserve every row would fail the
        legitimately destructive ones and be disabled within a week — which is
        why the ``one_way`` escape hatch is part of the same change rather than
        a follow-up.
        """
        reason = one_way_skip_reason()
        if reason:
            pytest.skip(reason)

        findings, _, _ = _roundtrip_findings(roundtrip_db_url)
        revision, path = head_revision()
        assert not findings, (
            f"migration {revision} ({path.name}) round-trips schema-identically "
            "and data-lossily. Counts and NOT NULL both survive this; only "
            "comparing values catches it:\n  " + "\n  ".join(findings)
        )


# ---------------------------------------------------------------------------
# The sufficient control — the real tree, mutated
# ---------------------------------------------------------------------------

_DESTRUCTIVE_HEAD = '''"""QA control: the a7b8c9d0e1f2 mechanism, appended to the real tree.

Add a nullable column and backfill it from a sibling. Schema round-trips
perfectly; the value does not.
"""

import sqlalchemy as sa
from alembic import op

revision = "qa726destructive"
down_revision = "{down_revision}"
branch_labels = None
depends_on = None
{marker}


def upgrade() -> None:
    # timezone=True to match users.created_at, which it is backfilled from.
    # SQLAlchemy renders both as "TIMESTAMP", so the difference is invisible in
    # a reflected type name and shows up only in the value: a naive column
    # backfilled from a tz-aware one round-trips to the same instant with a
    # different offset, and the differ correctly reports that as a change.
    op.add_column(
        "users",
        sa.Column("qa_probe_last_seen", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "UPDATE users SET qa_probe_last_seen = created_at "
        "WHERE qa_probe_last_seen IS NULL"
    )


def downgrade() -> None:
    op.drop_column("users", "qa_probe_last_seen")
'''


def _mutated_tree(tmp_path: Path, marker: str = "") -> Path:
    """A copy of the **real** migration tree with one destructive head appended.

    WORKFLOW_RULES §13: a synthetic fixture agrees with the check including
    where the check is wrong, so the control has to be the real artifact with
    the defect's shape introduced into it. This copies
    ``datanika/migrations/`` verbatim — the same ``env.py``, the same 36
    revisions — and appends one head. Returns the path to an ``alembic.ini``
    pointing at it.
    """
    real = PROJECT_ROOT / "datanika" / "migrations"
    dest = tmp_path / "migrations"
    shutil.copytree(real, dest, ignore=shutil.ignore_patterns("__pycache__"))
    head, _ = head_revision()
    (dest / "versions" / "qa726_destructive.py").write_text(
        _DESTRUCTIVE_HEAD.format(down_revision=head, marker=marker), encoding="utf-8"
    )
    ini = tmp_path / "alembic.ini"
    ini.write_text(
        (PROJECT_ROOT / "alembic.ini")
        .read_text(encoding="utf-8")
        .replace("script_location = datanika/migrations", f"script_location = {dest}"),
        encoding="utf-8",
    )
    return ini


class TestAgainstAMutatedMigrationTree:
    """Two runs of one artifact, differing only by the marker.

    Unmarked it must FAIL; marked it must SKIP and say so. That pair is the only
    thing that tells a skip which engaged apart from a skip which never
    engaged — on their own, both are green.
    """

    @pytest.fixture(autouse=True)
    def _leave_the_shared_database_usable(self, roundtrip_db_url):
        """🚨 Running a foreign migration tree leaves ``alembic_version`` holding
        a revision the real tree has never heard of.

        The Postgres container is session-scoped and shared by every module in
        this directory, and ``test_expand_contract.py``'s module fixture runs
        ``alembic upgrade head`` **without resetting first**. So without this,
        these two tests pass in isolation and break five tests in another file
        with ``Can't locate revision identified by 'qa726destructive'`` — a
        failure that names this file nowhere and only appears when the whole
        directory runs. Measured, not anticipated.
        """
        yield
        _reset_db(roundtrip_db_url)

    def test_a_value_destroying_head_is_caught(self, roundtrip_db_url, tmp_path):
        ini = _mutated_tree(tmp_path)
        assert one_way_skip_reason(ini) is None, "the unmarked control must not skip"

        # The application writing a real value after the migration ran. Without
        # this the column is still NULL when the round trip starts, and the
        # control degrades into "NULL became populated" — a strictly easier case
        # that a not-null check would also catch, and therefore not a control
        # for the assertion under test.
        findings, before, after = _roundtrip_findings(
            roundtrip_db_url,
            ini,
            after_seed=(
                "UPDATE users SET qa_probe_last_seen = TIMESTAMPTZ '2025-11-30 08:09:10+00'"
            ),
        )

        users_before = before["users"]
        users_after = after["users"]
        pk = next(iter(users_before))
        assert users_before[pk]["qa_probe_last_seen"] == "2025-11-30T08:09:10+00:00", (
            "the control did not arm: the column must hold a real value before "
            "the downgrade, or this is not the defect being reproduced"
        )

        # 🚨 Everything a cheaper guard would look at survives this round trip.
        # Stated as assertions rather than as prose, so the claim is measured.
        assert len(users_before) == len(users_after), "row counts differ — wrong control"
        assert users_after[pk]["qa_probe_last_seen"] != "NULL", (
            "the column comes back populated; a not-null check passes this"
        )
        assert sorted(users_before[pk]) == sorted(users_after[pk]), (
            "the column set is identical; the schema round-trip passes this"
        )

        changed = [f for f in findings if "users.qa_probe_last_seen" in f and "CHANGED" in f]
        assert changed, (
            "the round trip replaced users.qa_probe_last_seen with a value "
            "re-derived from created_at and this guard did not notice — which "
            "is exactly what happened to password_changed_at in a7b8c9d0e1f2. "
            "Findings were:\n  " + "\n  ".join(findings)
        )
        assert users_after[pk]["qa_probe_last_seen"] == users_after[pk]["created_at"], (
            "the restored value should be created_at — if it is not, this test "
            "is passing for some other reason than the one it names"
        )

    def test_the_same_head_marked_one_way_is_skipped_and_says_why(self, roundtrip_db_url, tmp_path):
        ini = _mutated_tree(
            tmp_path,
            marker=(
                f"\n{ONE_WAY_MARKER} = True"
                f'\n{ONE_WAY_REASON_MARKER} = "QA control: deliberately re-derives a value"'
            ),
        )
        reason = one_way_skip_reason(ini)
        assert reason is not None, (
            "the marker did not engage. A one_way skip that never engages and "
            "one that engages correctly are both green, which is why this is "
            "asserted against the same tree the previous test proves is red."
        )
        assert "qa726destructive" in reason, "the skip line must name the revision it exempts"
        assert "deliberately re-derives a value" in reason, (
            "the skip line must carry the declared reason — it is what someone "
            "reads while deciding whether a rollback is possible"
        )

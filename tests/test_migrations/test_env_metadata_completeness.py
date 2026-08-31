"""`alembic revision --autogenerate` must not propose dropping live tables.

Found while widening the tenant-accessor guard (#732), from the same shape one
level up: **a hand-maintained enumeration that silently omits members.**

`datanika/migrations/env.py` sets `target_metadata = Base.metadata` and imports
only `datanika.models.base`. `Base.metadata` is therefore populated by whatever
that import transitively pulls in — which is `datanika/models/__init__.py`, a
hand-written list. Four tenant-owned models were missing from it: `Invitation`,
`Notification`, `NotificationChannel`, `SSOConfig`.

Those four **are** in `PUBLIC_TABLES`, so `_include_public` returns True for
them, while they are absent from `target_metadata`. Alembic's rule for a table
that exists in the database, passes the filter, and is absent from the metadata
is to emit `op.drop_table(...)`. So the workflow `CLAUDE.md` documents —
`uv run alembic revision --autogenerate -m "..."` — produced a migration
dropping the SSO configuration, the invitations, the notification channels and
the notifications of every tenant. `DROP TABLE` is also exactly what the
expand/contract policy forbids shipping in the same release as the code needing
it, so the reviewer's first instinct ("that looks wrong") is the only control
that existed.

Nothing else caught it. `test_migration_coverage.py` imports every module under
`datanika/models/` with `pkgutil` before reading `Base.metadata`, so it sees all
22 tables and is blind to the omission by construction: it checks
**model → migration**, and this is a defect in what counts as a model.
"""

import contextlib

from datanika.migrations.helpers import PUBLIC_TABLES

# Tables owned by the cloud plugin's own models. `env.py` imports them under a
# suppressed ImportError, so they are present in the cloud edition and absent in
# a core-only checkout. Excluded here because their absence is intended.
CLOUD_TABLES = {"plans", "subscriptions", "usage_ledger", "charges"}


def _env_target_metadata_tables() -> set[str]:
    """Exactly what `env.py` has in `target_metadata`, imported the same way.

    Deliberately re-imports rather than calling a helper: the whole defect is
    that `env.py`'s import set is narrower than any test's, so a test that
    imports more than `env.py` does cannot see it. This must stay a replica.
    """
    from datanika.models.base import Base

    with contextlib.suppress(ImportError):
        import datanika_cloud.billing.models  # noqa: F401

    return set(Base.metadata.tables)


def test_autogenerate_would_not_drop_a_live_table() -> None:
    seen = _env_target_metadata_tables()
    expected = PUBLIC_TABLES - CLOUD_TABLES
    missing = sorted(expected - seen)

    assert missing == [], (
        "these tables are in PUBLIC_TABLES (so alembic's include_object keeps them) but "
        f"absent from env.py's target_metadata: {missing}. `alembic revision "
        "--autogenerate` will emit op.drop_table() for each one, and a reviewer noticing "
        "is the only thing between that and production. Import the model in "
        "datanika/models/__init__.py."
    )


def test_the_replica_actually_imports_something() -> None:
    """Arming check.

    If the import above ever silently yields an empty metadata, the assertion
    over `expected - seen` still passes trivially for an empty `expected`, and a
    check that cannot fail is this project's signature defect.
    """
    seen = _env_target_metadata_tables()
    assert len(seen) >= 18, f"env.py's metadata carries only {len(seen)} tables — replica broken"
    assert PUBLIC_TABLES - CLOUD_TABLES, "PUBLIC_TABLES is empty — the comparison is vacuous"

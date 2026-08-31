"""The password-reset migration is expand-only, and its backfill fails closed.

Two separate concerns:

1. **Expand/contract.** Blue/green starts the new container while the old one
   still serves, so the previously-deployed code runs against this schema for
   the whole swap window. A nullable ADD COLUMN and a CREATE TABLE are both
   invisible to it.

2. **The backfill's WHERE clause, which is the interesting half.**
   ``password_changed_at IS NULL`` is the discriminator for "never set a
   password" (D6). Backfilling only ``WHERE oauth_provider IS NULL`` — the
   obvious clause, and the one the spec's sketch carries — *misses* every
   password account that later linked Google or GitHub, because
   ``find_or_create_oauth_user`` backfills ``oauth_provider`` onto exactly those
   rows. Those users would come out of the migration marked "never had a
   password", which drops the current-password re-verification from their
   Settings card.

   That fails **open**. Backfilling every existing row instead fails **closed**:
   an OAuth-only account is merely told to use the email reset path, which sets
   a real password and corrects its row for good.
"""

import pathlib

MIGRATIONS = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "migrations" / "versions"

# Name of the module global the migration uses for its rollback stash. Referred
# to by identifier rather than by value so this file does not restate the table
# name and quietly stop matching if it is renamed.
_STASH_CONST = "_ROLLBACK_STASH"


def _migration() -> pathlib.Path:
    matches = [p for p in MIGRATIONS.glob("*.py") if "password_reset" in p.name]
    assert len(matches) == 1, f"expected exactly one password-reset migration, got {matches}"
    return matches[0]


def _upgrade_body() -> str:
    source = _migration().read_text(encoding="utf-8")
    start = source.index("def upgrade(")
    end = source.index("def downgrade(") if "def downgrade(" in source else len(source)
    return source[start:end]


def _add_column_call() -> str:
    """The whole ``op.add_column(...)`` statement, by paren balancing.

    A regex stopping at the first ``)`` reads only as far as
    ``sa.DateTime(timezone=True)`` and then reports a correct migration as
    unsafe — which is the kind of test failure that gets "fixed" in the source.
    """
    body = _upgrade_body()
    start = body.index("op.add_column(")
    depth = 0
    for offset in range(start, len(body)):
        if body[offset] == "(":
            depth += 1
        elif body[offset] == ")":
            depth -= 1
            if depth == 0:
                return body[start : offset + 1]
    raise AssertionError("unbalanced op.add_column(")


class TestExpandOnly:
    def test_no_destructive_operation_in_upgrade(self):
        """Nothing the previously-deployed release can see is removed at t1.

        ``upgrade()`` drops exactly one thing: ``_ROLLBACK_STASH``, which
        ``downgrade()`` writes and this function consumes (#726). No released
        version of the application has ever read that table, so it does not
        exist as far as the t1 window is concerned.

        The check is per line rather than per file so it keeps its teeth: a
        ``drop_column`` on ``users``, or a ``drop_table`` naming anything other
        than the stash, still fails here.
        """
        body = _upgrade_body()
        for call in ("drop_column", "drop_table", "drop_constraint", "rename_table"):
            for line in body.splitlines():
                if f"op.{call}(" not in line:
                    continue
                assert call == "drop_table" and "_ROLLBACK_STASH" in line, (
                    f"upgrade() performs op.{call} on something the deployed "
                    f"release can still see: {line.strip()}"
                )

    def test_the_rollback_stash_is_written_by_downgrade_and_consumed_by_upgrade(self):
        """Guard the guard above: the exemption is only sound while both halves
        exist. A stash that ``downgrade()`` stopped writing would leave the
        exemption permitting a drop of something nothing creates."""
        source = _migration().read_text(encoding="utf-8")
        start = source.index("def downgrade(")
        downgrade_body = source[start:]
        assert f"CREATE TABLE {{{_STASH_CONST}}}" in downgrade_body or (
            _STASH_CONST in downgrade_body and "CREATE TABLE" in downgrade_body
        ), "downgrade() no longer creates the rollback stash"
        assert _STASH_CONST in _upgrade_body(), "upgrade() no longer consumes the rollback stash"

    def test_the_new_user_column_is_nullable(self):
        """A NOT NULL column is invisible to the old container only while it is
        nullable; ``SET NOT NULL`` is a contract-phase operation."""
        call = _add_column_call()
        assert '"users"' in call
        assert "password_changed_at" in call
        assert "nullable=True" in call

    def test_it_creates_the_token_table(self):
        assert 'op.create_table(\n        "password_reset_tokens"' in _upgrade_body() or (
            'op.create_table("password_reset_tokens"' in _upgrade_body()
        )

    def test_it_commits_explicitly(self):
        """Alembic env.py with SQLAlchemy 2.0 autobegin does not auto-commit DDL."""
        assert "connection.commit()" in _upgrade_body()


class TestBackfillFailsClosed:
    def test_the_backfill_does_not_key_on_oauth_provider(self):
        body = _upgrade_body()
        update = [line for line in body.splitlines() if "password_changed_at =" in line]
        joined = " ".join(update) + " " + body
        assert "oauth_provider IS NULL" not in joined, (
            "backfilling only where oauth_provider IS NULL misses password accounts "
            "that later linked a provider, and strips their current-password check"
        )

    def test_every_existing_row_is_stamped(self):
        body = _upgrade_body()
        assert "UPDATE users" in body
        assert "password_changed_at" in body
        assert "created_at" in body

    def test_the_reasoning_is_recorded_in_the_migration(self):
        """The next person to read this will reach for the narrower WHERE."""
        source = _migration().read_text(encoding="utf-8")
        assert "oauth_provider" in source, "explain why the obvious clause is not used"


class TestModelAndMigrationAgree:
    def test_every_model_column_appears_in_the_migration(self):
        from datanika.models.password_reset import PasswordResetToken

        body = _upgrade_body()
        for column in PasswordResetToken.__table__.columns:
            assert f'"{column.name}"' in body, f"{column.name} missing from the migration"

    def test_the_user_column_exists_on_the_model(self):
        from datanika.models.user import User

        assert "password_changed_at" in User.__table__.columns
        assert User.__table__.columns["password_changed_at"].nullable is True

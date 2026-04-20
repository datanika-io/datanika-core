"""Regression test for the ``notifications.type`` VARCHAR widening
(core#279). Guards against accidental reversion to VARCHAR(14) that
would silently drop any notification whose type name is ≥15 chars.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_VERSIONS = Path(__file__).parent.parent.parent / "datanika" / "migrations" / "versions"
_MIGRATION = _VERSIONS / "c9d4e5f6g7h8_widen_notification_type_varchar.py"


@pytest.fixture(scope="module")
def migration_source() -> str:
    assert _MIGRATION.exists(), f"Missing migration file: {_MIGRATION}"
    return _MIGRATION.read_text(encoding="utf-8")


class TestWidenNotificationTypeMigration:
    def test_revision_id_and_parent(self, migration_source):
        assert 'revision: str = "c9d4e5f6g7h8"' in migration_source
        assert 'down_revision: str | None = "b8c3d4e5f6g7"' in migration_source

    def test_upgrade_widens_to_varchar50(self, migration_source):
        upgrade_match = re.search(r"def upgrade\(\).*?(?=\ndef |\Z)", migration_source, re.DOTALL)
        assert upgrade_match
        upgrade_body = upgrade_match.group(0)
        assert '"notifications"' in upgrade_body
        assert '"type"' in upgrade_body
        assert "type_=sa.String(length=50)" in upgrade_body
        assert "existing_type=sa.String(length=14)" in upgrade_body

    def test_downgrade_narrows_back(self, migration_source):
        downgrade_match = re.search(
            r"def downgrade\(\).*?(?=\ndef |\Z)", migration_source, re.DOTALL
        )
        assert downgrade_match
        downgrade_body = downgrade_match.group(0)
        assert "type_=sa.String(length=14)" in downgrade_body
        assert "existing_type=sa.String(length=50)" in downgrade_body


class TestNotificationModelTypeLength:
    """Model-level assert that catches the reverse mistake: migration
    widened but model still declares the old length. Future auto-generated
    migrations would then narrow the column again without anyone noticing.
    """

    def test_model_column_length_matches_migration(self):
        from datanika.models.notification import Notification

        col = Notification.__table__.c.type
        # SQLAlchemy's Enum type exposes length via ``.length`` when
        # ``native_enum=False``. We widened both the migration and the
        # model to 50.
        assert col.type.length == 50, (
            f"Notification.type column length is {col.type.length}, "
            f"expected 50. Model must match migration c9d4e5f6g7h8 "
            "(core#279). If you added a longer NotificationType value, "
            "bump both together."
        )

    def test_all_type_names_fit_declared_length(self):
        """Trip-wire for the class of bug that motivated core#279: a
        new enum value longer than the column width ships silently.
        """
        from datanika.models.notification import Notification, NotificationType

        limit = Notification.__table__.c.type.type.length
        too_long = [v.value for v in NotificationType if len(v.value) > limit]
        assert not too_long, (
            f"These NotificationType values exceed the column limit "
            f"({limit}): {too_long}. Widen the migration + model "
            "together before shipping."
        )

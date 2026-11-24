"""PII separation, release N+2: unmap the five legacy columns.

``docs/specs/SPEC_PII_SEPARATION.md`` §4 and §8a, tracked as core#939.
``origin/master`` still maps ``users.email``, ``users.full_name``,
``users.oauth_provider_id``, ``invitations.email`` and ``invitations.token``,
and SELECT names every mapped column. This revision leaves those columns in
the database. The models unmap them in the same release. A later revision
drops them only once production is this revision.

The email-channel recipient used to sit in ``notification_channels.config``
under the key ``email``. Release N copied it to ``notification_channel_pii``.
Dispatch reads the sidecar, not the stored key. This revision strips that key
from rows that still have it. Rows without the key are left untouched.

``downgrade()`` puts the key back from ``notification_channel_pii.recipient``
for an email channel when the key is absent. It does not re-add a column, and
it does not invent a plaintext invitation token.

Revision ID: f8b2c4d6e1a3
Revises: a670c1d2e3f4
Create Date: 2026-11-21
"""

from alembic import op

revision: str = "f8b2c4d6e1a3"
down_revision: str | None = "a670c1d2e3f4"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '3s'")
    op.execute(
        """
        UPDATE notification_channels
        SET config = (config::jsonb - 'email')::json
        WHERE config::jsonb ? 'email'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE notification_channels AS c
        SET config = (
            jsonb_set(
                COALESCE(c.config::jsonb, '{}'::jsonb),
                '{email}',
                to_jsonb(p.recipient)
            )
        )::json
        FROM notification_channel_pii AS p
        WHERE p.channel_id = c.id
          AND c.channel_type = 'email'
          AND NOT (COALESCE(c.config::jsonb, '{}'::jsonb) ? 'email')
        """
    )

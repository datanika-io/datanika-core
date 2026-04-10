"""Add sso_configs table

Revision ID: n3j0k1l2m4g5
Revises: m2i9j0k1l3f4
Create Date: 2026-03-24 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "n3j0k1l2m4g5"
down_revision: Union[str, None] = "m2i9j0k1l3f4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sso_configs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("org_id", sa.BigInteger(), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column("protocol", sa.String(10), nullable=False),
        sa.Column("display_name", sa.String(100), nullable=False),
        sa.Column("oidc_issuer_url", sa.String(500), nullable=True),
        sa.Column("oidc_client_id", sa.String(255), nullable=True),
        sa.Column("oidc_client_secret_encrypted", sa.Text(), nullable=True),
        sa.Column("saml_idp_metadata_url", sa.String(500), nullable=True),
        sa.Column("saml_idp_entity_id", sa.String(500), nullable=True),
        sa.Column("saml_idp_sso_url", sa.String(500), nullable=True),
        sa.Column("saml_idp_cert", sa.Text(), nullable=True),
        sa.Column("saml_sp_entity_id", sa.String(255), nullable=True, server_default="datanika"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.Column("deleted_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_sso_configs_org_id", "sso_configs", ["org_id"])


def downgrade() -> None:
    op.drop_table("sso_configs")

"""TDD tests for multi-tenant migration helpers."""

from etlfabric.migrations.helpers import (
    PUBLIC_TABLES,
    is_public_table,
    is_tenant_table,
)
from etlfabric.models.base import Base


class TestPublicTablesSet:
    def test_contains_organizations(self):
        assert "organizations" in PUBLIC_TABLES

    def test_contains_users(self):
        assert "users" in PUBLIC_TABLES

    def test_contains_memberships(self):
        assert "memberships" in PUBLIC_TABLES

    def test_contains_all_model_tables(self):
        """All model tables must be in PUBLIC_TABLES so Alembic creates them
        in the public schema. Services use org_id for tenant isolation, not
        per-tenant schemas. Regression test for 'relation does not exist' bug."""
        model_tables = {t.name for t in Base.metadata.sorted_tables}
        missing = model_tables - PUBLIC_TABLES
        assert missing == set(), f"Tables missing from PUBLIC_TABLES: {missing}"


class TestIsPublicTable:
    def test_organizations_is_public(self):
        assert is_public_table("organizations") is True

    def test_users_is_public(self):
        assert is_public_table("users") is True

    def test_memberships_is_public(self):
        assert is_public_table("memberships") is True

    def test_connections_is_public(self):
        assert is_public_table("connections") is True

    def test_pipelines_is_public(self):
        assert is_public_table("pipelines") is True

    def test_alembic_version_is_not_public(self):
        assert is_public_table("alembic_version") is False


class TestIsTenantTable:
    def test_organizations_is_not_tenant(self):
        assert is_tenant_table("organizations") is False

    def test_users_is_not_tenant(self):
        assert is_tenant_table("users") is False

    def test_alembic_version_is_not_tenant(self):
        assert is_tenant_table("alembic_version") is False

    def test_no_model_table_is_tenant(self):
        """Since all model tables are in PUBLIC_TABLES, none should be
        classified as tenant tables. This ensures Alembic creates them
        in public, not in per-tenant schemas."""
        model_tables = {t.name for t in Base.metadata.sorted_tables}
        tenant_classified = {t for t in model_tables if is_tenant_table(t)}
        assert tenant_classified == set()

"""TDD tests for multi-tenant migration helpers."""

from etlfabric.migrations.helpers import (
    PUBLIC_TABLES,
    is_public_table,
    is_tenant_table,
)


class TestPublicTablesSet:
    def test_contains_organizations(self):
        assert "organizations" in PUBLIC_TABLES

    def test_contains_users(self):
        assert "users" in PUBLIC_TABLES

    def test_contains_memberships(self):
        assert "memberships" in PUBLIC_TABLES

    def test_does_not_contain_tenant_tables(self):
        tenant_tables = [
            "connections",
            "pipelines",
            "transformations",
            "dependencies",
            "schedules",
            "runs",
        ]
        for t in tenant_tables:
            assert t not in PUBLIC_TABLES


class TestIsPublicTable:
    def test_organizations_is_public(self):
        assert is_public_table("organizations") is True

    def test_users_is_public(self):
        assert is_public_table("users") is True

    def test_memberships_is_public(self):
        assert is_public_table("memberships") is True

    def test_connections_is_not_public(self):
        assert is_public_table("connections") is False

    def test_pipelines_is_not_public(self):
        assert is_public_table("pipelines") is False

    def test_alembic_version_is_not_public(self):
        assert is_public_table("alembic_version") is False


class TestIsTenantTable:
    def test_connections_is_tenant(self):
        assert is_tenant_table("connections") is True

    def test_pipelines_is_tenant(self):
        assert is_tenant_table("pipelines") is True

    def test_transformations_is_tenant(self):
        assert is_tenant_table("transformations") is True

    def test_dependencies_is_tenant(self):
        assert is_tenant_table("dependencies") is True

    def test_schedules_is_tenant(self):
        assert is_tenant_table("schedules") is True

    def test_runs_is_tenant(self):
        assert is_tenant_table("runs") is True

    def test_organizations_is_not_tenant(self):
        assert is_tenant_table("organizations") is False

    def test_users_is_not_tenant(self):
        assert is_tenant_table("users") is False

    def test_alembic_version_is_not_tenant(self):
        assert is_tenant_table("alembic_version") is False

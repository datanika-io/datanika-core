"""TDD tests for tenant service (schema-per-tenant management)."""

import pytest

from etlfabric.services.tenant import TenantService


class TestTenantService:
    def test_config_schema_name(self):
        """Verify the naming convention for the tenant config schema."""
        svc = TenantService()
        assert svc.config_schema_name(42) == "tenant_42"
        assert svc.config_schema_name(1) == "tenant_1"
        assert svc.config_schema_name(999) == "tenant_999"

    def test_config_schema_names_different_per_org(self):
        """Different org_ids produce different schema names."""
        svc = TenantService()
        assert svc.config_schema_name(1) != svc.config_schema_name(2)

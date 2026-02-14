"""TDD tests for tenant service (schema-per-tenant management)."""

import uuid

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session

from etlfabric.services.tenant import TenantService


class TestTenantService:
    def test_schema_names_for_org(self):
        """Verify the naming convention for tenant schemas."""
        org_id = uuid.UUID("12345678-1234-1234-1234-123456789abc")
        svc = TenantService.__new__(TenantService)
        names = svc._schema_names(org_id)
        hex_id = org_id.hex[:12]

        assert names["config"] == f"tenant_{hex_id}"
        assert names["raw"] == f"tenant_{hex_id}_raw"
        assert names["staging"] == f"tenant_{hex_id}_staging"
        assert names["marts"] == f"tenant_{hex_id}_marts"

    def test_schema_names_are_deterministic(self):
        """Same org_id always produces the same schema names."""
        org_id = uuid.uuid4()
        svc = TenantService.__new__(TenantService)
        assert svc._schema_names(org_id) == svc._schema_names(org_id)

    def test_schema_names_different_per_org(self):
        """Different org_ids produce different schema names."""
        svc = TenantService.__new__(TenantService)
        names_a = svc._schema_names(uuid.uuid4())
        names_b = svc._schema_names(uuid.uuid4())
        assert names_a["config"] != names_b["config"]

"""V2 P1 scaffolding — IR package + elt_runner stub.

Per SPEC_ELT_IR_ARCHITECTURE.md §5.6, the Source → IR → {dlt ETL, dbt ELT}
module layout lands in P1 as empty stubs; builders/validators/runners
ship in P3. These tests pin the public interface so P3 doesn't drift
from the spec.
"""

import pytest


class TestIRPackageLayout:
    def test_ir_package_importable(self):
        import datanika.services.ir as ir

        assert ir is not None

    def test_ir_builder_module_exists(self):
        from datanika.services.ir import builder

        assert hasattr(builder, "build_ir")

    def test_ir_validator_module_exists(self):
        from datanika.services.ir import validator

        assert hasattr(validator, "validate_ir")

    def test_ir_introspect_module_exists(self):
        from datanika.services.ir import introspect

        assert hasattr(introspect, "introspect_columns")

    def test_ir_version_constant_exported(self):
        from datanika.services.ir import IR_VERSION

        assert IR_VERSION == 1


class TestIRModulesCallable:
    """P3 — stubs replaced with real implementations. Verify callability."""

    def test_build_ir_is_callable(self):
        from datanika.services.ir.builder import build_ir

        assert callable(build_ir)

    def test_validate_ir_is_callable(self):
        from datanika.services.ir.validator import validate_ir

        assert callable(validate_ir)

    def test_introspect_columns_is_callable(self):
        from datanika.services.ir.introspect import introspect_columns

        assert callable(introspect_columns)

    def test_build_ir_rejects_unsupported_source(self):
        """Non-SQL sources raise IRBuildError, not NotImplementedError."""
        from datanika.services.ir.builder import IRBuildError, build_ir

        with pytest.raises(IRBuildError, match="P4"):
            build_ir(
                source_type="stripe",
                source_config={},
                destination_connection_id=1,
                table="charges",
            )


class TestEltRunnerImportable:
    def test_elt_runner_importable(self):
        from datanika.services import elt_runner

        assert hasattr(elt_runner, "stream_to_raw")

    def test_stream_stats_importable(self):
        from datanika.services.elt_runner import StreamStats

        assert StreamStats is not None

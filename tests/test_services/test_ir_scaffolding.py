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


class TestIRStubsRaiseNotImplemented:
    """P1 stubs must raise so a mis-wired ELT path fails loudly, not silently."""

    def test_build_ir_raises(self):
        from datanika.services.ir.builder import build_ir

        with pytest.raises(NotImplementedError, match="P3"):
            build_ir(source=None)

    def test_validate_ir_raises(self):
        from datanika.services.ir.validator import validate_ir

        with pytest.raises(NotImplementedError, match="P3"):
            validate_ir(ir=None)

    def test_introspect_columns_raises(self):
        from datanika.services.ir.introspect import introspect_columns

        with pytest.raises(NotImplementedError, match="P3"):
            introspect_columns(source=None)


class TestEltRunnerStub:
    def test_elt_runner_importable(self):
        from datanika.services import elt_runner

        assert hasattr(elt_runner, "stream_to_raw")

    def test_stream_to_raw_raises(self):
        from datanika.services.elt_runner import stream_to_raw

        with pytest.raises(NotImplementedError, match="P3"):
            stream_to_raw(ir=None, run_id=1)

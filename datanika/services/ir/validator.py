"""IR validator — shape + type checks per SPEC_ELT_IR_ARCHITECTURE.md §5.1."""

from __future__ import annotations

from datanika.errors import UserFacingError
from datanika.services.ir import IR_VERSION
from datanika.services.ir.schema import IR


class IRValidationError(UserFacingError):
    """Raised when an IR document fails validation."""


def validate_ir(ir: IR) -> None:
    """Validate an IR document. Raises IRValidationError on failure."""
    if ir.ir_version != IR_VERSION:
        raise IRValidationError(f"Unsupported IR version {ir.ir_version}, expected {IR_VERSION}")

    if not ir.columns:
        raise IRValidationError("IR must have at least one column")

    # Duplicate target names
    target_names = [c.target_name for c in ir.columns]
    seen: set[str] = set()
    for name in target_names:
        if name in seen:
            raise IRValidationError(f"duplicate target_name: {name!r}")
        seen.add(name)

    # Primary key columns must exist in target_names
    for pk in ir.primary_key:
        if pk not in seen:
            raise IRValidationError(f"primary_key column {pk!r} not found in columns")

    # sql_table kind requires table
    if ir.source.kind == "sql_table" and not ir.source.table:
        raise IRValidationError("source kind 'sql_table' requires a table name")

    # Incremental cursor must reference an existing column
    if ir.incremental is not None and ir.incremental.cursor not in seen:
        raise IRValidationError(
            f"incremental cursor {ir.incremental.cursor!r} not found in columns"
        )

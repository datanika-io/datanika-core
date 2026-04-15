"""V2 P1 IR scaffolding — Source → IR → {dlt ETL, dbt ELT}.

Stub package. `build_ir`, `validate_ir`, and `introspect_columns` raise
`NotImplementedError` until the ELT builders land in P3 per
SPEC_ELT_IR_ARCHITECTURE.md §9.
"""

IR_VERSION: int = 1

__all__ = ["IR_VERSION"]

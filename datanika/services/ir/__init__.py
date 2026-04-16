"""V2 IR package — Source → IR → {dlt ETL, dbt ELT} dispatch.

The IR (Intermediate Representation) is a declarative schema-mapping document
between every source and the destination. Both ETL and ELT modes derive their
schemas from the same IR. See SPEC_ELT_IR_ARCHITECTURE.md.
"""

IR_VERSION: int = 1

__all__ = ["IR_VERSION"]

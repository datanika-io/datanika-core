"""IR column introspection — information-schema → IRColumn list.

Per SPEC_ELT_IR_ARCHITECTURE.md §5.2: for sql_table/sql_database sources,
introspects live via the existing ConnectionService.list_columns().
"""

from __future__ import annotations

from datanika.services.ir.schema import IRColumn


def introspect_columns(
    config: dict,
    connection_type: str,
    table: str,
    schema: str | None = None,
) -> list[IRColumn]:
    """Introspect a SQL table and return IR columns.

    Reuses ConnectionService.list_columns() — same SQLAlchemy inspect() path
    as POST /api/v1/connections/{id}/columns.
    """
    from datanika.services.connection_service import ConnectionService

    raw_columns = ConnectionService.list_columns(
        config=config,
        connection_type=connection_type,
        table=table,
        schema=schema,
    )

    return [
        IRColumn(
            source_ref=col["name"],
            target_name=col["name"],
            type=str(col["type"]).lower(),
            nullable=col.get("nullable", True),
        )
        for col in raw_columns
    ]

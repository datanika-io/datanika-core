"""IR document schema — frozen dataclasses per SPEC_ELT_IR_ARCHITECTURE.md §5.1.

The IR is a declarative schema-mapping document between every source and the
destination. Both ETL and ELT modes derive their schemas from the same IR.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IRColumn:
    """A single column mapping from source to target."""

    source_ref: str  # dotted-path (e.g. "payload.user.id")
    target_name: str  # destination column name
    type: str  # SQL type string (e.g. "bigint", "timestamptz", "numeric(18,4)")
    nullable: bool


@dataclass(frozen=True)
class IRSource:
    """Source descriptor — identifies what to extract."""

    kind: str  # "sql_table", "sql_database", "saas_rest", "file"
    connection_id: int
    schema: str | None = None
    table: str | None = None

    # For sql_database mode: optional list of tables to include
    table_names: list[str] | None = None


@dataclass(frozen=True)
class IRTarget:
    """Target descriptor — where ELT mode lands raw data."""

    connection_id: int
    raw_schema: str
    table: str


@dataclass(frozen=True)
class IRIncremental:
    """Incremental loading configuration."""

    mode: str  # "append" or "merge"
    cursor: str  # column name used for incremental cursor


@dataclass(frozen=True)
class IR:
    """The IR document — a complete schema mapping from source to destination.

    Stored as JSON on Upload.ir and Pipeline.ir columns (nullable, NULL = ETL-only).
    """

    ir_version: int
    source: IRSource
    columns: list[IRColumn]
    primary_key: list[str]
    target: IRTarget
    incremental: IRIncremental | None = None

    def to_dict(self) -> dict:
        """Serialize to a JSON-safe dict for storage in the ir column."""
        d: dict = {
            "ir_version": self.ir_version,
            "source": {
                "kind": self.source.kind,
                "connection_id": self.source.connection_id,
            },
            "columns": [
                {
                    "source_ref": c.source_ref,
                    "target_name": c.target_name,
                    "type": c.type,
                    "nullable": c.nullable,
                }
                for c in self.columns
            ],
            "primary_key": list(self.primary_key),
            "target": {
                "connection_id": self.target.connection_id,
                "raw_schema": self.target.raw_schema,
                "table": self.target.table,
            },
        }
        if self.source.schema is not None:
            d["source"]["schema"] = self.source.schema
        if self.source.table is not None:
            d["source"]["table"] = self.source.table
        if self.source.table_names is not None:
            d["source"]["table_names"] = list(self.source.table_names)
        if self.incremental is not None:
            d["incremental"] = {
                "mode": self.incremental.mode,
                "cursor": self.incremental.cursor,
            }
        return d

    @classmethod
    def from_dict(cls, d: dict) -> IR:
        """Deserialize from a JSON dict (e.g. from Upload.ir column)."""
        src_d = d["source"]
        source = IRSource(
            kind=src_d["kind"],
            connection_id=src_d["connection_id"],
            schema=src_d.get("schema"),
            table=src_d.get("table"),
            table_names=src_d.get("table_names"),
        )
        columns = [
            IRColumn(
                source_ref=c["source_ref"],
                target_name=c["target_name"],
                type=c["type"],
                nullable=c["nullable"],
            )
            for c in d["columns"]
        ]
        target_d = d["target"]
        target = IRTarget(
            connection_id=target_d["connection_id"],
            raw_schema=target_d["raw_schema"],
            table=target_d["table"],
        )
        incremental = None
        if "incremental" in d:
            inc_d = d["incremental"]
            incremental = IRIncremental(mode=inc_d["mode"], cursor=inc_d["cursor"])
        return cls(
            ir_version=d["ir_version"],
            source=source,
            columns=columns,
            primary_key=d["primary_key"],
            target=target,
            incremental=incremental,
        )

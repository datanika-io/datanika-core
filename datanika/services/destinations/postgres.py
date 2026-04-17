"""Postgres raw lander — COPY FROM STDIN with Arrow batches.

Per SPEC_ELT_IR_ARCHITECTURE.md §5.3: Postgres landing uses COPY FROM STDIN
with CSV format (psycopg copy_expert). Each Arrow batch is converted to
CSV and streamed to COPY.

E7 file-size guard: batches larger than FILE_MAX_BYTES (100 MB) are split
by slicing the Arrow table into sub-tables before conversion, preventing
OOM on large extracts. See Whisk doc §"Critical fix" — single multi-GB
uploads time out.
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.csv as pcsv
from sqlalchemy import create_engine, text

from datanika.services.connection_service import _build_sa_url
from datanika.services.elt_runner import FILE_MAX_BYTES

if TYPE_CHECKING:
    from datanika.services.ir.schema import IR

logger = logging.getLogger(__name__)


class PostgresLander:
    """Lands Arrow data into Postgres via COPY FROM STDIN (CSV format).

    Large Arrow tables are sliced to keep each COPY payload under
    FILE_MAX_BYTES (100 MB) per E7.
    """

    def land(
        self,
        source: object,
        ir: IR,
        destination_config: dict,
        run_id: int,
    ) -> dict:
        """Extract from dlt source, convert to Arrow, COPY into raw table."""
        sa_url = _build_sa_url(destination_config, "postgres")
        engine = create_engine(sa_url)

        total_rows = 0
        total_bytes_in = 0
        total_bytes_out = 0
        batches = 0

        try:
            # Ensure raw schema + table exist.
            with engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {ir.target.raw_schema}"))
                conn.commit()

            qualified = f"{ir.target.raw_schema}.{ir.target.table}"
            schema_ensured = False

            # Extract from source — dlt sources yield dicts or Arrow batches.
            # In Arrow mode (E6), SQL sources yield pa.Table per chunk.
            for item in source:
                if isinstance(item, (pa.Table, pa.RecordBatch)):
                    table = item if isinstance(item, pa.Table) else pa.Table.from_batches([item])
                elif isinstance(item, dict):
                    table = pa.Table.from_pylist([item])
                elif isinstance(item, list):
                    table = pa.Table.from_pylist(item)
                else:
                    continue

                if table.num_rows == 0:
                    continue

                total_bytes_in += table.nbytes

                # Ensure destination table exists (once, on first non-empty batch).
                if not schema_ensured:
                    with engine.connect() as conn:
                        _ensure_table(conn, qualified, table.schema)
                    schema_ensured = True

                # E7: split large tables into chunks <= FILE_MAX_BYTES.
                # Estimate target rows per chunk from nbytes ratio. If the
                # full table is already under the guard, this yields one chunk.
                for chunk in _split_by_bytes(table, FILE_MAX_BYTES):
                    csv_bytes = _table_to_csv_bytes(chunk)
                    total_bytes_out += len(csv_bytes)

                    raw_conn = engine.raw_connection()
                    try:
                        cursor = raw_conn.cursor()
                        cursor.copy_expert(
                            f"COPY {qualified} FROM STDIN WITH (FORMAT csv, HEADER true)",
                            io.BytesIO(csv_bytes),
                        )
                        raw_conn.commit()
                    finally:
                        raw_conn.close()

                    total_rows += chunk.num_rows
                    batches += 1

        finally:
            engine.dispose()

        return {
            "rows": total_rows,
            "bytes_in": total_bytes_in,
            "bytes_out": total_bytes_out,
            "batches": batches,
        }


def _split_by_bytes(table: pa.Table, max_bytes: int) -> list[pa.Table]:
    """Slice an Arrow table so each piece's nbytes <= max_bytes.

    nbytes is the in-memory Arrow footprint, a conservative upper bound
    for the resulting CSV size after serialization (CSV can be larger for
    strings with escapes, but also smaller for nullable integer columns).
    """
    if table.num_rows == 0 or table.nbytes <= max_bytes:
        return [table]

    # Derive rows-per-chunk from the byte ratio.
    rows_per_chunk = max(1, (table.num_rows * max_bytes) // table.nbytes)
    chunks: list[pa.Table] = []
    offset = 0
    while offset < table.num_rows:
        length = min(rows_per_chunk, table.num_rows - offset)
        chunks.append(table.slice(offset, length))
        offset += length
    return chunks


def _table_to_csv_bytes(table: pa.Table) -> bytes:
    """Serialize an Arrow table to CSV bytes for COPY FROM STDIN."""
    buf = io.BytesIO()
    pcsv.write_csv(table, buf)
    return buf.getvalue()


def _ensure_table(conn, qualified_name: str, arrow_schema: pa.Schema) -> None:
    """Create the raw table if it doesn't exist, based on Arrow schema."""
    cols = []
    for field in arrow_schema:
        pg_type = _arrow_to_pg_type(field.type)
        null = "" if field.nullable else " NOT NULL"
        cols.append(f'"{field.name}" {pg_type}{null}')

    ddl = f"CREATE TABLE IF NOT EXISTS {qualified_name} ({', '.join(cols)})"
    conn.execute(text(ddl))
    conn.commit()


def _arrow_to_pg_type(arrow_type: pa.DataType) -> str:
    """Map Arrow types to Postgres types."""
    mapping = {
        pa.int8(): "smallint",
        pa.int16(): "smallint",
        pa.int32(): "integer",
        pa.int64(): "bigint",
        pa.float16(): "real",
        pa.float32(): "real",
        pa.float64(): "double precision",
        pa.bool_(): "boolean",
        pa.date32(): "date",
        pa.date64(): "date",
    }
    if arrow_type in mapping:
        return mapping[arrow_type]
    if pa.types.is_timestamp(arrow_type):
        return "timestamptz" if arrow_type.tz else "timestamp"
    if pa.types.is_decimal(arrow_type):
        return f"numeric({arrow_type.precision},{arrow_type.scale})"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "text"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "bytea"
    return "text"  # safe fallback

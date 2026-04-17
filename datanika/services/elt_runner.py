"""ELT runner — streams Source → Arrow → parquet → destination raw schema.

Per SPEC_ELT_IR_ARCHITECTURE.md §5.3: destination adapters decide whether
parquet lands via COPY, EXTERNAL TABLE, or native Arrow ingest.

Arrow mode (E6, from Whisk architecture): SQL sources yield pa.Table per
chunk instead of dict rows. This bypasses dlt's JSON normalization entirely
— proven 5.8x speedup on 54M-row MySQL loads. See
D:/Projects/whisk/data_repos/DB data transfer architecture.md §"MySQL
Source: Arrow Mode".

File size guard (E7): DATA_WRITER__FILE_MAX_BYTES splits multi-GB Arrow
extracts into uploadable chunks. Without it, single files time out on
HTTP upload. See Whisk doc §"Critical fix".
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass

from datanika.services.ir.schema import IR

logger = logging.getLogger(__name__)

# Arrow-mode env vars per SPEC §5.3 / §16.1 + Whisk architecture.
# MUST be set BEFORE any dlt module is imported — dlt reads these at
# import time into its global config. Setting them inside stream_to_raw()
# is too late if dlt has already been imported elsewhere in the process.
#
# Prefix MUST be DATA_WRITER__ (global), NOT NORMALIZE__ — Arrow direct-
# import skips the normalize step, so NORMALIZE__ settings are ignored.
_ARROW_ENV = {
    "DATA_WRITER__FILE_MAX_ITEMS": "10000",
    "DATA_WRITER__FILE_MAX_BYTES": "100000000",  # 100 MB
    "LOAD__DELETE_COMPLETED_JOBS": "true",
}

# Apply at module import time so dlt sees them first.
for _k, _v in _ARROW_ENV.items():
    os.environ.setdefault(_k, _v)

# Exported for adapters that need the same threshold for their own
# in-memory buffers (e.g., PostgresLander CSV COPY batching).
FILE_MAX_BYTES = int(os.environ["DATA_WRITER__FILE_MAX_BYTES"])


@dataclass(frozen=True)
class StreamStats:
    """Result of stream_to_raw(). The single metering surface per spec §5.5."""

    rows: int
    bytes_in: int  # bytes pulled from source (post-decompression, pre-cast)
    bytes_out: int  # bytes written to destination raw (post-compression)
    batches: int
    duration_ms: int


def stream_to_raw(
    ir: IR,
    run_id: int,
    source_config: dict,
    destination_config: dict,
    source_type: str,
    destination_type: str,
) -> StreamStats:
    """Stream source data to destination's raw schema via Arrow/parquet.

    This is the ELT ingest path — no dlt normalization, no disk staging.
    Destination adapters handle the final landing strategy.
    """
    start_ms = int(time.time() * 1000)

    from datanika.services.destinations import get_lander

    lander = get_lander(destination_type)

    # Use dlt's source readers in Arrow mode for extraction.
    from datanika.services.dlt_runner import DltRunnerService

    runner = DltRunnerService()
    dlt_config = {
        "mode": "single_table" if ir.source.table else "full_database",
        # E6: Arrow backend for SQL sources — yields pa.Table per chunk
        # instead of dict rows. dlt_runner.build_source() passes this
        # through to sql_table()/sql_database().
        "backend": "pyarrow",
    }
    if ir.source.table:
        dlt_config["table"] = ir.source.table
    if ir.source.schema:
        dlt_config["source_schema"] = ir.source.schema

    source = runner.build_source(source_type, source_config, dlt_config, batch_size=10000)

    # Land to raw via destination adapter.
    stats = lander.land(
        source=source,
        ir=ir,
        destination_config=destination_config,
        run_id=run_id,
    )

    elapsed_ms = int(time.time() * 1000) - start_ms

    result = StreamStats(
        rows=stats["rows"],
        bytes_in=stats["bytes_in"],
        bytes_out=stats["bytes_out"],
        batches=stats["batches"],
        duration_ms=elapsed_ms,
    )

    logger.info(
        "stream_to_raw complete: run_id=%d rows=%d bytes_out=%d batches=%d elapsed=%dms",
        run_id,
        result.rows,
        result.bytes_out,
        result.batches,
        result.duration_ms,
    )

    return result

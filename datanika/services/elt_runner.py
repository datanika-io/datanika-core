"""ELT runner — P1 stub. Lands in P3.

Per SPEC_ELT_IR_ARCHITECTURE.md §5.3: streams Source → Arrow RecordBatches →
parquet files landed into the destination's raw schema. Destination adapters
decide whether parquet lands via COPY, EXTERNAL TABLE, or native Arrow ingest.
"""

from typing import Any


def stream_to_raw(ir: Any, run_id: int) -> Any:
    raise NotImplementedError(
        "ELT streaming runner lands in V2 P3 — SPEC_ELT_IR_ARCHITECTURE.md §5.3"
    )

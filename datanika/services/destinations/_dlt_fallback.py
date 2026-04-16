"""dlt fallback lander — wraps dlt for destinations without a native adapter.

Per SPEC_ELT_IR_ARCHITECTURE.md §5.3: destinations without a native RawLander
still use the ELT shape (land to raw only) but go through dlt's loader
internally. This covers the 6 non-Postgres destinations until native adapters
ship.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datanika.services.ir.schema import IR

logger = logging.getLogger(__name__)


class DltFallbackLander:
    """Wraps dlt pipeline for ELT-shaped landing on unsupported destinations."""

    def land(
        self,
        source: object,
        ir: IR,
        destination_config: dict,
        run_id: int,
    ) -> dict:
        """Use dlt's full pipeline (extract + normalize + load) to land data.

        Still ELT-shaped: lands to raw schema only. dlt handles the
        destination-specific transport.
        """
        import contextlib
        import os

        import dlt

        destination_type = "duckdb"  # inferred from config at call site

        pipeline = dlt.pipeline(
            pipeline_name=f"elt_fallback_{run_id}",
            destination=destination_type,
            dataset_name=ir.target.raw_schema,
        )

        load_info = pipeline.run(source, table_name=ir.target.table)

        # Extract stats from load info
        rows = 0
        bytes_out = 0
        try:
            trace = pipeline.last_trace
            if trace and trace.last_normalize_info:
                row_counts = trace.last_normalize_info.row_counts
                rows = sum(v for k, v in row_counts.items() if not k.startswith("_dlt_"))
            # Walk load packages for file sizes (bytes_processed)
            for pkg in load_info.load_packages:
                for job in pkg.jobs.get("completed_jobs", []):
                    if hasattr(job, "file_path"):
                        with contextlib.suppress(OSError):
                            bytes_out += os.path.getsize(job.file_path)
        except Exception:
            logger.warning("Failed to extract stats from dlt fallback", exc_info=True)

        return {
            "rows": rows,
            "bytes_in": bytes_out,  # approximate — dlt doesn't separate in/out
            "bytes_out": bytes_out,
            "batches": 1,
        }

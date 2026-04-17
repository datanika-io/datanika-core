"""Destination landers — PostgresLander file-size guard (E7).

Arrow-mode (E6) tests live in test_elt_runner.py; this file focuses on
adapter-internal behavior.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from datanika.services.destinations.postgres import _split_by_bytes, _table_to_csv_bytes


@pytest.fixture
def sample_table() -> pa.Table:
    """10k rows × 2 int64 columns ≈ 160 KB in Arrow memory."""
    n = 10_000
    return pa.table(
        {
            "id": pa.array(range(n), type=pa.int64()),
            "val": pa.array(range(n), type=pa.int64()),
        }
    )


class TestSplitByBytes:
    """E7 — _split_by_bytes splits Arrow tables at a size threshold."""

    def test_small_table_returns_single_chunk(self, sample_table):
        # Threshold much larger than table — single chunk.
        chunks = _split_by_bytes(sample_table, max_bytes=1_000_000)
        assert len(chunks) == 1
        assert chunks[0].num_rows == sample_table.num_rows

    def test_large_table_splits_into_multiple_chunks(self, sample_table):
        # Threshold a quarter of the table size — expect ~4 chunks.
        max_bytes = sample_table.nbytes // 4
        chunks = _split_by_bytes(sample_table, max_bytes=max_bytes)
        assert len(chunks) >= 3  # 3-5 depending on row math
        total_rows = sum(c.num_rows for c in chunks)
        assert total_rows == sample_table.num_rows
        # Each chunk should be at or under the threshold.
        for c in chunks:
            assert c.nbytes <= max_bytes * 2  # allow 2x slack for metadata

    def test_empty_table_returns_empty_list_like(self):
        empty = pa.table({"id": pa.array([], type=pa.int64())})
        chunks = _split_by_bytes(empty, max_bytes=1_000_000)
        assert len(chunks) == 1
        assert chunks[0].num_rows == 0

    def test_single_row_never_splits_below_one(self, sample_table):
        # Pathologically tiny max_bytes — still yields at-least-1-row chunks.
        chunks = _split_by_bytes(sample_table, max_bytes=1)
        assert all(c.num_rows >= 1 for c in chunks)
        total = sum(c.num_rows for c in chunks)
        assert total == sample_table.num_rows


class TestTableToCsvBytes:
    def test_roundtrip(self, sample_table):
        csv_bytes = _table_to_csv_bytes(sample_table)
        # Header + 10k lines.
        assert csv_bytes.count(b"\n") == sample_table.num_rows + 1
        # pyarrow CSV quotes column names by default.
        assert csv_bytes.startswith(b'"id","val"\n')


class TestFileMaxBytesConstant:
    """E6/E7 — FILE_MAX_BYTES is set from env and used by landers."""

    def test_default_is_100mb(self):
        from datanika.services.elt_runner import FILE_MAX_BYTES

        # Default from _ARROW_ENV in elt_runner.py.
        assert FILE_MAX_BYTES == 100_000_000

    def test_env_vars_applied_at_import(self):
        import os

        # All three Arrow env vars should be in os.environ after
        # elt_runner import (which happens via the test imports above).
        assert os.environ.get("DATA_WRITER__FILE_MAX_BYTES") == "100000000"
        assert os.environ.get("DATA_WRITER__FILE_MAX_ITEMS") == "10000"
        assert os.environ.get("LOAD__DELETE_COMPLETED_JOBS") == "true"

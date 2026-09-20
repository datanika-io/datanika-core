"""An initial value typed as text meets the cursor column's own type (core#1414).

The upload form stores **Initial value** as text. dlt compares the initial value with each row's
cursor in Python, and a string never compares with an integer, a decimal, a date or a timestamp: the
run failed at extract. Measured before the fix on dlt 1.21.0, for each of those column types, with
``IncrementalCursorInvalidCoercion`` or ``TypeError``, while the same value given as the column's
own type loaded.

The loader now reads the cursor column's type from the source and converts the text to it. These
tests drive the real ``DltRunnerService.execute`` from a real SQLite file into a real DuckDB file,
and read the destination back rather than trusting a count.
"""

from __future__ import annotations

import sqlite3

import duckdb
import pytest

from datanika.services.dlt_runner import DltRunnerError, DltRunnerService

ROWS = 5


def _seed(path, column_type: str, values: list) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(f"CREATE TABLE events (id INTEGER PRIMARY KEY, cursor {column_type} NOT NULL)")
        con.executemany("INSERT INTO events VALUES (?, ?)", list(enumerate(values, start=1)))
        con.commit()
    finally:
        con.close()


def _load(tmp_path, initial_value) -> list[int]:
    incremental = {"cursor_path": "cursor"}
    if initial_value is not None:
        incremental["initial_value"] = initial_value
    DltRunnerService(pipelines_dir=str(tmp_path / "dlt")).execute(
        pipeline_id=1,
        source_type="sqlite",
        source_config={"path": str(tmp_path / "source.sqlite")},
        destination_type="duckdb",
        destination_config={"path": str(tmp_path / "destination.duckdb")},
        dlt_config={
            "mode": "single_table",
            "table": "events",
            "write_disposition": "append",
            "incremental": incremental,
        },
        dataset_name="probe",
        run_id=1,
    )
    con = duckdb.connect(str(tmp_path / "destination.duckdb"))
    try:
        return [r[0] for r in con.execute('SELECT id FROM "probe"."events" ORDER BY id').fetchall()]
    finally:
        con.close()


#: (declared type, the five cursor values, the text a user types for the third one)
TYPED = [
    pytest.param("INTEGER", [10, 20, 30, 40, 50], "30", id="integer"),
    pytest.param("NUMERIC(10,2)", [10.5, 20.5, 30.5, 40.5, 50.5], "30.5", id="numeric"),
    pytest.param(
        "DATE",
        ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        "2024-01-03",
        id="date",
    ),
    pytest.param(
        "DATETIME",
        [f"2024-01-0{i} 10:00:00.000000" for i in range(1, 6)],
        "2024-01-03T10:00:00",
        id="datetime",
    ),
]


@pytest.mark.parametrize("column_type, values, typed", TYPED)
def test_an_initial_value_typed_as_text_loads_the_rows_at_or_past_it(
    tmp_path, column_type, values, typed
):
    _seed(tmp_path / "source.sqlite", column_type, values)

    assert _load(tmp_path, typed) == [3, 4, 5]


def test_control_a_text_cursor_keeps_its_text_initial_value(tmp_path):
    """A text column compares text, so the value is not converted."""
    _seed(tmp_path / "source.sqlite", "TEXT", ["a", "b", "c", "d", "e"])

    assert _load(tmp_path, "c") == [3, 4, 5]


def test_control_no_initial_value_loads_every_row(tmp_path):
    _seed(tmp_path / "source.sqlite", "INTEGER", [10, 20, 30, 40, 50])

    assert _load(tmp_path, None) == [1, 2, 3, 4, 5]


def test_a_value_that_is_not_the_columns_type_is_refused_by_name(tmp_path):
    """A clear refusal naming the column and its type, not dlt's comparison error."""
    _seed(tmp_path / "source.sqlite", "INTEGER", [10, 20, 30, 40, 50])

    with pytest.raises(DltRunnerError) as raised:
        _load(tmp_path, "thirty")

    message = str(raised.value)
    assert "thirty" in message and "cursor" in message and "INTEGER" in message.upper()

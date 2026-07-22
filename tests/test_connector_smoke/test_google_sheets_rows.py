"""Live row probe for the Google Sheets builder (core#545).

The last credentialed builder in the #545 audit. Everything provable without an
account lives in ``tests/test_services/test_source_builders_move_rows.py``; this
one needs a real Google identity, so it belongs here, behind
``DATANIKA_CONNECTOR_SMOKE=1``.

## Why it exists

``_build_google_sheets_source`` was mocked-only, like the other eight. The audit
found two of those genuinely unable to move a row (#550 MongoDB, #551 Kafka), so
"it is only a thin wrapper" was not a safe assumption anywhere.

## What it asserts, and what it deliberately does not

It drives the real ``DltRunnerService.execute()`` — the same entrypoint
``upload_tasks.py`` calls — into a scratch DuckDB, then **reads the destination
back**. Never ``result["rows_loaded"]``: that is the pipeline's own report of its
work, and #492 is precisely the case where the count looked plausible while the
contents were wrong.

It proves rows arrive. It says nothing about multi-sheet selection, large sheets,
or type coercion.

## Fixture

A throwaway sheet owned by the founder, shared **read-only** with the smoke
service account (``qa-smoke@datanika.iam.gserviceaccount.com``):

    id,name,price
    1,alpha,100
    2,beta,200
    3,gamma,300

The spreadsheet id below is **not a secret** — it grants nothing without the
service-account key, exactly like the Paddle sandbox fixture ids in
``datanika-cloud``. The sheet is private; only that SA and the owner can read it.

Google Drive's CSV import names the first worksheet ``untitled``, and dlt uses the
worksheet name as the table name — hence ``untitled`` rather than a tidy name.
Left as-is on purpose: renaming the tab would make the fixture prettier and the
probe less representative of what a user's own imported CSV actually produces.

⚠️ **Provisioning note (2026-07-22):** the Sheets API had to be enabled on GCP
project ``datanika`` (628591595881) before any of this worked — it was off, and
so was the Drive API. The credentials existed all along; the *API* did not. If
this probe starts failing with HTTP 403 "has not been used in project", that is
the enablement having been reverted, not a credential problem.

Locally::

    set -a && source secrets/qa-connectors.env && set +a
    BIGQUERY_CREDENTIALS_FILE=secrets/qa-bigquery-sa.json \\
    DATANIKA_CONNECTOR_SMOKE=1 uv run pytest \\
        tests/test_connector_smoke/test_google_sheets_rows.py -v
"""

from __future__ import annotations

import duckdb

from datanika.services.dlt_runner import DltRunnerService

# Fixture spreadsheet — see the module docstring on why this is not a secret.
SPREADSHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1iZ1Y1XAgaoFJwtVyHbgZYv9n7scIbSUBmqPeo_hoPAk/edit"
)

# Drive's CSV conversion names the first worksheet "untitled"; dlt derives the
# destination table name from the worksheet name.
WORKSHEET_TABLE = "untitled"

EXPECTED_ROWS = [("alpha", 100), ("beta", 200), ("gamma", 300)]


def test_google_sheets_moves_rows_into_the_destination(require_env, tmp_path):
    """Extract the fixture sheet into DuckDB and assert the CONTENTS landed.

    ``require_env`` fails rather than skips when the credential is absent — a
    probe that skips on a missing dependency reports green while testing
    nothing, which is how a dead connector stays green in the matrix (core#407).
    """
    env = require_env("BIGQUERY_CREDENTIALS_FILE")
    with open(env["BIGQUERY_CREDENTIALS_FILE"], encoding="utf-8") as fh:
        service_account_json = fh.read()

    db_path = str(tmp_path / "scratch.duckdb")
    runner = DltRunnerService(pipelines_dir=str(tmp_path / "dlt"))
    runner.execute(
        pipeline_id=1,
        source_type="google_sheets",
        source_config={
            "spreadsheet_url": SPREADSHEET_URL,
            "service_account_json": service_account_json,
        },
        destination_type="duckdb",
        destination_config={"path": db_path},
        dlt_config={"write_disposition": "replace"},
        dataset_name="probe",
        run_id=1,
    )

    con = duckdb.connect(db_path)
    try:
        rows = con.execute(
            f'SELECT name, price FROM "probe"."{WORKSHEET_TABLE}" ORDER BY price'
        ).fetchall()
    finally:
        con.close()

    assert rows == EXPECTED_ROWS, (
        "google_sheets did not deliver the row CONTENTS into DuckDB. Getting one "
        "row per worksheet, or the worksheet names instead of their values, is "
        f"#492's failure mode — which a kwargs assertion cannot see. Got: {rows}"
    )

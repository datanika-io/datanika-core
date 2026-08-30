"""Real-data row probes for the source builders that need no credentials (core#545).

## Why this file exists

#492 (P0) shipped for months: `csv`/`json`/`parquet`/`s3` loaded a **file listing**
instead of file contents. 2,500 green tests could not have caught it, because every
test in ``TestBuildFileSource`` patches ``dlt_runner.filesystem`` and asserts *the
kwargs we passed in* — what dlt actually **yields** was unobservable to all of them.

``tests/test_services/test_dlt_runner.py`` has 140 tests and 103 ``@patch``
decorators; ``sql_database`` is patched 24 times, *more* than ``filesystem``'s 7.
The mocking pattern is not specific to files — it is how that module is tested
throughout. So the audit question is not "were we sloppy about files" but **"which
builders have ever been proven to move a row?"**

This suite answers that for the builders provable **without any credential, any
vendor account, or staging**: a real source, a real dlt pipeline, a real DuckDB
destination, and the assertion made by **reading the destination back** — never off
``result["rows_loaded"]``, which is a number the pipeline reports about itself.

Deliberately NOT in ``tests/test_connector_smoke/``: that directory is gated behind
``DATANIKA_CONNECTOR_SMOKE=1`` for the whole collection, so these would be skipped
in PR CI — which is exactly where a credential-free probe belongs.

## What this does not do (core#545 "not in scope")

It does not convert the 103 mocked tests to integration tests. Mocks are the right
tool for "did we pass the right kwargs"; the defect was that **nothing else
existed**. This is the thin layer that was missing, not a replacement.

## Coverage limits, stated here rather than left implicit

- One resource / one collection per builder. These prove *rows arrive*, not that
  pagination, incremental cursors, auth flows or schema contracts are correct.
- ``google_sheets`` is absent: it needs an account.
- ``saas`` is covered only at the **shared fallback spine** (see
  ``TestSaasRestFallbackMovesRows``), not per connector. Each connector's own auth
  assembly and default resource list still need credentials. ``google_analytics``,
  ``google_ads`` and ``facebook_ads`` are excluded outright — #543 says they cannot
  run at all.
- A pass here says the builder can move a row **today, on this shape of data**. It
  is a floor, not a guarantee.
"""

from __future__ import annotations

import http.server
import json
import os
import threading
import time

import duckdb
import pytest

from datanika.services.dlt_runner import DltRunnerService

# ── The fixture payloads ────────────────────────────────────────────────────────
# Distinct values per row so a "rows landed" assertion cannot be satisfied by the
# same record repeated, and a column with a non-trivial value so we can prove the
# *contents* arrived rather than a listing of them (the #492 failure mode).
WIDGETS = [
    {"id": 1, "name": "alpha", "price": 100},
    {"id": 2, "name": "beta", "price": 200},
    {"id": 3, "name": "gamma", "price": 300},
]


class _JsonHandler(http.server.BaseHTTPRequestHandler):
    """Serves the fixture over a real socket — no `requests` mocking anywhere.

    #404 established this pattern: it proved redirect-hop behaviour against a real
    server rather than by reading `requests` internals. Same reasoning here — the
    whole point is to observe what dlt *yields*, which a mock cannot show.
    """

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path.startswith("/widgets"):
            body = json.dumps(WIDGETS).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def log_message(self, *args):  # silence per-request noise in pytest output
        pass


@pytest.fixture
def json_api():
    """A real HTTP server on an ephemeral loopback port. Yields its base URL."""
    server = http.server.HTTPServer(("127.0.0.1", 0), _JsonHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def allow_loopback(monkeypatch):
    """Let the egress guard reach a loopback test server.

    Three patches, not one — this is the shape established in
    ``tests/test_services/test_openapi_fetch.py`` and it is not optional:

    1. ``dlt_runner.validate_egress_host`` — the pre-flight check in
       ``_rest_api_from_parts``.
    2. ``egress_guard.validate_egress_host`` — the guarded session re-validates
       every request the worker actually makes.
    3. ``egress_guard.resolve_public_ip`` — since #405 the session resolves **once**
       and pins the address, so no-oping only the validators leaves the adapter
       still refusing to connect. Patching the validator alone silently does not
       reach the server, which is the trap #441 recorded.

    The guard's real behaviour is covered by ``tests/test_security/`` — this fixture
    exists so a row probe can run offline, not to weaken the guard.
    """
    monkeypatch.setattr("datanika.services.dlt_runner.validate_egress_host", lambda url: None)
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
    )


def _extract_load(tmp_path, source_type: str, source_config: dict, dlt_config: dict) -> str:
    """Drive the REAL ``DltRunnerService.execute()`` into a scratch DuckDB.

    Same entrypoint the upload task calls (``upload_tasks.py`` → ``runner.execute``),
    so this exercises dispatch + build + extract + load, not just the builder.
    Returns the DuckDB path; the caller reads it back.
    """
    db_path = str(tmp_path / "scratch.duckdb")
    runner = DltRunnerService(pipelines_dir=str(tmp_path / "dlt"))
    runner.execute(
        pipeline_id=1,
        source_type=source_type,
        source_config=source_config,
        destination_type="duckdb",
        destination_config={"path": db_path},
        dlt_config={"write_disposition": "replace", **dlt_config},
        dataset_name="probe",
        run_id=1,
    )
    return db_path


def _rows(db_path: str, table: str) -> list[tuple]:
    """Read the destination back. The assertion source of truth.

    Deliberately NOT ``result["rows_loaded"]``: that is the pipeline's own report
    of its work. #492 is precisely the case where the count looked plausible and
    the contents were wrong — one row per file, being the file's *name*.
    """
    con = duckdb.connect(db_path)
    try:
        return con.execute(f'SELECT name, price FROM "probe"."{table}" ORDER BY price').fetchall()
    finally:
        con.close()


class TestRestApiSourceMovesRows:
    """``_build_rest_api_source`` — previously mocked-only (core#545)."""

    def test_rows_land_in_the_destination(self, tmp_path, json_api, allow_loopback):
        db_path = _extract_load(
            tmp_path,
            "rest_api",
            {"base_url": json_api},
            {"resources": [{"name": "widgets", "endpoint": {"path": "widgets"}}]},
        )

        rows = _rows(db_path, "widgets")

        assert rows == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "rest_api did not deliver the record CONTENTS into DuckDB. Rows landing "
            "with the wrong shape — e.g. one row per resource naming it — is #492's "
            "failure mode, which a kwargs assertion cannot see."
        )


class TestOpenApiSourceMovesRows:
    """``_build_openapi_source`` — previously mocked-only (core#545).

    Distinct from rest_api despite sharing ``_rest_api_from_parts``: it reads the
    catalog off the *connection* and selects via ``dlt_config["resource_names"]``,
    stripping the private ``columns`` / ``_source`` keys before handing resources
    to dlt. That stripping is a real transformation with no other live coverage.
    """

    def test_rows_land_for_the_selected_resource(self, tmp_path, json_api, allow_loopback):
        db_path = _extract_load(
            tmp_path,
            "openapi",
            {
                "base_url": json_api,
                "resources": [
                    {
                        "name": "widgets",
                        "endpoint": {"path": "widgets"},
                        # Private keys the builder must strip; dlt rejects unknown keys.
                        "columns": {"id": {"data_type": "bigint"}},
                        "_source": "probe-spec",
                    }
                ],
            },
            {"resource_names": ["widgets"]},
        )

        rows = _rows(db_path, "widgets")

        assert rows == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "openapi did not deliver record contents into DuckDB — check that "
            "`columns`/`_source` stripping still produces a resource dlt accepts."
        )


class TestFileSourceMovesRows:
    """``_build_file_source`` — the last outstanding row of the audit (core#545).

    This is the builder #492 actually broke, and until now its only live assertion
    was ``e2e/tests/golden-path.spec.ts``. That spec reads the **Runs table's
    ``Rows`` column** (``e2e/fixtures/data.ts`` → ``runUploadAndAwait``, cell 5),
    which is the pipeline's own report of its own work — precisely the number #492
    made look plausible while the contents were a listing. So a green golden path
    does not retire this row. Reading the destination back does.

    No credential, no Docker, no network: a real CSV on disk, through the real
    ``DltRunnerService.execute()``, into a real DuckDB file.

    Scope, stated rather than implied (core#684 closed the gap #545 left):
    ``csv``, ``json`` (all three shapes ``_json_chunks`` handles) and ``parquet``
    are measured here.

    **``s3`` is NOT measured, and is deferred on a named blocker** rather than
    left to look done: it needs a reachable bucket plus a key pair, which is a
    credential this suite deliberately does not take (see the module docstring —
    everything here must run in PR CI with no account). Covering it means either
    a `minio` service in the compose stack or an AWS key in
    ``plans/SECRETS_INVENTORY.md``; until one exists, ``s3``'s only assertions
    are the mocked kwargs tests in ``test_dlt_runner.py``, which are the exact
    kind that could not see #492.

    What ``s3`` *does* share with the three above is the format resolution path:
    it carries no format in its type, so ``_resolve_file_format`` falls to the
    glob's extension and **raises** on a bare ``*`` rather than guessing. That
    refusal is unit-tested. The unmeasured part is the transport.
    """

    @staticmethod
    def _write_csv(directory) -> None:
        header = "id,name,price\n"
        body = "".join(f"{w['id']},{w['name']},{w['price']}\n" for w in WIDGETS)
        (directory / "widgets.csv").write_text(header + body, encoding="utf-8")

    def test_rows_land_in_the_destination(self, tmp_path):
        drop = tmp_path / "drop"
        drop.mkdir()
        self._write_csv(drop)

        db_path = _extract_load(
            tmp_path,
            "csv",
            {"bucket_url": str(drop)},
            {"file_glob": "widgets.csv"},
        )

        rows = _rows(db_path, "widgets")

        assert rows == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "csv did not deliver the record CONTENTS into DuckDB. That is #492's "
            "exact shape: `filesystem()` on its own is a *lister*, so losing the "
            "`| read_csv()` pipe yields one row per file describing the file — and "
            "a row COUNT still looks healthy, which is why the count is not the "
            "assertion here."
        )

    # ── core#684: the formats #545 left unmeasured ──────────────────────────
    #
    # #545 proved `csv` only, and recorded the reason the rest were deferred:
    # "json and parquet share the same lister-plus-reader assembly and differ
    # solely in the transformer `_build_format_reader` returns."
    #
    # That argument is weaker than it looks, and reading the code is what shows
    # it. `csv` and `parquet` are dlt's own `read_csv` / `read_parquet`. **`json`
    # is ours** — `dlt_runner.read_json`, a hand-written `@dlt.transformer` that
    # exists because dlt's `read_jsonl` is JSON-Lines-only while our default glob
    # for the `json` type is `*.json`. Feeding an array to `read_jsonl` does not
    # raise: it parses as one value, and dlt writes a parent row with no business
    # columns plus a `__value` child table — #492 again, in a new costume.
    #
    # So `_json_chunks` detects the shape, in three branches, none of which had
    # ever been observed to put a record in a destination. Each gets its own test
    # below, because "shares the assembly" is exactly the reasoning that has to
    # be false for any of this to be worth writing.

    @staticmethod
    def _payload_lines() -> list[str]:
        return [json.dumps(w) for w in WIDGETS]

    def test_json_array_rows_land_in_the_destination(self, tmp_path):
        """Branch 1: a `[...]` array — the shape our default `*.json` glob implies."""
        drop = tmp_path / "drop"
        drop.mkdir()
        (drop / "widgets.json").write_text(json.dumps(WIDGETS), encoding="utf-8")

        db_path = _extract_load(
            tmp_path,
            "json",
            {"bucket_url": str(drop)},
            {"file_glob": "widgets.json"},
        )

        assert _rows(db_path, "widgets") == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "json (array) did not deliver record CONTENTS into DuckDB. Losing the "
            "reader pipe yields one row per file describing the file; routing an "
            "array through `read_jsonl` instead yields a parent row with no "
            "business columns and a `__value` child table. Both report a healthy "
            "row count, which is why the count is not the assertion."
        )

    def test_jsonl_rows_land_in_the_destination(self, tmp_path):
        """Branch 2: JSON Lines, streamed a line at a time."""
        drop = tmp_path / "drop"
        drop.mkdir()
        payload = "\n".join(self._payload_lines()) + "\n"
        (drop / "widgets.jsonl").write_text(payload, encoding="utf-8")

        db_path = _extract_load(
            tmp_path,
            "json",
            {"bucket_url": str(drop)},
            {"file_glob": "widgets.jsonl"},
        )

        assert _rows(db_path, "widgets") == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "json (JSON Lines) did not deliver record CONTENTS into DuckDB."
        )

    def test_pretty_printed_single_object_lands_as_one_record(self, tmp_path):
        """Branch 3: one indented object spanning many lines.

        Reached only through `_json_chunks`' `except` fallback: the first line is
        a bare `{`, which fails to parse as a line-delimited record, and the file
        is then re-read whole. A silent regression here would look like an empty
        load, not an error.
        """
        drop = tmp_path / "drop"
        drop.mkdir()
        (drop / "widget.json").write_text(json.dumps(WIDGETS[0], indent=2), encoding="utf-8")

        db_path = _extract_load(
            tmp_path,
            "json",
            {"bucket_url": str(drop)},
            {"file_glob": "widget.json"},
        )

        assert _rows(db_path, "widget") == [("alpha", 100)], (
            "a pretty-printed single JSON object did not land as one record"
        )

    def test_parquet_rows_land_in_the_destination(self, tmp_path):
        """`read_parquet` is dlt's, but the pipe that reaches it is ours."""
        import pyarrow
        import pyarrow.parquet

        drop = tmp_path / "drop"
        drop.mkdir()
        pyarrow.parquet.write_table(
            pyarrow.table(
                {
                    "id": [w["id"] for w in WIDGETS],
                    "name": [w["name"] for w in WIDGETS],
                    "price": [w["price"] for w in WIDGETS],
                }
            ),
            drop / "widgets.parquet",
        )

        db_path = _extract_load(
            tmp_path,
            "parquet",
            {"bucket_url": str(drop)},
            {"file_glob": "widgets.parquet"},
        )

        assert _rows(db_path, "widgets") == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "parquet did not deliver record CONTENTS into DuckDB."
        )


def _docker_available() -> bool:
    try:
        import docker

        docker.from_env().ping()
        return True
    except Exception:
        return False


# In CI this must FAIL rather than skip: a probe that skips when its dependency is
# missing reports green while testing nothing, which is strictly worse than red
# (core#407, and the same rule the connector-smoke conftest states). Locally,
# without Docker, skipping is the honest answer — you have not disproved anything.
requires_docker = pytest.mark.skipif(
    not _docker_available() and not os.environ.get("CI"),
    reason="Docker unavailable locally; in CI this is a hard failure, not a skip",
)


def await_setup(what: str, thunk, attempts: int = 12, delay: float = 1.0):
    """Retry a container **setup** step until it succeeds, then return its result.

    ── Read this before extending it (core#578) ────────────────────────────────
    This retries *setup only* — starting a container, connecting to it, seeding
    it. It must **never** wrap an assertion, and it must never wrap the call to
    `DltRunnerService.execute()`.

    The distinction is the whole point and it is not stylistic:

      * a retry around **container startup** hides nothing. The container being
        slow is not a fact about our product; a probe that fails on it reports
        something untrue.
      * a retry around an **assertion** hides a product bug. It converts "this
        is broken half the time" into green, which is precisely the class of
        defect this file exists to catch (#492, #550, #551 were all invisible
        to 103 mocked tests).

    So: if you find yourself wanting to retry something and cannot say which of
    those two it is, it is the second one. Leave it failing.

    Why it exists: `MongoDbContainer` returns once mongod logs "ready", but auth
    initialisation can still be in flight, so the first `insert_many` races it.
    Engineering hit exactly that — a full-suite run failed inside the probe's own
    pymongo seeding, before any product code ran, and passed in isolation. That
    is a readiness race, not a flaky test, and it is worth fixing at the root
    rather than moving the suite somewhere nobody reads.
    """
    last: Exception | None = None
    for _ in range(attempts):
        try:
            return thunk()
        except Exception as exc:  # noqa: BLE001 — any setup failure is retryable
            last = exc
            time.sleep(delay)
    raise AssertionError(
        f"container setup never became ready: {what} (after {attempts} attempts, "
        f"{attempts * delay:.0f}s). Last error: {type(last).__name__}: {last}"
    ) from last


class TestSaasRestFallbackMovesRows:
    """The SaaS REST fallback — shared machinery for ten connectors (core#545).

    ## Why this one test covers ten connectors

    `stripe`, `github`, `hubspot`, `salesforce`, `shopify`, `jira`, `slack`, `zendesk`,
    `airtable` and `notion` all reach `_rest_api_fallback()` inside an
    `except ImportError`. **That branch is the production path**, not an edge case:
    none of the verified sources ship — checked all ten, every one absent — and the
    Dockerfile says so deliberately ("fallback when not installed via `dlt init` …
    avoids dependency conflicts in Docker").

    So what runs in production for those ten is: per-connector base URL + auth +
    default resource list → `_rest_api_fallback` → `rest_api_source`. This exercises
    that spine end to end.

    ## Why salesforce is the representative

    It is the only one of the ten whose **host** comes from config
    (`instance_url`); the rest hardcode it (`api.stripe.com`) or fix the suffix
    (`{store}.myshopify.com`). So it is the only one that can be pointed at a local
    server without patching the source — the others would need their URL rewritten,
    which would test the rewrite rather than the connector.

    ## What this does NOT cover, per connector

    Each connector's own auth assembly and default resource list are still unproven
    — those differ per connector and mostly need credentials. This proves the shared
    fallback spine yields rows, not that any specific vendor integration works.
    `pipedrive` / `freshdesk` / `asana` already have live row evidence from the
    nightly Wave-1 suite (core#311), so the fallback has been exercised against real
    vendors too — just not anywhere PR CI can see.
    """

    def test_the_fallback_spine_delivers_rows(self, tmp_path, json_api, allow_loopback):
        db_path = _extract_load(
            tmp_path,
            "salesforce",
            {"instance_url": json_api, "access_token": "probe-token"},
            # Override the default Account/Contact/Opportunity list with the one
            # resource the local server serves. The override path itself is what
            # #532 fixed, so exercising it is deliberate.
            {"resources": [{"name": "widgets", "endpoint": {"path": "widgets"}}]},
        )

        rows = _rows(db_path, "widgets")

        assert rows == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "the SaaS REST fallback did not deliver record contents into DuckDB — "
            "this is the path ten connectors run in production, since no verified "
            "source ships."
        )


@requires_docker
class TestSqlDatabaseSourceMovesRows:
    """The SQL path — the last builder core#545 listed as unproven.

    Its only prior evidence was **one manual prod run** (landing#280, 19 rows
    verified in the destination). Everything automated was mocked: ``sql_database``
    is patched 24 times in ``test_dlt_runner.py``, more than ``filesystem``'s 7.

    #545 argues this is the **lowest-risk** builder, and that argument is sound:
    ``sql_database()`` yields rows by construction, so there is no
    missing-transformer shape for #492's mechanism to recur in. Worth being precise
    about what that means though — it says the *specific* defect cannot recur, not
    that the path works. Credentials assembly, drivername mapping and the
    user→username rename are all real transformations with no live coverage, and
    #550 is exactly what an unexercised credentials path looks like one connector
    over.

    Both modes are covered because they call different dlt functions:
      full_database → sql_database()
      single_table  → sql_table()
    """

    @staticmethod
    def _seed(postgres) -> dict:
        """Create the fixture table and return the connection config the app stores."""
        import sqlalchemy

        engine = sqlalchemy.create_engine(postgres.get_connection_url())
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("CREATE TABLE widgets (id int, name text, price int)"))
            conn.execute(
                sqlalchemy.text(
                    "INSERT INTO widgets VALUES (1,'alpha',100),(2,'beta',200),(3,'gamma',300)"
                )
            )
        engine.dispose()
        # The shape ConnectionState saves: note `user`, which _to_dlt_credentials
        # renames to `username`. That rename is only exercised for real here.
        return {
            "host": postgres.get_container_host_ip(),
            "port": int(postgres.get_exposed_port(5432)),
            "user": postgres.username,
            "password": postgres.password,
            "database": postgres.dbname,
        }

    def test_full_database_mode_moves_rows(self, tmp_path):
        from testcontainers.postgres import PostgresContainer

        with PostgresContainer("postgres:16-alpine") as postgres:
            # SETUP retry only — see await_setup's docstring. The load below is
            # never retried.
            config = await_setup("postgres accepting writes", lambda: self._seed(postgres))
            db_path = _extract_load(
                tmp_path,
                "postgres",
                config,
                {"mode": "full_database", "table_names": ["widgets"]},
            )
            rows = _rows(db_path, "widgets")

        assert rows == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "sql_database() did not deliver row contents into DuckDB."
        )

    def test_single_table_mode_moves_rows(self, tmp_path):
        from testcontainers.postgres import PostgresContainer

        with PostgresContainer("postgres:16-alpine") as postgres:
            # SETUP retry only — see await_setup's docstring. The load below is
            # never retried.
            config = await_setup("postgres accepting writes", lambda: self._seed(postgres))
            db_path = _extract_load(
                tmp_path,
                "postgres",
                config,
                {"mode": "single_table", "table": "widgets"},
            )
            rows = _rows(db_path, "widgets")

        assert rows == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "sql_table() did not deliver row contents into DuckDB."
        )


@requires_docker
class TestMongoDbSourceMovesRows:
    """``_build_mongodb_source`` — authenticate against a real mongod, assert rows.

    **The marker came off because this started passing (core#550).** A real
    mongod, seeded through pymongo, extracted by our own ``mongodb_source`` (not
    a dlt verified source) into DuckDB. It used to fail at authentication, and
    the cause was the builder rather than this harness — measured on one
    container with one set of credentials, varying only the auth database:

        mongodb://u:p@host:port/probedb                   -> Authentication failed
        mongodb://u:p@host:port/probedb?authSource=admin  -> OK
        mongodb://u:p@host:port/admin                     -> OK

    The builder emitted the first shape. MongoDB users conventionally live in
    ``admin`` (``MONGO_INITDB_ROOT_USERNAME``, Atlas, every managed provider),
    and the database in a Mongo URI path doubles as the **auth** database — so
    every authenticated deployment was rejected. Unauthenticated mongod is
    unaffected, which is what a local dev instance looks like and why this went
    unnoticed.

    ``strict=True`` did its job: the fix XPASSed, pytest turned that into a
    failure, and the marker had to come off. It could not land silently.
    """

    def test_rows_land_in_the_destination(self, tmp_path):
        from testcontainers.mongodb import MongoDbContainer

        with MongoDbContainer("mongo:7") as mongo:
            # SETUP retry only (core#578). This exact line is where Engineering
            # saw a full-suite run fail and an isolated run pass: the container
            # logs "ready" before auth initialisation has finished, so the first
            # insert races it. Retrying the seed hides nothing about the product;
            # the load and the assertion below are never retried.
            def _seed():
                client = mongo.get_connection_client()
                client["probedb"]["widgets"].delete_many({})
                client["probedb"]["widgets"].insert_many([dict(w) for w in WIDGETS])

            await_setup("mongod accepting authenticated writes", _seed)

            db_path = _extract_load(
                tmp_path,
                "mongodb",
                {
                    "host": mongo.get_container_host_ip(),
                    "port": int(mongo.get_exposed_port(27017)),
                    "user": mongo.username,
                    "password": mongo.password,
                    "database": "probedb",
                },
                {"collection_names": ["widgets"]},
            )

            rows = _rows(db_path, "widgets")

        assert rows == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "mongodb did not deliver document contents into DuckDB."
        )


@requires_docker
class TestKafkaSourceMovesRows:
    """``_build_kafka_source`` — produce to a real broker, assert rows land.

    **The marker came off because this started passing (core#551).** It was
    ``xfail(strict=True)``: ``_build_kafka_source`` did
    ``from kafka import kafka_consumer``, the name of the **dlt verified source**
    created by ``dlt init kafka`` rather than of ``kafka-python`` (which exports
    ``KafkaConsumer``), and neither shipped — so the builder always raised and
    every Kafka upload failed. ``strict=True`` was the point: the moment Kafka
    worked, the XPASS became a failure and forced this rewrite, so the fix could
    not land silently and the test could not rot into asserting the bug.

    It earned that keep twice over. Fixing the builder was not enough — the run
    still died in ``execute()`` with
    ``TypeError: Pipeline.run() got an unexpected keyword argument 'topics'``,
    because ``topics`` was missing from ``INTERNAL_CONFIG_KEYS`` along with
    fifteen other builder keys (``endpoints``, ``owner``, ``repo``,
    ``start_date`` …). Driving the real ``execute()`` is what surfaced that;
    a builder-level test would have gone green over it.

    Note what the nightly Kafka smoke does and does not prove: it lists topics
    with ``kafka-python`` installed *by the workflow*, so it proves the sandbox
    is reachable. It never touches this builder. That is #545's thesis exactly —
    a green about connectivity read as a green about rows.
    """

    def test_rows_land_in_the_destination(self, tmp_path):
        from testcontainers.kafka import KafkaContainer

        with KafkaContainer() as kafka:
            from kafka import KafkaProducer

            # SETUP retry only (core#578) — a broker that has logged startup can
            # still refuse the first producer connection while it elects a
            # controller. The load and the assertion below are never retried.
            producer = await_setup(
                "kafka broker accepting producers",
                lambda: KafkaProducer(
                    bootstrap_servers=kafka.get_bootstrap_server(),
                    value_serializer=lambda v: json.dumps(v).encode(),
                ),
            )
            for widget in WIDGETS:
                producer.send("widgets", widget)
            producer.flush()

            db_path = _extract_load(
                tmp_path,
                "kafka",
                {"bootstrap_servers": kafka.get_bootstrap_server(), "group_id": "probe"},
                {"topics": ["widgets"]},
            )

            rows = _rows(db_path, "widgets")

        assert rows == [("alpha", 100), ("beta", 200), ("gamma", 300)]

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

import contextlib
import http.server
import inspect
import json
import os
import re
import threading
import time

import duckdb
import pytest

from datanika.services.dlt_runner import DltRunnerError, DltRunnerService

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

    ``s3`` is measured too, in ``TestS3FileSourceMovesRows`` below, against a real
    MinIO container (core#684).

    ⚠️ **This paragraph used to say ``s3`` was "NOT measured, deferred on a named
    blocker" — and it kept saying that AFTER the s3 tests landed in the same
    file.** Corrected 2026-08-31. It is worth leaving the correction visible
    rather than silently rewriting, because a docstring asserting that a thing is
    uncovered, sitting a few hundred lines above the tests that cover it, is the
    same defect this module exists to catch: a claim about coverage that nobody
    re-derived. Scope claims go stale in the direction that flatters nobody —
    check them against the class list, not against memory.

    What every file format here shares is the format resolution path: a type that
    carries no format falls to the glob's extension and **raises** on a bare
    ``*`` rather than guessing. That refusal is unit-tested.
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


def _s3fs_available() -> bool:
    """Is the s3 protocol resolvable at all?

    Asked by importing, not by reading a lockfile. fsspec maps the ``s3``
    protocol to ``s3fs`` specifically, so this import is the actual precondition
    for every ``s3://`` URL in the product.
    """
    try:
        import s3fs  # noqa: F401
    except ImportError:
        return False
    return True


#: 🚨 This marker is the ONE case in this file where a skip is expected in CI too,
#: and that is a deliberate departure from ``requires_docker`` directly above.
#:
#: ``s3fs`` was dropped in core#825 and is absent EVERYWHERE, CI included, so
#: ``requires_docker``'s "hard-fail in CI" polarity would wedge the build
#: permanently rather than tell anyone anything. The hazard that polarity exists
#: to prevent — a probe that skips itself into a meaningless green — is instead
#: covered from the other side, by
#: ``tests/test_security/test_dependency_advisories.py::TestDeferredCapability``:
#: that suite fails the day the blocker lifts, and its failure message names
#: this marker. So the pair is: these tests may skip indefinitely, and something
#: else is responsible for noticing when they should stop.
#:
#: ⚠️ Do NOT delete these tests. They are the only executable evidence that
#: ``s3://`` ever worked, they cost four months to write (core#684), and the
#: capability is still documented at datanika.io/docs/connectors/s3/. Deleting
#: them converts a recorded deferral into a silent, unrecoverable capability
#: loss.
requires_s3fs = pytest.mark.skipif(
    not _s3fs_available(),
    reason=(
        "s3fs is not installed (core#825). It is excluded by two constraints on "
        "either side of a version gap: s3fs<=2025.12.0 needs aiobotocore<3.0.0 "
        "(vs redshift-connector>=2.1.14's boto3>=1.42.22, the CVE-2026-8838 RCE "
        "fix), and s3fs>=2026.1.0 needs fsspec>=2026.1 -> gcsfs -> "
        "google-cloud-storage>=3.7.0 (vs dbt-bigquery's <3.2). The second is the "
        "live one. See BLOCKED_BY_S3FS_CONFLICT in "
        "tests/test_security/test_dependency_advisories.py."
    ),
)


def _container_status(container) -> str | None:
    """The container's real docker status, or None when it cannot be read.

    Asks testcontainers' own `get_status`, which already handles both
    `DockerContainer` and `DockerCompose`. Deriving rather than reaching into
    `_container.status` ourselves means a change on their side surfaces as an
    error here instead of silently reading `None` forever.
    """
    try:
        from testcontainers.core.wait_strategies import ContainerStatusWaitStrategy

        return ContainerStatusWaitStrategy().get_status(container)
    except Exception:  # noqa: BLE001 — a status we cannot read is not a verdict
        return None


def _is_terminal_status(status: str) -> bool:
    """Is this a status no amount of waiting recovers from?

    **Derived by asking testcontainers' own predicate**, not by restating their
    status list: `ContainerStatusWaitStrategy.running()` returns for `running`,
    returns `False` for the statuses it will keep polling on, and raises
    `StopIteration` for everything else. That `StopIteration` is exactly the
    "give up now" signal, so catching it *is* the definition. Their list can
    move; this cannot drift from it.
    """
    from testcontainers.core.wait_strategies import ContainerStatusWaitStrategy

    try:
        ContainerStatusWaitStrategy.running(status)
    except StopIteration:
        return True
    return False


def _container_log_tail(container, lines: int = 25) -> str:
    """Last few log lines, for a container that died before we could seed it."""
    try:
        stdout, stderr = container.get_logs()
        text = (stdout or b"").decode("utf-8", "replace")
        text += (stderr or b"").decode("utf-8", "replace")
        tail = [ln for ln in text.splitlines() if ln.strip()][-lines:]
        return "\n".join(f"      | {ln}" for ln in tail) or "      | (no output)"
    except Exception as exc:  # noqa: BLE001
        return f"      | (logs unavailable: {type(exc).__name__}: {exc})"


def await_setup(what: str, thunk, attempts: int = 30, delay: float = 2.0, container=None):
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

    ── Two failure modes, not one (core#758) ───────────────────────────────────
    Pass ``container=`` and this distinguishes them. Without it the behaviour is
    the old one, which conflated them.

    ``_seed`` reaches ``get_exposed_port()``, and in testcontainers 4.x that
    builds a **fresh ``ContainerStatusWaitStrategy`` on every call** with a
    **120 s** budget. It raises ``TimeoutError("container did not become
    running")`` from two unrelated situations:

      * the container is **slow** (``created``/``restarting``) — it polls for the
        full 120 s, and retrying with a bigger budget genuinely helps;
      * the container is **already dead** (``exited``/``dead``/``removing``/
        ``paused``) — ``running()`` raises ``StopIteration``, ``_poll`` returns
        ``False`` *immediately*, and retrying is pure sleeping.

    One message, opposite remedies. core#758 was filed as the first and was in
    fact the second: 12 attempts against a 120 s inner budget is a nominal
    1452 s, in a run that finished in **436.47 s** — so at least 9 of the 12
    attempts had returned instantly. The proposed fix, raising the attempt
    count, would have added ~48 s of sleeping to a run already lost.

    ── The message reports what it measured ────────────────────────────────────
    The old text printed ``attempts * delay``, which is the time spent
    **sleeping** — the thunk's own duration is not in that product, and the
    thunk is what slows down under the contention that causes these failures.
    Measured with a thunk blocking 0.3 s at ``attempts=12, delay=0.1``, it
    claimed ``1s`` against an actual **4.8 s**. That fabricated number is what
    produced core#758's wrong diagnosis. Both are printed now, labelled.
    """
    last: Exception | None = None
    started = time.monotonic()
    tried = 0
    for _ in range(attempts):
        tried += 1
        try:
            return thunk()
        except Exception as exc:  # noqa: BLE001 — any setup failure is retryable
            last = exc
            status = _container_status(container) if container is not None else None
            if status is not None and _is_terminal_status(status):
                raise AssertionError(
                    f"container setup CANNOT succeed: {what}\n"
                    f"    container status : {status!r} — terminal. This is NOT a "
                    f"readiness timeout; retrying cannot fix it, and a larger "
                    f"budget would only sleep longer.\n"
                    f"    gave up after    : {tried} attempt(s), "
                    f"{time.monotonic() - started:.1f}s measured\n"
                    f"    last error       : {type(last).__name__}: {last}\n"
                    f"    Under several concurrent suites the usual causes are an "
                    f"OOM or a ryuk reap. Container log tail:\n"
                    f"{_container_log_tail(container)}"
                ) from last
            time.sleep(delay)
    final_status = _container_status(container) if container is not None else None
    # An unreadable status must not read as "checked, and fine". If it is None
    # while a container WAS supplied, the classification above never ran and this
    # is the old conflating behaviour — say so rather than degrade silently.
    if container is None:
        status_note = "not supplied — pass `container=` to get the terminal-status check"
    elif final_status is None:
        status_note = (
            "UNREADABLE — the terminal-status check did not run; "
            "treat this verdict as the old, conflated one"
        )
    else:
        status_note = f"{final_status!r} (not terminal, so retrying was right)"
    raise AssertionError(
        f"container setup never became ready: {what}\n"
        f"    gave up after   : {tried} attempts, {time.monotonic() - started:.1f}s "
        f"measured wall clock\n"
        f"    configured      : {attempts} x {delay:g}s = {attempts * delay:.0f}s of "
        f"SLEEPING — the difference is the thunk's own duration, which is the part "
        f"that grows under contention\n"
        f"    container status: {status_note}\n"
        f"    last error      : {type(last).__name__}: {last}"
    ) from last


class _FakeContainer:
    """Enough of `DockerContainer` for `await_setup`'s classification path.

    `_container_status` goes through testcontainers' own `get_status`, which
    type-checks its argument — so these tests patch that one function rather
    than pretending to be a `DockerContainer`. Patching the narrow seam keeps
    the real `_is_terminal_status` (the part that must not drift) under test.
    """

    def __init__(self, logs: bytes = b"out-of-memory\nkilled\n"):
        self._logs = logs

    def get_logs(self):
        return self._logs, b""


class TestAwaitSetupBudget:
    """`await_setup` had no test at all, and it is what stands between daemon
    contention and a false red on someone else's push (core#758).

    No Docker: the container seam is patched, and everything else is arithmetic
    over a thunk. These run in every suite.
    """

    def test_returns_the_thunk_result_without_retrying_on_success(self):
        calls = []

        def thunk():
            calls.append(1)
            return "seeded"

        assert await_setup("x", thunk, attempts=3, delay=0) == "seeded"
        assert len(calls) == 1, "a successful thunk must not be called twice"

    def test_succeeds_on_a_later_attempt(self):
        state = {"n": 0}

        def thunk():
            state["n"] += 1
            if state["n"] < 3:
                raise ConnectionError("auth init still in flight")
            return "seeded"

        assert await_setup("x", thunk, attempts=5, delay=0) == "seeded"
        assert state["n"] == 3

    def test_exhausting_the_budget_names_what_when_and_why(self):
        def thunk():
            raise TimeoutError("container did not become running")

        with pytest.raises(AssertionError) as excinfo:
            await_setup("mongod accepting authenticated writes", thunk, attempts=4, delay=0)
        message = str(excinfo.value)
        assert "mongod accepting authenticated writes" in message
        assert "4 attempts" in message
        assert "TimeoutError" in message
        assert "container did not become running" in message

    def test_the_message_reports_measured_time_not_the_configured_product(self):
        """core#758's actual defect: the old text printed `attempts * delay`.

        With `delay=0` the configured sleeping budget is **0s**, so any non-zero
        wall-clock figure in the message can only have been measured. That is
        the discriminating assertion — a message that recomputed the product
        would print 0.0s here and pass a test that merely checked for a number.
        """

        def thunk():
            time.sleep(0.05)
            raise TimeoutError("container did not become running")

        with pytest.raises(AssertionError) as excinfo:
            await_setup("x", thunk, attempts=6, delay=0)
        message = str(excinfo.value)
        assert "0s of" in message, "the configured sleeping budget must still be shown"
        match = re.search(r"([\d.]+)s measured wall clock", message)
        assert match, f"no measured wall clock in:\n{message}"
        assert float(match.group(1)) >= 0.25, (
            f"message reports {match.group(1)}s measured, but the thunk alone "
            f"took ~0.3s — this is the product, not a measurement"
        )

    def test_a_terminal_container_aborts_immediately_with_a_different_message(self, monkeypatch):
        """The core#758 case: the container was dead, not slow.

        Two things are asserted, and the second is the one that matters —
        retrying a terminal container is pure sleeping, so it must stop after
        ONE attempt rather than burning the whole budget.
        """
        calls = []

        def thunk():
            calls.append(1)
            raise TimeoutError("container did not become running")

        monkeypatch.setattr(
            "tests.test_services.test_source_builders_move_rows._container_status",
            lambda c: "exited",
        )
        with pytest.raises(AssertionError) as excinfo:
            await_setup("mongod", thunk, attempts=30, delay=0, container=_FakeContainer())
        message = str(excinfo.value)
        assert len(calls) == 1, f"aborted after {len(calls)} attempts; must stop at 1"
        assert "CANNOT succeed" in message
        assert "'exited'" in message and "terminal" in message
        assert "retrying cannot fix it" in message
        assert "out-of-memory" in message, "the container's log tail is the diagnosis"
        # and it must NOT read like the timeout case
        assert "never became ready" not in message

    def test_a_slow_container_still_exhausts_the_budget(self, monkeypatch):
        """The negative control for the abort: `created` is not terminal.

        Without this, an over-eager classifier that called everything terminal
        would pass the test above and silently stop retrying the case the retry
        exists for — the core#578 auth-init race.
        """
        calls = []

        def thunk():
            calls.append(1)
            raise TimeoutError("container did not become running")

        monkeypatch.setattr(
            "tests.test_services.test_source_builders_move_rows._container_status",
            lambda c: "created",
        )
        with pytest.raises(AssertionError) as excinfo:
            await_setup("mongod", thunk, attempts=5, delay=0, container=_FakeContainer())
        assert len(calls) == 5, "a slow container must be retried, not abandoned"
        assert "never became ready" in str(excinfo.value)
        assert "'created'" in str(excinfo.value), "the status belongs in the message either way"

    def test_an_unreadable_status_does_not_read_as_checked_and_fine(self, monkeypatch):
        """The silent-fallback guard.

        If `get_status` ever stops working for a real container,
        `_container_status` returns None and the classification quietly never
        runs — reverting to the old conflating behaviour with no sign. The
        message has to say the check did not run, not print a reassuring blank.
        """
        monkeypatch.setattr(
            "tests.test_services.test_source_builders_move_rows._container_status",
            lambda c: None,
        )

        def thunk():
            raise TimeoutError("container did not become running")

        with pytest.raises(AssertionError) as excinfo:
            await_setup("mongod", thunk, attempts=2, delay=0, container=_FakeContainer())
        message = str(excinfo.value)
        assert "UNREADABLE" in message
        assert "did not run" in message
        assert "not terminal" not in message, "an unread status must not claim to be non-terminal"

    def test_terminal_classification_is_derived_from_testcontainers(self):
        """Asks their predicate; never restates their status list.

        `ContainerStatusWaitStrategy.running()` raises `StopIteration` for the
        statuses it refuses to continue on. If they add one, this follows.
        """
        from testcontainers.core.wait_strategies import ContainerStatusWaitStrategy

        assert _is_terminal_status("exited")
        assert _is_terminal_status("dead")
        assert not _is_terminal_status("running")
        assert not _is_terminal_status("created")
        # the derivation, spelled out: everything they will keep polling on is
        # non-terminal here, whatever that set happens to be.
        for ok in ContainerStatusWaitStrategy.CONTINUE_STATUSES:
            assert not _is_terminal_status(ok), f"{ok} is a continue-status upstream"

    def test_the_default_budget_is_bracketed_from_both_sides(self):
        """Derived from the signature, so a future edit cannot quietly undo it.

        Lower bound: core#758 failed at 12 attempts on a machine measured
        running **four concurrent full suites**. Upper bound: a budget large
        enough to turn a genuinely dead container into a hang is the same
        defect one level up — which is why the terminal-status abort above
        exists, and why this is allowed to stay finite.
        """
        signature = inspect.signature(await_setup)
        attempts = signature.parameters["attempts"].default
        delay = signature.parameters["delay"].default
        assert attempts * delay >= 60, (
            f"default budget is {attempts} x {delay}s = {attempts * delay}s; "
            f"core#758 failed at 12s under normal contention"
        )
        assert attempts * delay <= 120, (
            f"default budget is {attempts * delay}s — long enough that a dead "
            f"container reads as a hang"
        )


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
            config = await_setup(
                "postgres accepting writes",
                lambda: self._seed(postgres),
                container=postgres,
            )
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
            config = await_setup(
                "postgres accepting writes",
                lambda: self._seed(postgres),
                container=postgres,
            )
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

            await_setup("mongod accepting authenticated writes", _seed, container=mongo)

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
                container=kafka,
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


# ── An AUTHENTICATED broker, with no vendor account ─────────────────────────────
# The class above proves rows move off a broker that lets anyone in. **No managed
# Kafka works that way.** Confluent Cloud, Redpanda Serverless, Aiven and Upstash
# are all TLS + SASL on their free tiers, so every broker a user can actually
# reach requires a credential — and `CONFIG_SCHEMAS["kafka"]` had no credential
# field, `_build_kafka_source` passed no security kwargs, and the security keys
# were absent from `INTERNAL_CONFIG_KEYS`. Three layers, one outcome: the
# connector could not connect to any broker a customer owns.
#
# 🚨 **The nightly smoke could not see it**, and that is the part worth carrying.
# `tests/test_connector_smoke/test_paid_connectors.py` builds its own
# `KafkaAdminClient(security_protocol="SASL_SSL", sasl_plain_username=...)` **inside
# the test**. So it proves the vendor sandbox is reachable and proves nothing about
# our builder — reviving a dead Redpanda account would have turned that green while
# the product still could not connect. A test that supplies the missing thing
# itself is core#992's shape exactly.
#
# A broker in a container closes this with no card, no vendor account and no
# managed tier — and it is the only arrangement in which this connector has a
# test that is able to fail.

_SASL_USER = "datanika"
_SASL_PASSWORD = "probe-secret-pw"

#: No underscore in the listener name, deliberately. Confluent's env→property
#: translation lowercases and maps ``_`` to ``.``, so
#: ``KAFKA_LISTENER_NAME_SASL_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG`` arrives as
#: ``listener.name.sasl.plaintext.plain.sasl.jaas.config`` and binds to **no**
#: listener. Measured: the broker then exits 1 before logging "Kafka Server
#: started", and testcontainers reports only a wait-strategy timeout.
_SASL_LISTENER = "SASL"

_JAAS = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{_SASL_USER}" password="{_SASL_PASSWORD}" '
    f'user_{_SASL_USER}="{_SASL_PASSWORD}";'
)


def _anonymous_write_refused(bootstrap: str) -> bool | None:
    """Does a credential-less client actually get turned away at the wire?

    Returns ``True`` refused, ``False`` accepted, and ``None`` when the probe
    never reached the broker — which the caller must **not** read as a refusal.

    That third case is not defensive padding. The first version of this control
    passed ``api_version_auto_timeout_ms``, which kafka-python 3.x rejects, so it
    raised ``ValueError: Unrecognized configs`` in the **constructor** and reported
    the broker as authenticating while nothing had left the process. It is the same
    kwarg core#331 already caught once in the nightly smoke.
    """
    from kafka import KafkaProducer

    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap, bootstrap_timeout_ms=15000)
    except (TypeError, ValueError):
        return None  # bad kwarg / bad config: never reached the broker
    except Exception:  # noqa: BLE001 - any transport-level rejection counts
        return True
    try:
        producer.send("widgets-sasl", b"anonymous").get(timeout=20)
    except Exception:  # noqa: BLE001
        return True
    finally:
        # A close that fails tells us nothing about the broker's auth policy,
        # which is the only question this helper answers.
        with contextlib.suppress(Exception):
            producer.close(timeout=5)
    return False


@pytest.fixture(scope="class")
def sasl_broker():
    """A real Kafka broker that refuses anonymous clients. ~40 s to start."""
    from testcontainers.kafka import KafkaContainer

    container = (
        KafkaContainer(listener_name=_SASL_LISTENER, security_protocol="SASL_PLAINTEXT")
        .with_kraft()
        .with_env("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
        .with_env(f"KAFKA_LISTENER_NAME_{_SASL_LISTENER}_PLAIN_SASL_JAAS_CONFIG", _JAAS)
    )
    with container as broker:
        yield broker


@requires_docker
class TestKafkaSaslSourceMovesRows:
    """``_build_kafka_source`` against a broker that demands a credential.

    Two tests, and the order matters. The first is the **arming control**: it
    proves the broker turns an anonymous client away. Without it a green on the
    second test is satisfiable by a broker that never asked for anything, which is
    exactly what the credential-free class above runs against.
    """

    def test_the_broker_refuses_an_unauthenticated_client(self, sasl_broker):
        """Arming control. If this fails, the test below proves nothing."""
        refused = _anonymous_write_refused(sasl_broker.get_bootstrap_server())

        assert refused is not None, (
            "the control never reached the broker — it failed in the client "
            "constructor, which is indistinguishable from a refusal and would make "
            "the SASL probe below vacuous"
        )
        assert refused is True, (
            "an anonymous producer was ACCEPTED, so this broker is not enforcing "
            "SASL and a green below would say nothing about credentials"
        )

    def test_rows_land_from_an_authenticated_broker(self, sasl_broker, tmp_path):
        from kafka import KafkaProducer

        bootstrap = sasl_broker.get_bootstrap_server()
        sasl = {
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": _SASL_USER,
            "sasl_plain_password": _SASL_PASSWORD,
        }

        # SETUP retry only (core#578) — never around the load or the assertion.
        producer = await_setup(
            "kafka broker accepting authenticated producers",
            lambda: KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode(),
                **sasl,
            ),
            container=sasl_broker,
        )
        for widget in WIDGETS:
            producer.send("widgets-sasl", widget)
        producer.flush()

        db_path = _extract_load(
            tmp_path,
            "kafka",
            {"bootstrap_servers": bootstrap, "group_id": "probe-sasl", **sasl},
            {"topics": ["widgets-sasl"]},
        )

        rows = _rows(db_path, "widgets_sasl")

        assert rows == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "the Kafka source did not deliver rows from an authenticated broker. "
            "Every managed Kafka free tier is SASL-only, so this is the only "
            "configuration a customer can actually reach."
        )


@requires_docker
@requires_s3fs
class TestS3FileSourceMovesRows:
    """``s3`` over a real object store — the last unproven row of #545 (core#684).

    **The blocker this sat behind was not real.** It was recorded as needing "a
    bucket + key pair, which is a credential this suite deliberately does not
    take". But ``endpoint_url`` is in ``AWS_CREDENTIAL_KEYS``, the connector form
    exposes it as a first-class field, and ``datanika.io/docs/connectors/s3/``
    ships it to users as *"only needed for S3-compatible stores (MinIO,
    Backblaze B2, etc.)"* — checked against the live page, not the repo. So the
    S3 *protocol* is exercisable with no AWS account, no vendor sign-up and no
    secret on disk, exactly as Mongo and Kafka are exercised above.

    Which makes the gap worse than "a format we did not get to": we **published**
    a capability and never once executed it. The deferral held for four months,
    over the transport belonging to the format ``#492`` actually broke.

    🔴 **SKIPPED SINCE core#825 — the capability is not currently shipped.**
    ``s3fs`` was dropped as a consequence of taking the ``redshift-connector``
    CVE-2026-8838 fix and moving the dbt stack off its 1.7 pin; without it,
    fsspec cannot resolve the ``s3`` protocol at all. ``gs://`` (gcsfs) and
    ``az://`` (adlfs) are unaffected.

    ⚠️ **These tests are not obsolete and must not be deleted.** They are the
    only executable evidence this transport ever worked, and
    ``datanika.io/docs/connectors/s3/`` still documents it. The failure that
    matters now is not theirs — it is that a documented connector has no working
    implementation. See ``requires_s3fs`` above for the exact constraint pair and
    the re-check trigger.

    **Why "csv already passes" does not cover this.** Every other file test in
    this module reads from a local directory, so ``filesystem()`` resolves to
    ``LocalFileSystem`` and no S3 code path runs at all. The *reader* is shared;
    the *transport* is not, and the transport is the half that has never been
    observed. Dropping ``endpoint_url`` from ``AWS_CREDENTIAL_KEYS`` turns the
    three tests that have to reach the server red and leaves the fourth — which
    raises before any socket is opened — green. That asymmetry is what
    distinguishes these from a local-filesystem test wearing an ``s3://`` label;
    a mutation that reddened all four would only show the harness is fragile.
    """

    BUCKET = "probe-bucket"
    ACCESS_KEY = "probeaccesskey"
    SECRET_KEY = "probesecretkey"

    @staticmethod
    def _csv_bytes() -> bytes:
        header = "id,name,price\n"
        body = "".join(f"{w['id']},{w['name']},{w['price']}\n" for w in WIDGETS)
        return (header + body).encode("utf-8")

    @staticmethod
    def _parquet_bytes() -> bytes:
        import io

        import pyarrow
        import pyarrow.parquet

        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pyarrow.table(
                {
                    "id": [w["id"] for w in WIDGETS],
                    "name": [w["name"] for w in WIDGETS],
                    "price": [w["price"] for w in WIDGETS],
                }
            ),
            buf,
        )
        return buf.getvalue()

    @pytest.fixture(scope="class")
    def bucket(self):
        """One MinIO for the class, seeded with a CSV and a Parquet object.

        Defined here rather than imported: pytest registers an imported fixture
        as a separate FixtureDef in the importing module, so sharing a
        ``scope="class"`` fixture by import starts a second container. See
        ``tests/test_fixture_sharing.py``.
        """
        from testcontainers.core.container import DockerContainer

        container = (
            DockerContainer("minio/minio:RELEASE.2025-09-07T16-13-09Z")
            .with_command("server /data")
            .with_env("MINIO_ROOT_USER", self.ACCESS_KEY)
            .with_env("MINIO_ROOT_PASSWORD", self.SECRET_KEY)
            .with_exposed_ports(9000)
        )
        with container as running:
            endpoint = f"http://{running.get_container_host_ip()}:{running.get_exposed_port(9000)}"

            # SETUP retry only (core#578): MinIO answers its port before the
            # object layer is initialised, so the first `create_bucket` races
            # it. The loads and the assertions below are never retried.
            def _seed():
                import boto3

                client = boto3.client(
                    "s3",
                    endpoint_url=endpoint,
                    aws_access_key_id=self.ACCESS_KEY,
                    aws_secret_access_key=self.SECRET_KEY,
                    region_name="us-east-1",
                )
                client.create_bucket(Bucket=self.BUCKET)
                client.put_object(Bucket=self.BUCKET, Key="csv/widgets.csv", Body=self._csv_bytes())
                client.put_object(
                    Bucket=self.BUCKET, Key="pq/widgets.parquet", Body=self._parquet_bytes()
                )
                # Read one back through the same client. Without this the
                # fixture is satisfied by a PUT that 200s against a
                # half-started server, and the empty listing that follows
                # would read as a product bug.
                got = client.get_object(Bucket=self.BUCKET, Key="csv/widgets.csv")
                assert got["Body"].read() == self._csv_bytes()

            # `DockerContainer` is the GENERIC wrapper — unlike
            # `MongoDbContainer` it carries no readiness wait of its own, so
            # this retry is the only thing standing between the fixture and a
            # half-started server. The explicit `attempts=30` is now the
            # default (core#758 raised it from 12), and is kept here so the
            # intent survives a future change to that default.
            # This is a SETUP retry (core#578) and hides nothing: the loads and
            # assertions below are never retried.
            await_setup("minio accepting bucket writes", _seed, attempts=30, container=running)
            yield endpoint

    def _config(self, endpoint: str, prefix: str) -> dict:
        return {
            "bucket_url": f"s3://{self.BUCKET}/{prefix}",
            "aws_access_key_id": self.ACCESS_KEY,
            "aws_secret_access_key": self.SECRET_KEY,
            "endpoint_url": endpoint,
            "region_name": "us-east-1",
        }

    def test_rows_land_in_the_destination(self, tmp_path, bucket):
        """csv, end to end: s3:// -> fsspec -> reader -> DuckDB."""
        db_path = _extract_load(tmp_path, "s3", self._config(bucket, "csv"), {"file_glob": "*.csv"})

        # `*.csv` is a wildcard, so `_file_table_name` falls past the glob-stem
        # branch to the connection type. Asserting the real default rather than
        # setting `table_name` keeps this honest about what a user actually gets.
        assert _rows(db_path, "s3") == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "s3 did not deliver record CONTENTS into DuckDB over a real object "
            "store. #492's shape is one row per file carrying that file's own "
            "metadata, and its row COUNT looks healthy — which is why the count "
            "is not the assertion here."
        )

    def test_parquet_rows_land_over_the_same_transport(self, tmp_path, bucket):
        """The reader and the transport are independent axes.

        `test_parquet_rows_land_in_the_destination` above proves `read_parquet`
        against a local directory; the test above this one proves the S3
        transport with `read_csv`. Neither proves the pair, and pyarrow opens a
        remote file through a different fsspec path than a local one — which is
        exactly the "shares the assembly" reasoning #545 was written to distrust.
        """
        db_path = _extract_load(
            tmp_path, "s3", self._config(bucket, "pq"), {"file_glob": "*.parquet"}
        )

        assert _rows(db_path, "s3") == [("alpha", 100), ("beta", 200), ("gamma", 300)], (
            "parquet did not deliver record CONTENTS into DuckDB over s3."
        )

    def test_a_bare_glob_refuses_rather_than_guessing(self, tmp_path, bucket):
        """The `s3` default glob is `*`, and that MUST raise.

        `s3` carries no format in its connection type, so `_resolve_file_format`
        has only the glob's extension to go on and a bare `*` reaches nothing.
        Guessing there reproduces #492 one layer along: a plausible row count
        read through the wrong reader. Unit-tested already, never over the real
        transport — and the refusal has to arrive before any bytes move.
        """
        with pytest.raises(DltRunnerError, match="no way to read it"):
            _extract_load(tmp_path, "s3", self._config(bucket, "csv"), {})

    def test_a_missing_prefix_fails_instead_of_loading_nothing(self, tmp_path, bucket):
        """#493 over S3: matching nothing is not a successful load of nothing.

        A typo'd prefix is the most likely S3 misconfiguration there is — the
        bucket is real, the credentials work, the listing is simply empty — and
        it is the case that used to report `success` / `Rows: 0`. Every other
        test of this guard exercises the LOCAL branch of
        `describe_empty_file_match`, which is the one that can inspect the path;
        the remote branch has never been run.
        """
        with pytest.raises(DltRunnerError, match="No files matched"):
            _extract_load(
                tmp_path, "s3", self._config(bucket, "no-such-prefix"), {"file_glob": "*.csv"}
            )

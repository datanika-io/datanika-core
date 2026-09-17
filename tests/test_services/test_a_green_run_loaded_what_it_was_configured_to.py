"""A run that reports success loaded everything its configuration selected (QA class probe).

## The class

A run reports **success** while the destination holds **less than its configuration asked for**.
For a data pipeline this is the worst failure shape, because the success is the part that lies:
nothing is red, the row count looks plausible, and nobody has an error message to search for.

Three instances in one week, each found by a person walking a guide rather than by a test:

* **core#1337** (landing#599): Shopify's ``status`` defaults to ``open``, and the loader did not
  send it, so a green run loaded only the orders open at run time.
* **core#1401**: a SQLite run whose file the worker cannot see opens an empty database and succeeds
  with 0 tables.
* **core#1408**: a Kafka upload with two topics, both holding messages, succeeds with one loaded.

The same shape earlier: **core#823** (Stripe: 15 customers, 10 landed, because no paginator was
set) and **core#493** (a glob matching nothing: ``success`` with 0 rows).

Each instance got its own fix. The class had no probe, so the next one would again be found by a
reader.

## The invariant

Each **arm** is a configuration a user can save, plus the source's ground truth for every unit that
configuration selects (tables, files, resources, endpoints, topics)::

    execute() returned  =>  every selected unit that holds records at the source has a destination
                            table holding every one of those record ids

* **A refusal satisfies the class invariant.** Failing loudly is honest, and it is exactly what
  core#1401's first acceptance criterion asks for. An arm whose source really holds data is also
  marked ``loadable``, and a refusal there is its own red (``RefusedALoadableConfiguration``), so a
  harness that breaks the source cannot pass as a refusal.
* **Loading more than configured** is a different class (core#1336) and is not judged here.

The three readings are independent of each other and of the run's own report (QA_RULES §16):

* **what was asked**: the arm's configuration, built through the upload form's own
  ``UploadState._build_config`` wherever the form offers the selector;
* **what exists**: record ids written by the fixture that seeded the source, before the run;
* **what landed**: the destination read back by query, never ``rows_loaded``.

## The population is derived, twice, and independently of the arms

This file does not choose which source types need probing. It reads the product twice (QA_RULES
§2a): ``SOURCE_TYPES | WITHDRAWN_SOURCE_TYPES`` (what a user can pick, or has stored), and every
type ``DltRunnerService.build_source`` dispatches, found by walking that method's syntax tree.
The two must agree. Every type in that population then has an arm here, or a stated reason in
``NOT_YET_PROBED``. The ratchet is strict: a new connector arrives red until somebody decides which.

## Seen red (2026-09-17, core ``dev`` ``3a2d414``, one mutation at a time, each reverted)

Existing suites are the modules nearest the mutated code, run on the same tree. A mocked test that
goes red only because a mutation changed a call's shape is not counted as catching the behaviour.

* **core#1337 reverted** (no ``status=any``): shopify ``orders`` 2 of 5.
* **core#823 reverted** (no vendor paginator): stripe ``customers`` 2 of 5, ``charges`` 2 of 3.
* **core#1401, on dev as is**: strict xfail, ``customers`` and ``orders`` have no table. A mutation
  refusing a SQLite path with no file behind it turns it into ``XPASS(strict)``.
* **Planted, a SQL source loads only its first chunk**: sqlite 2 of 5 and 2 of 7. Existing: 184
  tests and the Postgres and MySQL row probes stay green.
* **Planted, an OpenAPI selection keeps only its first resource**: ``gadgets`` has no table.
  Existing: 295 green.
* **Planted, a REST client with no paginator stops at page one**: rest_api and openapi short.
  Existing: 295 green.
* **Planted, a glob loads only its first file**: csv, json and parquet 4 of 10. Existing: one red,
  ``test_peeking_does_not_consume_the_first_file``, which loads two CSVs.

The core#1408 arm needs a Kafka broker, and its red is read from CI (see the PR).
"""

from __future__ import annotations

import ast
import copy
import http.server
import inspect
import json
import sqlite3
import textwrap
import threading
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest

from datanika.services import dlt_runner
from datanika.services.connection_service import SOURCE_TYPES, WITHDRAWN_SOURCE_TYPES
from datanika.services.dlt_runner import DltRunnerService
from tests.test_services.test_rerun_lands_each_record_once import form_config
from tests.test_services.test_source_builders_move_rows import await_setup, requires_docker

DATASET = "probe"


class HarnessError(RuntimeError):
    """A setup invariant failed. Never absorbed by an ``xfail(raises=AssertionError)``."""


class RefusedALoadableConfiguration(RuntimeError):  # noqa: N818 - it names the outcome
    """The run raised on an arm whose source holds data: not the class, and never a pass."""


# ─────────────────────────────────────────────────────────────────────────────────────────────
# The verdict
# ─────────────────────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Verdict:
    refused: str | None
    #: selected units that hold records at the source and have no destination table at all
    missing: tuple[str, ...] = ()
    #: (unit, records landed, records at the source) for a table holding only some of them
    short: tuple[tuple[str, int, int], ...] = ()

    @property
    def success_disagrees(self) -> bool:
        return self.refused is None and bool(self.missing or self.short)

    def explain(self) -> str:
        return (
            f"the run reported SUCCESS, and the destination lacks what the configuration selected: "
            f"units with no table={list(self.missing)}; units short "
            f"(unit, landed, at source)={list(self.short)}"
        )


def judge(
    truth: dict[str, frozenset], landed: dict[str, frozenset] | None, error: BaseException | None
) -> Verdict:
    """Compare what the source holds with what landed. Pure: no I/O, so it is tested directly."""
    if error is not None:
        return Verdict(refused=f"{type(error).__name__}: {str(error)[:400]}")
    landed = landed or {}
    missing = tuple(sorted(u for u, ids in truth.items() if ids and u not in landed))
    short = tuple(
        sorted(
            (u, len(ids & landed[u]), len(ids))
            for u, ids in truth.items()
            if u in landed and not ids <= landed[u]
        )
    )
    return Verdict(refused=None, missing=missing, short=short)


def landed_ids(dest: Path) -> dict[str, frozenset]:
    """Every user-facing table in the destination dataset, with the record ids it holds."""
    if not dest.exists():
        return {}
    con = duckdb.connect(str(dest))
    try:
        tables = [
            t
            for (t,) in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                [DATASET],
            ).fetchall()
            if not t.startswith("_dlt_")
        ]
        out: dict[str, frozenset] = {}
        for table in tables:
            columns = {
                c
                for (c,) in con.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = ? AND table_name = ?",
                    [DATASET, table],
                ).fetchall()
            }
            if "id" not in columns:
                raise HarnessError(f"destination table {table!r} has no id column: {columns}")
            rows = con.execute(f'SELECT DISTINCT id FROM "{DATASET}"."{table}"').fetchall()
            out[table] = frozenset(r[0] for r in rows)
        return out
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────────────────────────────────────
# A loopback HTTP server, with the pagination contracts the arms need
# ─────────────────────────────────────────────────────────────────────────────────────────────

PAGE = 2


def _json(payload, status: int = 200, headers: dict | None = None):
    return status, payload, headers or {}


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        parsed = urllib.parse.urlsplit(self.path)
        query = {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}
        self.server.requests.append((parsed.path, query))
        route = next(
            (fn for suffix, fn in self.server.routes.items() if parsed.path.endswith(suffix)), None
        )
        if route is None:
            self.send_error(404)
            return
        status, payload, headers = route(query, f"http://{self.headers['Host']}{parsed.path}")
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in headers.items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def stub(monkeypatch):
    """A real loopback server; the egress guard is opened for it the way #441 documents."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.routes = {}
    server.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr("datanika.services.dlt_runner.validate_egress_host", lambda url: None)
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
    )
    server.root = f"http://127.0.0.1:{server.server_port}"
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


def link_header_pages(records: list[dict]) -> Callable:
    """RFC 5988 ``Link: <…>; rel="next"`` paging over ``records``, as a bare JSON array."""

    def route(query, url):
        start = int(query.get("page", "0"))
        page = records[start : start + PAGE]
        headers = {}
        if start + PAGE < len(records):
            headers["Link"] = f'<{url}?page={start + PAGE}>; rel="next"'
        return _json(page, headers=headers)

    return route


def shopify_orders(orders: list[dict], key: str) -> Callable:
    """Shopify's contract: ``status`` defaults to ``open``, and ``page_info`` travels alone."""

    def _status(order):
        if order["cancelled_at"]:
            return "cancelled"
        return "closed" if order["closed_at"] else "open"

    def route(query, url):
        if "page_info" in query:
            if set(query) - {"page_info", "limit", "fields"}:
                return _json({"errors": {"page_info": "Invalid value."}}, status=400)
            wanted, start = query["page_info"].split(":")
            start = int(start)
        else:
            wanted, start = query.get("status", "open"), 0
        matching = [o for o in orders if wanted == "any" or _status(o) == wanted]
        headers = {}
        if start + PAGE < len(matching):
            headers["Link"] = f'<{url}?limit={PAGE}&page_info={wanted}:{start + PAGE}>; rel="next"'
        return _json({key: matching[start : start + PAGE]}, headers=headers)

    return route


def shopify_list(records: list[dict], key: str) -> Callable:
    return lambda query, url: _json({key: records})


def stripe_list(records: list[dict]) -> Callable:
    """Stripe's list contract: ``data`` + ``has_more``, walked with ``starting_after=<last id>``."""

    def route(query, url):
        ids = [r["id"] for r in records]
        after = query.get("starting_after")
        start = ids.index(after) + 1 if after in ids else 0
        page = records[start : start + PAGE]
        return _json({"object": "list", "data": page, "has_more": start + PAGE < len(records)})

    return route


def _rewrite_saas_host(monkeypatch, stub, seen: list[str]) -> None:
    """Send the SaaS fallback to the stub, keeping every resource, header and paginator it built."""
    original = DltRunnerService._rest_api_fallback.__func__

    def fallback(cls, base_url, auth, resources, *, paginator, headers=None):
        seen.append(base_url)
        rewritten = stub.root + urllib.parse.urlsplit(base_url).path
        return original(cls, rewritten, auth, resources, paginator=paginator, headers=headers)

    monkeypatch.setattr(DltRunnerService, "_rest_api_fallback", classmethod(fallback))


# ─────────────────────────────────────────────────────────────────────────────────────────────
# Arms
# ─────────────────────────────────────────────────────────────────────────────────────────────


@dataclass
class Env:
    tmp_path: Path
    monkeypatch: pytest.MonkeyPatch
    stub: object | None = None
    kafka: object | None = None


@dataclass(frozen=True)
class Case:
    source_type: str
    source_config: dict
    dlt_config: dict
    #: destination table -> the record ids the SOURCE holds for that unit, written before the run
    truth: dict[str, frozenset]
    #: a check to run after execute(), for arms that must confirm their own plumbing
    after: Callable[[], None] | None = None
    #: how many source batches (files, pages, chunks) one unit's records were written across. An
    #: arm with a single unit can only show "less" when its records are spread over several.
    spread: int = 1


@dataclass(frozen=True)
class Arm:
    id: str
    source_type: str
    #: the configuration key through which the user selects the units
    selects: str
    #: True when the source really holds the data, so a refusal is not an acceptable outcome
    loadable: bool
    build: Callable[[Env], Case]
    needs: tuple[str, ...] = ()


def _sqlite_file(path: Path, tables: dict[str, int]) -> dict[str, frozenset]:
    con = sqlite3.connect(path)
    try:
        for table, count in tables.items():
            con.execute(f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, label TEXT)')
            con.executemany(
                f'INSERT INTO "{table}" VALUES (?, ?)',
                [(i, f"{table}-{i}") for i in range(1, count + 1)],
            )
        con.commit()
        return {
            table: frozenset(r[0] for r in con.execute(f'SELECT id FROM "{table}"').fetchall())
            for table in tables
        }
    finally:
        con.close()


def _arm_sqlite_selected_tables_in_chunks(env: Env) -> Case:
    path = env.tmp_path / "shop.sqlite"
    truth = _sqlite_file(path, {"customers": 5, "orders": 7, "audit": 3})
    dlt_config = form_config("sqlite", form_table_names="customers, orders", form_batch_size="2")
    if (
        dlt_config.get("table_names") != ["customers", "orders"]
        or dlt_config.get("batch_size") != 2
    ):
        raise HarnessError(f"the form did not store the selection: {dlt_config}")
    # `audit` exists and is NOT selected, so it is not part of what was asked for.
    selected = {t: truth[t] for t in ("customers", "orders")}
    return Case("sqlite", {"path": str(path)}, dlt_config, selected)


def _arm_sqlite_path_the_worker_cannot_see(env: Env) -> Case:
    """core#1401 arm B: the web app saw the file when it was tested; the worker does not."""
    web_app_view = env.tmp_path / "web" / "online_store.sqlite"
    web_app_view.parent.mkdir()
    truth = _sqlite_file(web_app_view, {"customers": 5, "orders": 7})
    worker_view = env.tmp_path / "worker" / "online_store.sqlite"
    worker_view.parent.mkdir()  # the directory exists; the file is not in it
    return Case("sqlite", {"path": str(worker_view)}, form_config("sqlite"), truth)


def _write_files(directory: Path, fmt: str, parts: dict[str, range]) -> frozenset:
    import pyarrow
    import pyarrow.parquet

    directory.mkdir()
    ids: set[int] = set()
    for name, id_range in parts.items():
        rows = [{"id": i, "label": f"{name}-{i}"} for i in id_range]
        ids.update(id_range)
        target = directory / f"{name}.{fmt}"
        if fmt == "csv":
            body = "id,label\n" + "".join(f"{r['id']},{r['label']}\n" for r in rows)
            target.write_text(body, encoding="utf-8")
        elif fmt == "json":
            target.write_text(json.dumps(rows), encoding="utf-8")
        else:
            table = pyarrow.table(
                {"id": [r["id"] for r in rows], "label": [r["label"] for r in rows]}
            )
            pyarrow.parquet.write_table(table, target)
    (directory / "README.txt").write_text("not matched by the glob", encoding="utf-8")
    return frozenset(ids)


def _arm_files(fmt: str) -> Callable[[Env], Case]:
    def build(env: Env) -> Case:
        drop = env.tmp_path / "drop"
        ids = _write_files(drop, fmt, {"part-1": range(1, 5), "part-2": range(5, 11)})
        dlt_config = form_config(fmt, form_file_glob=f"part-*.{fmt}")
        # A wildcard glob names the table after the connection type (`_file_table_name`).
        return Case(fmt, {"bucket_url": str(drop)}, dlt_config, {fmt: ids}, spread=2)

    return build


def _records(prefix: str, count: int, *, start: int = 1) -> list[dict]:
    return [{"id": i, "label": f"{prefix}-{i}"} for i in range(start, start + count)]


def _arm_rest_api_two_resources(env: Env) -> Case:
    widgets, gadgets = _records("widget", 5), _records("gadget", 3)
    env.stub.routes.update(
        {"/widgets": link_header_pages(widgets), "/gadgets": link_header_pages(gadgets)}
    )
    resources = [
        {"name": "widgets", "endpoint": {"path": "widgets"}},
        {"name": "gadgets", "endpoint": {"path": "gadgets"}},
    ]
    truth = {
        "widgets": frozenset(r["id"] for r in widgets),
        "gadgets": frozenset(r["id"] for r in gadgets),
    }
    return Case("rest_api", {"base_url": env.stub.root}, {"resources": resources}, truth)


def _arm_openapi_two_selected_resources(env: Env) -> Case:
    widgets, gadgets, gizmos = _records("widget", 5), _records("gadget", 3), _records("gizmo", 4)
    env.stub.routes.update(
        {
            "/widgets": link_header_pages(widgets),
            "/gadgets": link_header_pages(gadgets),
            "/gizmos": link_header_pages(gizmos),
        }
    )
    catalog = [
        {"name": name, "endpoint": {"path": name}, "columns": {}, "_source": "probe-spec"}
        for name in ("widgets", "gadgets", "gizmos")
    ]
    truth = {
        "widgets": frozenset(r["id"] for r in widgets),
        "gadgets": frozenset(r["id"] for r in gadgets),
    }
    return Case(
        "openapi",
        {"base_url": env.stub.root, "resources": catalog},
        {"resource_names": ["widgets", "gadgets"]},
        truth,
    )


def _arm_shopify_default_endpoints(env: Env) -> Case:
    """core#1337's configuration: the form's default Shopify upload, orders in every status."""

    def order(i, closed=False, cancelled=False):
        return {
            "id": i,
            "closed_at": "2026-08-01T10:00:00Z" if closed else None,
            "cancelled_at": "2026-08-02T10:00:00Z" if cancelled else None,
        }

    orders = [
        order(1001),
        order(1002, closed=True),
        order(1003),
        order(1004, cancelled=True),
        order(1005, closed=True),
    ]
    products, customers = _records("product", 3), _records("customer", 2)
    env.stub.routes.update(
        {
            "/orders.json": shopify_orders(orders, "orders"),
            "/products.json": shopify_list(products, "products"),
            "/customers.json": shopify_list(customers, "customers"),
        }
    )
    seen: list[str] = []
    _rewrite_saas_host(env.monkeypatch, env.stub, seen)

    def after():
        if seen != ["https://probe-store.myshopify.com/"]:
            raise HarnessError(f"the Shopify branch no longer asked for the store's host: {seen}")

    truth = {
        "orders": frozenset(o["id"] for o in orders),
        "products": frozenset(r["id"] for r in products),
        "customers": frozenset(r["id"] for r in customers),
    }
    source = {"store": "probe-store", "api_key": "shpat_probe"}
    return Case("shopify", source, form_config("shopify"), truth, after)


def _arm_stripe_two_selected_endpoints(env: Env) -> Case:
    """core#823's connector: a selection of two endpoints, each longer than one page."""
    customers = [{"id": f"cus_{i}", "email": f"c{i}@example.com"} for i in range(1, 6)]
    charges = [{"id": f"ch_{i}", "amount": 100 * i} for i in range(1, 4)]
    env.stub.routes.update(
        {"/v1/customers": stripe_list(customers), "/v1/charges": stripe_list(charges)}
    )
    seen: list[str] = []
    _rewrite_saas_host(env.monkeypatch, env.stub, seen)

    def after():
        if seen != ["https://api.stripe.com/v1/"]:
            raise HarnessError(f"the Stripe branch no longer asked for api.stripe.com: {seen}")

    dlt_config = form_config("stripe", form_selected_endpoints=["customers", "charges"])
    if dlt_config.get("endpoints") != ["customers", "charges"]:
        raise HarnessError(f"the form did not store the endpoint selection: {dlt_config}")
    truth = {
        "customers": frozenset(r["id"] for r in customers),
        "charges": frozenset(r["id"] for r in charges),
    }
    return Case("stripe", {"api_key": "sk_test_probe"}, dlt_config, truth, after)


def _arm_kafka_two_topics_both_waiting(env: Env) -> Case:
    """core#1408 run 13: two topics, both holding unread messages, the guide's own example."""
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic

    bootstrap = env.kafka.get_bootstrap_server()
    admin = await_setup(
        "kafka broker accepting admin clients",
        lambda: KafkaAdminClient(bootstrap_servers=bootstrap),
        container=env.kafka,
    )
    try:
        admin.create_topics(
            [
                NewTopic("events", num_partitions=1, replication_factor=1),
                NewTopic("orders", num_partitions=3, replication_factor=1),
            ]
        )
    finally:
        admin.close()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode()
    )
    events, orders = _records("event", 5), _records("order", 7)
    for topic, records in (("events", events), ("orders", orders)):
        for record in records:
            producer.send(topic, record)
    producer.flush()
    producer.close()
    truth = {
        "events": frozenset(r["id"] for r in events),
        "orders": frozenset(r["id"] for r in orders),
    }
    source = {"bootstrap_servers": bootstrap, "topics": "events, orders"}
    return Case("kafka", source, form_config("kafka"), truth)


ARMS: tuple[Arm, ...] = (
    Arm(
        "sqlite-selected-tables-in-chunks",
        "sqlite",
        "table_names",
        True,
        _arm_sqlite_selected_tables_in_chunks,
    ),
    Arm(
        "sqlite-path-the-worker-cannot-see",
        "sqlite",
        "path",
        False,
        _arm_sqlite_path_the_worker_cannot_see,
    ),
    Arm("csv-glob-matching-two-files", "csv", "file_glob", True, _arm_files("csv")),
    Arm("json-glob-matching-two-files", "json", "file_glob", True, _arm_files("json")),
    Arm("parquet-glob-matching-two-files", "parquet", "file_glob", True, _arm_files("parquet")),
    Arm(
        "rest-api-two-resources-one-paged",
        "rest_api",
        "resources",
        True,
        _arm_rest_api_two_resources,
        needs=("stub",),
    ),
    Arm(
        "openapi-two-of-three-resources",
        "openapi",
        "resource_names",
        True,
        _arm_openapi_two_selected_resources,
        needs=("stub",),
    ),
    Arm(
        "shopify-default-endpoints-every-order-status",
        "shopify",
        "endpoints",
        True,
        _arm_shopify_default_endpoints,
        needs=("stub",),
    ),
    Arm(
        "stripe-two-endpoints-each-paged",
        "stripe",
        "endpoints",
        True,
        _arm_stripe_two_selected_endpoints,
        needs=("stub",),
    ),
    Arm(
        "kafka-two-topics-both-waiting",
        "kafka",
        "topics",
        True,
        _arm_kafka_two_topics_both_waiting,
        needs=("kafka",),
    ),
)

#: The known instances of the class that are unfixed on the branch this file ships in. Strict: an
#: entry cannot outlive its defect, because the fix turns the arm into an XPASS, which fails.
KNOWN_INSTANCES: dict[str, str] = {
    "sqlite-path-the-worker-cannot-see": (
        "core#1401: with no file at the path, sql_database opens an empty SQLite database and the "
        "run succeeds with 0 tables"
    ),
    "kafka-two-topics-both-waiting": (
        "core#1408: one consumer per topic in one group and one process; the second is never "
        "assigned a partition and its topic loads nothing on a green run"
    ),
}

#: Every source type without an arm, with the reason. Not a list of exemptions: a list of what this
#: probe does not yet see. An arm makes its entry fail until it is removed.
NOT_YET_PROBED: dict[str, str] = {
    **{
        t: "needs its own database server; the sql_database path is probed through sqlite, the "
        "engine's own credential and dialect handling is not"
        for t in ("postgres", "mysql", "mssql", "oracle", "clickhouse")
    },
    "duckdb": "an in-process arm is possible and not yet written",
    "mongodb": "needs a MongoDB container, and its own mongodb_source reader",
    "google_sheets": "needs a Google account",
    "google_analytics": "own builder (_build_ga4_source) paging at GA4_PAGE_SIZE; no stub yet",
    "google_ads": "own builder (_build_google_ads_source); no stub yet",
    **{
        t: "REST fallback with its own SAAS_PAGINATORS contract; no stub of that contract yet"
        for t in (
            "github",
            "hubspot",
            "salesforce",
            "jira",
            "slack",
            "zendesk",
            "airtable",
            "notion",
            "pipedrive",
            "freshdesk",
            "asana",
            "facebook_ads",
        )
    },
    "s3": "withdrawn (core#863): s3fs is not installed, so no s3 run can load at all",
}


# ─────────────────────────────────────────────────────────────────────────────────────────────
# The probe
# ─────────────────────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def kafka_broker():
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer() as broker:
        yield broker


def _param(arm: Arm):
    marks = []
    if "kafka" in arm.needs:
        marks.append(requires_docker)
    if arm.id in KNOWN_INSTANCES:
        marks.append(
            pytest.mark.xfail(strict=True, raises=AssertionError, reason=KNOWN_INSTANCES[arm.id])
        )
    return pytest.param(arm, id=arm.id, marks=marks)


def run_and_judge(case: Case, tmp_path: Path) -> Verdict:
    dest = tmp_path / "dest.duckdb"
    try:
        DltRunnerService(pipelines_dir=str(tmp_path / "dlt")).execute(
            pipeline_id=1,
            source_type=case.source_type,
            source_config=copy.deepcopy(case.source_config),
            destination_type="duckdb",
            destination_config={"path": str(dest)},
            dlt_config=copy.deepcopy(case.dlt_config),
            dataset_name=DATASET,
            run_id=1,
        )
    except Exception as exc:  # noqa: BLE001 - a refusal is an outcome, judged below
        return judge(case.truth, None, exc)
    if case.after is not None:
        case.after()
    return judge(case.truth, landed_ids(dest), None)


@pytest.mark.parametrize("arm", [_param(arm) for arm in ARMS])
def test_a_green_run_loaded_everything_its_configuration_selected(
    arm: Arm, request, tmp_path, monkeypatch
):
    env = Env(tmp_path, monkeypatch)
    if "stub" in arm.needs:
        env.stub = request.getfixturevalue("stub")
    if "kafka" in arm.needs:
        env.kafka = request.getfixturevalue("kafka_broker")
    try:
        case = arm.build(env)
    except AssertionError as exc:
        # `await_setup` reports an unreachable container as an AssertionError. Under a known
        # instance's `xfail(raises=AssertionError)` that would read as "the defect is still there"
        # when the broker never answered at all (QA_RULES §5). Setup is never the verdict.
        raise HarnessError(f"{arm.id}: setup failed before the run: {exc}") from exc
    if case.source_type != arm.source_type:
        raise HarnessError(f"{arm.id} built a {case.source_type} case")
    if sum(1 for ids in case.truth.values() if ids) < 2 and case.spread < 2:
        raise HarnessError(f"{arm.id}: one unit in one batch cannot show that less was loaded")

    verdict = run_and_judge(case, tmp_path)

    assert not verdict.success_disagrees, f"{arm.id}: {verdict.explain()}"
    if arm.loadable and verdict.refused:
        raise RefusedALoadableConfiguration(f"{arm.id}: {verdict.refused}")


# ─────────────────────────────────────────────────────────────────────────────────────────────
# The verdict, tested directly — both directions, and the controls that attribute them
# ─────────────────────────────────────────────────────────────────────────────────────────────

_TRUTH = {"a": frozenset({1, 2, 3}), "b": frozenset({10, 11})}


class TestTheVerdict:
    def test_a_unit_with_no_table_disagrees(self):
        assert judge(_TRUTH, {"a": frozenset({1, 2, 3})}, None).missing == ("b",)

    def test_a_short_table_disagrees(self):
        verdict = judge(_TRUTH, {"a": frozenset({1, 2}), "b": frozenset({10, 11})}, None)
        assert verdict.success_disagrees and verdict.short == (("a", 2, 3),)

    def test_same_count_different_records_disagrees(self):
        """A count would pass this: 3 rows landed, and one of them is not a source record."""
        verdict = judge(_TRUTH, {"a": frozenset({1, 2, 99}), "b": frozenset({10, 11})}, None)
        assert verdict.short == (("a", 2, 3),)

    def test_control_everything_landed_agrees(self):
        assert not judge(_TRUTH, dict(_TRUTH), None).success_disagrees

    def test_control_more_than_configured_is_not_this_class(self):
        landed = {"a": frozenset({1, 2, 3, 4}), "b": frozenset({10, 11}), "extra": frozenset({7})}
        assert not judge(_TRUTH, landed, None).success_disagrees

    def test_control_a_unit_empty_at_the_source_needs_no_table(self):
        assert not judge(
            {"a": frozenset({1}), "b": frozenset()}, {"a": frozenset({1})}, None
        ).success_disagrees

    def test_a_refusal_never_disagrees_and_is_recorded(self):
        verdict = judge(_TRUTH, None, ValueError("no such file"))
        assert not verdict.success_disagrees
        assert verdict.refused == "ValueError: no such file"


def test_the_destination_reader_sees_a_table_it_did_not_write(tmp_path):
    """Positive control for ``landed_ids``: an empty result must not be the reader being blind."""
    dest = tmp_path / "dest.duckdb"
    con = duckdb.connect(str(dest))
    try:
        con.execute(f"CREATE SCHEMA {DATASET}")
        con.execute(f"CREATE TABLE {DATASET}.things (id INTEGER, label TEXT)")
        con.execute(f"CREATE TABLE {DATASET}._dlt_loads (load_id TEXT)")
        con.execute(f"INSERT INTO {DATASET}.things VALUES (1, 'x'), (2, 'y')")
    finally:
        con.close()
    assert landed_ids(dest) == {"things": frozenset({1, 2})}


# ─────────────────────────────────────────────────────────────────────────────────────────────
# The population — read from the product twice, and the ratchet over it
# ─────────────────────────────────────────────────────────────────────────────────────────────


def population_from_registry() -> set[str]:
    """What a user can pick as a source, plus the withdrawn types stored connections still carry."""
    return set(SOURCE_TYPES) | set(WITHDRAWN_SOURCE_TYPES)


def dispatch_sets_in(source: str) -> set[str]:
    """Names of the sets ``build_source`` tests ``connection_type`` against, read from its AST."""
    names: set[str] = set()
    for node in ast.walk(ast.parse(textwrap.dedent(source))):
        if not (isinstance(node, ast.Compare) and isinstance(node.left, ast.Name)):
            continue
        if node.left.id != "connection_type":
            continue
        for op, comparator in zip(node.ops, node.comparators, strict=True):
            if not isinstance(op, ast.In | ast.NotIn):
                continue
            if isinstance(comparator, ast.Name):
                names.add(comparator.id)
            elif isinstance(comparator, ast.Attribute) and isinstance(comparator.value, ast.Name):
                names.add(comparator.attr)
    return names


def population_from_dispatch() -> tuple[set[str], set[str]]:
    names = dispatch_sets_in(inspect.getsource(DltRunnerService.build_source))
    types: set[str] = set()
    for name in names:
        value = getattr(dlt_runner, name, None)
        if value is None:
            value = getattr(DltRunnerService, name)
        types |= set(value)
    return types, names


def coverage_gaps(population: set[str], armed: set[str], reasons: set[str]) -> dict:
    """Every way the arms and the stated reasons can disagree with the population. Pure."""
    return {
        "neither_armed_nor_explained": sorted(population - armed - reasons),
        "armed_yet_listed_as_not_probed": sorted(armed & reasons),
        "armed_but_not_a_source": sorted(armed - population),
        "explained_but_not_a_source": sorted(reasons - population),
    }


class TestThePopulation:
    def test_it_reads_the_same_two_ways(self):
        registry = population_from_registry()
        dispatch, set_names = population_from_dispatch()
        assert len(set_names) >= 8, f"the dispatch walk found only {sorted(set_names)}"
        assert len(registry) >= 30, f"the registry holds only {sorted(registry)}"
        assert registry == dispatch, (
            f"only offered as a source: {sorted(registry - dispatch)}; "
            f"only dispatched: {sorted(dispatch - registry)}"
        )

    def test_arming_a_walk_that_misses_a_dispatch_set_disagrees_with_the_registry(self):
        """The realistic failure is a SUBSET, not an empty walk (QA_RULES §2a)."""
        source = inspect.getsource(DltRunnerService.build_source).replace(
            "connection_type in SUPPORTED_SAAS_TYPES", "connection_type == 'nothing'"
        )
        names = dispatch_sets_in(source)
        assert "SUPPORTED_SAAS_TYPES" not in names and names, names
        subset = set().union(
            *(getattr(dlt_runner, n, None) or getattr(DltRunnerService, n) for n in names)
        )
        assert population_from_registry() != subset

    def test_every_source_type_has_an_arm_or_a_reason(self):
        population = population_from_registry()
        armed = {arm.source_type for arm in ARMS}
        gaps = coverage_gaps(population, armed, set(NOT_YET_PROBED))
        assert not any(gaps.values()), gaps
        docker = sorted({arm.source_type for arm in ARMS if "kafka" in arm.needs})
        print(
            f"\nclass probe: {len(armed)} of {len(population)} source types have an arm "
            f"({sorted(armed)}; {docker} only where Docker runs); "
            f"{len(NOT_YET_PROBED)} not yet probed"
        )

    def test_arming_a_new_source_type_with_no_arm_and_no_reason_is_refused(self):
        armed = {arm.source_type for arm in ARMS}
        planted = population_from_registry() | {"brand_new_connector"}
        gaps = coverage_gaps(planted, armed, set(NOT_YET_PROBED))
        assert gaps["neither_armed_nor_explained"] == ["brand_new_connector"]

    def test_arming_an_armed_type_still_listed_as_not_probed_is_refused(self):
        armed = {arm.source_type for arm in ARMS}
        gaps = coverage_gaps(population_from_registry(), armed, set(NOT_YET_PROBED) | {"sqlite"})
        assert gaps["armed_yet_listed_as_not_probed"] == ["sqlite"]

    def test_every_known_instance_names_an_arm(self):
        assert set(KNOWN_INSTANCES) <= {arm.id for arm in ARMS}

    def test_arm_ids_are_unique(self):
        ids = [arm.id for arm in ARMS]
        assert len(ids) == len(set(ids))

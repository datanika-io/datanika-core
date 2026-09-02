"""The ``path`` label must be bounded, and the module's prose about it must be true (#896).

``datanika/services/metrics.py`` labels ``http_requests_total`` and
``http_request_duration_seconds`` with the request path. That label value is
**unauthenticated input from the public internet**, and every distinct value mints
a new Prometheus time series.

Measured on production over the 30-day retention window (core#896):

===================================================== =========
distinct ``path`` label values ever seen                  298
of those, seen only as ``status="404"``                   290
``http_request_duration_seconds_bucket`` series           3,520
share of the entire TSDB                                  **31 %**
===================================================== =========

The 290 are vulnerability scanners — ``/api/.env/wp-includes/wso112233``,
``/api/vendor/phpunit/phpunit/src/Util/PHP/eval-stdin.php`` and 288 more. Growth
is monotonic in how creative a scanner gets and has no ceiling.

Why this file exists at all
---------------------------
``datanika/services/metrics.py`` had **no direct test of any kind** before it, so
every possible break in its path handling was missed by the whole suite. That
clears the "does this test earn its place" bar (``docs/QA_RULES.md`` §4) by
inspection rather than by argument.

Everything here drives the REAL Starlette router
------------------------------------------------
Not a stub ASGI app. The distinction is the whole point of the file: whether a
request matched a route is a fact the *router* records in the scope, and the
middleware reads that scope after the router has mutated it. A stub app cannot
produce the matched and unmatched cases, so a suite built on one would be
asserting against a world where every request looks the same
(``docs/QA_RULES.md`` §6).

🚨 Driving the real router also refuted core#896's own proposed fix. The issue
says *"Starlette exposes the matched route on ``scope["route"]`` … so the
middleware can read ``route.path``"*. Measured against **starlette 0.52.1**, the
version this project runs: the string ``"route"`` is never used as a scope key
anywhere in the package, and ``scope.get("route")`` is ``None`` for matched and
unmatched requests alike. What the router actually sets is::

    /api/v1/pipelines/7                    endpoint=<fn>  path_params={'pipeline_id': 7}
    /api/v1/connections                    endpoint=<fn>  path_params={}
    /api/.env/wp-includes/wso112233        (neither key present)

A fix written to the issue's instruction therefore reads ``None`` on every
request and buckets **everything** into ``<other>``. That satisfies the two
cardinality criteria perfectly and destroys the metric — including the two
REST-API latency SLIs in ``docs/slo_instruments.yml``, which select on
``path=~"/api/v1/connections|/api/v1/pipelines"``.
``test_control_permissive_distinct_routes_stay_distinct`` is the assertion that
catches it, and it is not a hypothetical: it is what the issue's own fix
instruction produces.

The workable derivation, from the same measurement: ``path_params`` supplies the
values to redact (yielding ``/api/v1/pipelines/:pipeline_id``) and the absence of
``endpoint`` identifies an unmatched request. That needs no regex guessing at
identifier shapes, and it collapses UUIDs and ULIDs for free.

What is asserted, and why each one is here
------------------------------------------
The controls come first on purpose. Four of them **pass against the unfixed
code**, which is what attributes a red in the four that do not: a bare red could
otherwise mean the harness never reached the middleware at all
(``docs/QA_RULES.md`` §3).

⚠️ core#896's original acceptance criterion 2 read *"a request to
``/api/v1/pipelines/7`` and one to ``/api/v1/pipelines/9`` produce one series"* —
**that already passes against the unfixed code**, because ``part.isdigit()``
handles exactly that case. It is kept here, correctly labelled as a *control*
rather than as the defect it was filed as. The real AC2 is the UUID/ULID case,
which the code demonstrably fails while its prose claims otherwise.

The xfail markers
-----------------
Four assertions describe the defect and are therefore
``xfail(strict=True, raises=AssertionError)``. **Strict is load-bearing**: the
moment the fix lands they XPASS, which is a failure, so the markers cannot
outlive the defect and the fix cannot land silently (``docs/QA_RULES.md`` §5).
Removing all four is part of the fix's PR.

Setup invariants raise ``HarnessError`` — a ``RuntimeError``, never an
``AssertionError`` — because a bare ``assert`` in the setup of an
``xfail(raises=AssertionError)`` test is absorbed by the marker, and a broken
harness then reads as a satisfied expected-failure, indistinguishable from the
defect being present. This has already earned its keep here: the first version of
``SCANNER_PATHS`` held 46 paths where the criterion names 50, and the guard said
so instead of quietly xfailing.

Not asserted here: core#896 AC3 (a checked-in ceiling on
``count(count_over_time(http_request_duration_seconds_bucket[30d]))``). That is a
statement about the production TSDB, scored by ``scripts/slo_report.py`` against
Prometheus, not by pytest.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Iterable

import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from datanika.services import metrics as metrics_module
from datanika.services.metrics import (
    PrometheusMiddleware,
    http_request_duration_seconds,
    http_requests_total,
)


class HarnessError(RuntimeError):
    """A failure of this test file, not of the code under test.

    Deliberately not an ``AssertionError``: the xfail markers below name
    ``raises=AssertionError``, so anything raised from setup must be of a
    different type or a broken harness is silently absorbed as an expected
    failure.
    """


# --- The real scanner corpus -------------------------------------------------
#
# The first four are verbatim from the production TSDB (core#896). The rest are
# in the same families, so the set is 50 distinct paths — the number core#896's
# AC1 names — while staying recognisably the traffic this actually meters.
REAL_SCANNER_PATHS = (
    "/api/.env/wp-includes/wso112233",
    "/api/vendor/phpunit/phpunit/src/Util/PHP/eval-stdin.php",
    "/api/config/.env.sendgrid.backup",
    "/api/.env:80/_profiler/phpinfo",
)

SCANNER_PATHS: tuple[str, ...] = (
    REAL_SCANNER_PATHS
    + tuple(f"/api/.env.{s}" for s in ("bak", "old", "save", "prod", "dev", "local", "dist"))
    + tuple(f"/{p}/wp-includes/wso112233" for p in ("wp", "blog", "cms", "old", "site", "shop"))
    + tuple(f"/vendor/phpunit/{n}/eval-stdin.php" for n in range(1, 12))
    + tuple(f"/.git/{p}" for p in ("config", "HEAD", "index", "logs/HEAD", "refs/heads/master"))
    + tuple(f"/actuator/{p}" for p in ("env", "health", "heapdump", "gateway/routes"))
    + tuple(
        f"/{p}/.aws/credentials"
        for p in ("home", "root", "app", "srv", "opt", "var", "usr", "tmp", "mnt")
    )
    + (
        "/cgi-bin/luci/;stok=/locale",
        "/telescope/requests",
        "/_ignition/execute-solution",
        "/aws.yml",
    )
)

UUID_A = "550e8400-e29b-41d4-a716-446655440000"
UUID_B = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
ULID_A = "01HQ8Z9Y3K4M5N6P7Q8R9S0T1V"
ULID_B = "01HQ8Z9Y3K4M5N6P7Q8R9S0T1W"


# --- Reading the metric the way Prometheus reads it --------------------------


def _path_label_values(*collectors: object) -> set[str]:
    """Every ``path`` label value currently present in the exposition.

    Read through ``collect()`` rather than a collector's private child map,
    because ``collect()`` is what ``generate_latest`` — and therefore Prometheus
    — actually sees. A normaliser that bounded the child map but not the
    exposition would be a fix that fixes nothing.
    """
    values: set[str] = set()
    for collector in collectors:
        for family in collector.collect():  # type: ignore[attr-defined]
            for sample in family.samples:
                path = sample.labels.get("path")
                if path is not None:
                    values.add(path)
    return values


def _path_counts() -> dict[str, float]:
    """``path`` label value -> total ``http_requests_total`` across methods/statuses."""
    counts: dict[str, float] = {}
    for family in http_requests_total.collect():
        for sample in family.samples:
            if not sample.name.endswith("_total"):
                continue
            path = sample.labels.get("path")
            if path is not None:
                counts[path] = counts.get(path, 0.0) + sample.value
    return counts


async def _label_for(path: str) -> str | None:
    """Meter exactly one request and return the label value it landed on.

    ⚠️ **Attribution is by counter increment, not by "which label values are new".**
    ``http_requests_total`` lives on the process-wide default registry, so a value
    an earlier test already created is not new when a later test produces it — and
    a delta-of-sets reads that as *"this path produced nothing"*. The first version
    of this file did exactly that and reported two working cases as broken,
    including one where the code is correct. A count that rose is the same fact
    whatever ran before.

    ``None`` means the request was not metered at all, which is the correct answer
    for a ``_SKIP_PREFIXES`` path.
    """
    before = _path_counts()
    await _drive([path])
    after = _path_counts()

    moved = sorted(k for k, v in after.items() if v > before.get(k, 0.0))
    if len(moved) > 1:
        raise HarnessError(
            f"one request to {path!r} incremented {len(moved)} label values "
            f"({moved}); a single request must meter exactly once"
        )
    return moved[0] if moved else None


async def _endpoint(request):  # noqa: ANN001, ANN202 - a Starlette endpoint
    return PlainTextResponse("ok")


# The route table only has to be representative of the *shapes* production
# serves: an int-converted param, a bare string param (which is what a UUID or a
# ULID arrives as), and two parameterless routes that must stay distinguishable.
ROUTES = [
    Route("/api/v1/pipelines", _endpoint),
    Route("/api/v1/pipelines/{pipeline_id:int}", _endpoint),
    Route("/api/v1/connections", _endpoint),
    Route("/api/v1/connections/{connection_id}", _endpoint),
    Route("/api/v1/runs/{run_id}", _endpoint),
]


async def _drive(paths: Iterable[str], *, method: str = "GET") -> None:
    """Send each path through the middleware wrapped around a REAL Starlette router.

    The router is what decides matched vs unmatched and what it records in the
    scope, so a stub app here would make every request look identical and every
    assertion below vacuous.
    """
    wanted = list(paths)
    middleware = PrometheusMiddleware(Starlette(routes=ROUTES))
    statuses: list[int] = []

    async def receive() -> dict:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict) -> None:
        if message["type"] == "http.response.start":
            statuses.append(message["status"])

    for path in wanted:
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "root_path": "",
            "headers": [],
            "client": ("127.0.0.1", 5000),
            "server": ("testserver", 80),
        }
        await middleware(scope, receive, send)

    if len(statuses) != len(wanted):
        raise HarnessError(
            f"the router produced {len(statuses)} response starts for {len(wanted)} "
            "requests — it did not run for all of them, so nothing below is a "
            "measurement"
        )


# ---------------------------------------------------------------------------
# Controls — these pass against the UNFIXED code. They are what attributes a red.
# ---------------------------------------------------------------------------


async def test_control_router_distinguishes_matched_from_unmatched() -> None:
    """The harness itself: the router must 200 a real route and 404 a junk one.

    Without this, "everything became ``<other>``" and "the router never ran" are
    the same observation.
    """
    seen: list[int] = []

    async def receive() -> dict:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict) -> None:
        if message["type"] == "http.response.start":
            seen.append(message["status"])

    app = Starlette(routes=ROUTES)
    for path in ("/api/v1/connections", REAL_SCANNER_PATHS[0]):
        await app(
            {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.3"},
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": path,
                "raw_path": path.encode(),
                "query_string": b"",
                "root_path": "",
                "headers": [],
                "client": ("127.0.0.1", 5000),
                "server": ("testserver", 80),
            },
            receive,
            send,
        )

    assert seen == [200, 404], (
        f"expected a match then a miss, got {seen}. The route table in this file no "
        "longer represents both cases, so every cardinality assertion here is vacuous."
    )


async def test_control_integer_segments_collapse() -> None:
    """``/api/v1/pipelines/7`` and ``/9`` already produce one label value.

    Filed as core#896's AC2, and it **passes against the unfixed code** —
    ``part.isdigit()`` is exactly this case. A control, not a criterion: it proves
    the harness reached the metering code and that it does something rather than
    passing the raw path through.
    """
    seven = await _label_for("/api/v1/pipelines/7")
    nine = await _label_for("/api/v1/pipelines/9")

    assert seven == nine, f"two integer ids on one route produced {seven!r} and {nine!r}"
    assert seven != "/api/v1/pipelines/7", (
        "the raw path became the label value; nothing is normalising at all, so "
        "every other result in this file is suspect"
    )


async def test_control_skipped_prefixes_are_not_metered() -> None:
    """``/healthz`` and friends produce no series at all.

    Proves ``_drive`` exercises the middleware's real branch structure rather
    than only its metering tail.
    """
    metered = {
        path: await _label_for(path)
        for path in ("/healthz", "/readyz", "/metrics", "/static/app.css", "/_next/x.js")
    }

    assert set(metered.values()) == {None}, (
        f"skipped prefixes were metered: { {k: v for k, v in metered.items() if v} }. "
        "_SKIP_PREFIXES is the one bound that already exists; losing it would make "
        "the cardinality problem strictly worse."
    )


async def test_control_permissive_distinct_routes_stay_distinct() -> None:
    """Two different real routes must remain two different label values.

    🔑 **The control most likely to be missed** (``docs/QA_RULES.md`` §3), and the
    one that catches the fix core#896 itself proposes. Reading ``scope["route"]``
    — a key starlette 0.52.1 never sets — yields ``None`` for every request, so
    every path becomes ``<other>``: perfect cardinality, and both REST-API latency
    SLIs in ``docs/slo_instruments.yml`` go blind, because they select on
    ``path=~"/api/v1/connections|/api/v1/pipelines"``.
    """
    connections = await _label_for("/api/v1/connections")
    pipelines = await _label_for("/api/v1/pipelines")

    assert connections != pipelines, (
        f"two distinct real routes both metered as {connections!r}. Collapsing real "
        "routes satisfies every cardinality assertion in this file and leaves the "
        "metric unable to answer anything it exists for."
    )
    assert connections is not None and pipelines is not None, (
        f"a real route was not metered at all: connections={connections!r}, pipelines={pipelines!r}"
    )


async def test_control_different_routes_with_ids_stay_distinct() -> None:
    """``/api/v1/pipelines/7`` and ``/api/v1/connections/7`` are two things.

    The second permissive control. A fix that collapses *anything containing an
    identifier* into one bucket passes AC1 and AC2 and loses the resource.
    """
    pipeline = await _label_for("/api/v1/pipelines/7")
    connection = await _label_for("/api/v1/connections/7")

    assert pipeline != connection, (
        f"two different routes carrying an id both metered as {pipeline!r}; the "
        "resource is gone from the label"
    )


# ---------------------------------------------------------------------------
# The defect. Each of these is red against the unfixed code.
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="core#896: unmatched paths are used verbatim as label values; no <other> bucket exists",
)
async def test_unmatched_paths_collapse_to_one_bucket() -> None:
    """core#896 AC1 — 50 unmatched paths must add at most one label value.

    Unfixed, this adds 50. The assertion is on the *delta*, so it is unaffected by
    whatever earlier tests in the session already metered.
    """
    distinct = set(SCANNER_PATHS)
    if len(distinct) != 50:
        raise HarnessError(
            f"the corpus holds {len(distinct)} distinct paths, not 50; core#896's "
            "AC1 is stated over 50 and the number is part of the criterion"
        )

    landed = {await _label_for(path) for path in sorted(distinct)}

    assert len(landed) <= 1, (
        f"50 distinct unmatched paths landed on {len(landed)} label values. Each is "
        f"~14 Prometheus series once the histogram is counted. Sample: "
        f"{sorted(v for v in landed if v)[:3]}"
    )
    # The two metrics are labelled from the same value, so a fix that bounds one
    # and not the other would be a fix that fixes half the series.
    counter_paths = _path_label_values(http_requests_total)
    histogram_paths = _path_label_values(http_request_duration_seconds)
    assert counter_paths == histogram_paths, (
        "the counter and the histogram disagree about which paths exist: "
        f"counter-only={sorted(counter_paths - histogram_paths)[:3]}, "
        f"histogram-only={sorted(histogram_paths - counter_paths)[:3]}"
    )


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="core#896: nothing produces a '<other>' bucket, so unmatched paths have no name",
)
async def test_unmatched_paths_land_in_a_named_other_bucket() -> None:
    """The bucket has to be *named*, not merely singular.

    Asserted separately from AC1 because the two fail for different reasons and a
    combined red would not say which. The name is what makes the bucket legible in
    a query and lets an SLI selector exclude it, instead of enumerating every
    route it does not want.
    """
    landed = {await _label_for(path) for path in REAL_SCANNER_PATHS}

    assert landed == {"<other>"}, (
        f"the four scanner paths taken verbatim off the production TSDB landed on "
        f"{sorted(str(v) for v in landed)}, not on a single '<other>' bucket."
    )


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="core#896 AC2 (revised): only integer segments collapse; UUID and ULID ids do not",
)
async def test_uuid_and_ulid_identifiers_collapse() -> None:
    """core#896 AC2, as revised — the criterion the code actually fails.

    The original AC2 used integer ids and passed unfixed; it is
    ``test_control_integer_segments_collapse`` above. These are the identifier
    shapes real resources carry, and both routes here are *matched* routes, so a
    correct fix has the router's ``path_params`` to work from.
    """
    uuid_a = await _label_for(f"/api/v1/connections/{UUID_A}")
    uuid_b = await _label_for(f"/api/v1/connections/{UUID_B}")
    ulid_a = await _label_for(f"/api/v1/runs/{ULID_A}")
    ulid_b = await _label_for(f"/api/v1/runs/{ULID_B}")

    assert uuid_a == uuid_b, (
        f"two UUIDs on one route metered as {uuid_a!r} and {uuid_b!r}. Every "
        "connection a customer creates will mint its own time series."
    )
    assert ulid_a == ulid_b, (
        f"two ULIDs on one route metered as {ulid_a!r} and {ulid_b!r}. Every run "
        "will mint its own time series."
    )


# --- Prose that a reader will act on is a specification ---------------------
#
# core#673 shipped the same shape today: a docstring asserted the reach of a
# guard ("~20 mutating handlers already route through it, so one guard covers
# them all"), the claim was never tested, and eight handlers were outside it.
# `_normalize_path`'s docstring makes a narrower claim of the same kind, and it
# is likewise false — which is exactly what would have got core#896 closed as
# stale by a reader who grepped `metrics.py` and read that one sentence.

# Each identifier kind a docstring may name, and a pair of paths differing in
# exactly one identifier of that kind. A named kind with no demonstrator is a
# harness failure, not a pass.
_KIND_DEMONSTRATORS: dict[str, tuple[str, str]] = {
    "id": ("/api/v1/pipelines/7", "/api/v1/pipelines/9"),
    "ids": ("/api/v1/pipelines/7", "/api/v1/pipelines/9"),
    "integer": ("/api/v1/pipelines/7", "/api/v1/pipelines/9"),
    "integers": ("/api/v1/pipelines/7", "/api/v1/pipelines/9"),
    "numeric": ("/api/v1/pipelines/7", "/api/v1/pipelines/9"),
    "uuid": (f"/api/v1/connections/{UUID_A}", f"/api/v1/connections/{UUID_B}"),
    "uuids": (f"/api/v1/connections/{UUID_A}", f"/api/v1/connections/{UUID_B}"),
    "ulid": (f"/api/v1/runs/{ULID_A}", f"/api/v1/runs/{ULID_B}"),
    "ulids": (f"/api/v1/runs/{ULID_A}", f"/api/v1/runs/{ULID_B}"),
    "slug": ("/api/v1/connections/acme-corp", "/api/v1/connections/globex-inc"),
    "slugs": ("/api/v1/connections/acme-corp", "/api/v1/connections/globex-inc"),
}

_PARENTHETICAL_RE = re.compile(r"\(([^)]*)\)")


# Words that NAME a kind of identifier. Deliberately a superset of
# _KIND_DEMONSTRATORS: a claim about a kind we cannot demonstrate must raise
# rather than be dropped.
#
# ⚠️ The first version of the scanner returned only tokens already present in
# _KIND_DEMONSTRATORS, which made the "unrecognised claim" guard below **dead
# code** — a guard that cannot fire, in a file about guards that cannot fire. The
# two lists have to be different for that check to mean anything: this one is
# what counts as a claim, the other is what we can prove.
_IDENTIFIER_WORDS = frozenset(
    {
        "id",
        "ids",
        "identifier",
        "identifiers",
        "integer",
        "integers",
        "numeric",
        "uuid",
        "uuids",
        "guid",
        "guids",
        "ulid",
        "ulids",
        "cuid",
        "cuids",
        "nanoid",
        "nanoids",
        "slug",
        "slugs",
        "hash",
        "hashes",
        "token",
        "tokens",
        "key",
        "keys",
        "pk",
        "pks",
        "primary key",
        "primary keys",
    }
)


def claimed_identifier_kinds(text: str | None) -> list[str]:
    """The identifier kinds a piece of prose promises to replace.

    Derived from the prose rather than hardcoded, so the assertion tracks what
    the module currently says. Rewording to drop a false claim is a legitimate
    fix and this has to follow it there — **the defect is the mismatch, not any
    particular word**.

    Only *identifier* words are returned. The module docstring also contains
    ``(count, latency)``, which is a parenthetical and not a claim about path
    normalisation; matching every parenthetical would make this raise on prose
    that promises nothing.
    """
    if not text:
        return []
    kinds: list[str] = []
    for group in _PARENTHETICAL_RE.findall(text):
        for token in re.split(r"[,/]| and | or ", group):
            token = token.strip().strip(".").lower()
            if token in _IDENTIFIER_WORDS:
                kinds.append(token)
    return list(dict.fromkeys(kinds))


def test_control_the_claim_scanner_can_find_a_claim() -> None:
    """A permanent negative control for the scanner below.

    Once the prose is corrected, ``test_module_prose_about_identifiers_is_true``
    passes by finding nothing — and "found nothing" is indistinguishable from "the
    scanner is broken" unless something proves the scanner still works. This runs
    it over a fixed string with a known claim, so it cannot decay with the module.
    """
    found = claimed_identifier_kinds("Replace dynamic segments (IDs, UUIDs) with placeholders.")

    assert found == ["ids", "uuids"], (
        f"the claim scanner returned {found} for a sentence that plainly names IDs "
        "and UUIDs. A scanner that finds nothing makes the assertion below vacuous."
    )

    # A parenthetical that promises nothing about path normalisation. The module
    # docstring contains exactly this one, and matching it would make the
    # assertion below raise on prose that makes no claim.
    assert claimed_identifier_kinds("HTTP request metrics (count, latency) via middleware") == []

    # 🔑 The branch that matters: a kind we can RECOGNISE but cannot DEMONSTRATE.
    # This is what makes `unknown` in the test below reachable at all. Without it,
    # the scanner's own filter would drop such a claim and the HarnessError could
    # never fire — a guard that cannot fire, inside a file about guards that
    # cannot fire.
    unprovable = claimed_identifier_kinds("Replace dynamic segments (IDs, GUIDs).")
    assert unprovable == ["ids", "guids"], unprovable
    assert "guids" not in _KIND_DEMONSTRATORS, (
        "a demonstrator was added for 'guids', so this control no longer proves "
        "the unrecognised-claim branch is reachable. Pick another word that this "
        "file can name but cannot prove."
    )


def _module_prose() -> list[tuple[str, str]]:
    """(where, text) for every docstring in ``datanika/services/metrics.py``."""
    prose: list[tuple[str, str]] = [("<module>", metrics_module.__doc__ or "")]
    for name, obj in vars(metrics_module).items():
        if getattr(obj, "__module__", None) != metrics_module.__name__:
            continue
        if inspect.isfunction(obj) or inspect.isclass(obj):
            prose.append((name, inspect.getdoc(obj) or ""))
    return prose


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "core#896: _normalize_path's docstring says '(IDs, UUIDs)' and UUIDs are not "
        "handled — a false prose coverage claim, the same shape core#673 fixed today"
    ),
)
async def test_module_prose_about_identifiers_is_true() -> None:
    """Every identifier kind the module's prose names must actually be collapsed.

    ``_normalize_path``'s docstring reads *"Replace dynamic segments (IDs, UUIDs)
    with placeholders."* A reader who greps ``metrics.py``, finds a normaliser and
    reads that sentence could reasonably close core#896 as stale. It is not stale:
    the function collapses ``part.isdigit()`` segments and nothing else.

    Either half may be repaired. Fixing the code makes the claim true; deleting
    ``UUIDs`` from the sentence makes it honest. This passes on either, and on
    nothing else. It scans the whole module rather than one function name so that
    a refactor moves the assertion with the prose instead of silencing it.
    """
    claims = [
        (where, kind) for where, text in _module_prose() for kind in claimed_identifier_kinds(text)
    ]

    unknown = sorted({k for _, k in claims} - set(_KIND_DEMONSTRATORS))
    if unknown:
        raise HarnessError(
            f"the module's prose claims to handle {unknown}, and this file has no "
            "pair of paths demonstrating those kinds. Add them to "
            "_KIND_DEMONSTRATORS — an unrecognised claim must not read as a "
            "satisfied one."
        )

    broken: list[str] = []
    for where, kind in claims:
        first, second = _KIND_DEMONSTRATORS[kind]
        first_label = await _label_for(first)
        second_label = await _label_for(second)
        if first_label != second_label:
            broken.append(
                f"{where} claims '{kind}'; {first} -> {first_label!r} but "
                f"{second} -> {second_label!r}"
            )

    assert not broken, (
        "the module's prose says these identifier kinds are replaced with "
        f"placeholders, and they are not: {broken}. Prose a reader will act on is "
        "a specification; this one points a reader at closing core#896 as stale."
    )

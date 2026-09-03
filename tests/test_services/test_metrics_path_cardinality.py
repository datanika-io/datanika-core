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

The xfail markers are gone, and that is the record of the fix
--------------------------------------------------------------
Four assertions described the defect and shipped as
``xfail(strict=True, raises=AssertionError)``. **Strict was load-bearing**: an
XPASS is a failure, so the markers could not outlive the defect. All four were
removed when the fix landed (core#896, Engineering) and the assertions now run
as ordinary tests — which is the only state in which they can catch a
*regression* rather than merely pin a known defect.

⚠️ core#896 was once closed **COMPLETED with only this file shipped** — 624
insertions, no production code — while all four markers were still live and
naming the defect. If you are reading this because the issue looks settled,
the check is ``git log -- datanika/services/metrics.py``, not the issue state.

Three assertions were added by the fix, for surfaces the issue does not cover
-----------------------------------------------------------------------------
``<other>`` is a bucket for **unmatched** paths, and the two worst surfaces here
are *matched*:

* ``/api/auth/sso/login/{org_slug}`` and three sibling auth routes take a
  free-form segment from an unauthenticated caller and match a real route, so no
  unmatched-path bucket reaches them (all 34 ``/api/v1/*`` parameters are
  ``:int`` and were already collapsed by ``part.isdigit()``);
* ``app._api`` carries two ``Mount``s — Reflex puts socket.io at ``/_event`` and
  a ``StaticFiles`` server at ``/_upload`` — and a ``Mount`` matches, sets
  ``endpoint``, then hands an arbitrary tail to a sub-application that reports
  nothing about what it matched.

The third, ``test_the_producible_label_set_is_bounded_by_the_route_table``, is
the class statement the other two are instances of, and the only one that can
catch a surface nobody has thought of yet.

Setup invariants raise ``HarnessError`` — a ``RuntimeError``, never an
``AssertionError`` — because a bare ``assert`` in the setup of an
``xfail(raises=AssertionError)`` test is absorbed by the marker, and a broken
harness then reads as a satisfied expected-failure, indistinguishable from the
defect being present. This has already earned its keep here: the first version of
``SCANNER_PATHS`` held 46 paths where the criterion names 50, and the guard said
so instead of quietly xfailing.

core#896 AC3 is asserted here in the only form pytest can hold: a ceiling on the
label values the middleware can *produce*, derived from the route table rather
than written down as a number. The production half — a ceiling on
``count(count_over_time(http_request_duration_seconds_bucket[30d]))`` — is a
statement about the TSDB and belongs to ``scripts/slo_report.py``.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Iterable

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Mount, Route

from datanika.services import metrics as metrics_module
from datanika.services.metrics import (
    UNMATCHED_PATH_LABEL,
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


async def _mounted_app(scope, receive, send):  # noqa: ANN001, ANN201 - a bare ASGI app
    """A sub-application mounted under a prefix.

    Stands in for the two real ones on ``app._api``: Reflex mounts socket.io at
    ``/_event`` and a ``StaticFiles`` server at ``/_upload``. A ``Mount`` serves
    an arbitrary tail and reports **nothing** about what it matched, so the tail
    is caller-controlled on a route that nonetheless *matched*.
    """
    await PlainTextResponse("ok")(scope, receive, send)


# The route table only has to be representative of the *shapes* production
# serves: an int-converted param, a bare string param (which is what a UUID or a
# ULID arrives as), two parameterless routes that must stay distinguishable, the
# free-form caller-supplied segments the auth routes take, and a Mount.
ROUTES = [
    Route("/api/v1/pipelines", _endpoint),
    Route("/api/v1/pipelines/{pipeline_id:int}", _endpoint),
    Route("/api/v1/connections", _endpoint),
    Route("/api/v1/connections/{connection_id}", _endpoint),
    Route("/api/v1/runs/{run_id}", _endpoint),
    # Verbatim shapes from datanika/services/{sso,oauth}_routes.py. The segment
    # is invented by an unauthenticated caller and the route MATCHES, so an
    # "<other>" bucket for unmatched paths does not reach it.
    Route("/api/auth/sso/login/{org_slug}", _endpoint),
    Route("/api/auth/login/{provider}", _endpoint),
    # Reflex's own mounts, in shape: a matched prefix with an unbounded tail.
    Mount("/_files", app=_mounted_app),
]

# Every parameterless route above, plus one template per parameterised route.
# Used as the ceiling in ``test_the_producible_label_set_is_bounded``: the fix's
# whole claim is that the label value comes from the ROUTE TABLE and never from
# the request, so the table's size is the bound.
DISTINCT_ROUTE_TEMPLATES = 8


def _literal_collision_paths() -> list[str]:
    """One path per (single-string-parameter route) x (literal segment of that same route).

    core#1020. The ceiling test's **predicate** was always right; its **corpus** was
    ``tenant-0…29`` / ``provider-0…29`` — every value invented, and not one equal to a
    literal of its own route. The attack was never in the sample, so a guard that would
    have fired for free stayed green.

    Derived from ``ROUTES`` rather than listed, per core#1020 AC2: a route added tomorrow
    is covered with no edit here, which is the whole property that makes a ceiling worth
    keeping. A hardcoded list of ten paths would close today's instance and nothing else.

    Only ``str`` parameters qualify — an ``int`` convertor cannot take a word, so all 34
    ``/api/v1/*`` parameters in production are immune and would generate 404s here rather
    than collisions.
    """
    paths: list[str] = []
    for route in ROUTES:
        template = getattr(route, "path", "")
        segments = [s for s in template.split("/") if s]
        params = [s for s in segments if s.startswith("{") and s.endswith("}")]
        literals = [s for s in segments if not s.startswith("{")]
        if len(params) != 1:
            continue
        declaration = params[0][1:-1]
        if ":" in declaration and not declaration.endswith(":str"):
            continue
        paths.extend(template.replace(params[0], literal) for literal in literals)
    return paths


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


async def test_free_form_segments_on_a_matched_route_collapse() -> None:
    """core#896's extra AC — the vector an ``<other>`` bucket does **not** reach.

    ``/api/auth/sso/login/{org_slug}``, ``/api/auth/sso/metadata/{org_slug}``,
    ``/api/auth/login/{provider}`` and ``/api/auth/callback/{provider}`` take a
    segment invented by an unauthenticated caller, are outside
    ``_SKIP_PREFIXES``, and **match a real route**. So they are not "unmatched",
    and AC1's ``<other>`` bucket for unmatched paths leaves every one of them
    minting a permanent series per slug.

    All 34 ``/api/v1/*`` parameters are ``:int`` and were already collapsed by
    ``part.isdigit()``; these four are the exposed surface, which is why a fix
    has to derive the label from **which segments the router treated as
    parameters** rather than from what the segments look like. No shape test
    distinguishes ``acme`` from ``login``.
    """
    slugs = [f"tenant-{n}" for n in range(50)]
    landed = {await _label_for(f"/api/auth/sso/login/{slug}") for slug in slugs}

    assert len(landed) == 1, (
        f"50 invented org slugs on ONE matched route landed on {len(landed)} label "
        f"values. Sample: {sorted(str(v) for v in landed)[:3]}"
    )
    (label,) = landed
    assert label not in (None, "<other>"), (
        f"a real route metered as {label!r}. Collapsing matched routes into the "
        "unmatched bucket is the failure mode "
        "test_control_permissive_distinct_routes_stay_distinct exists for; the "
        "slug must be redacted while the route survives."
    )

    # Two different free-form routes must stay two different series, exactly as
    # two different int-parameterised routes do.
    provider = await _label_for("/api/auth/login/google")
    assert provider != label, (
        f"/api/auth/login/{{provider}} and /api/auth/sso/login/{{org_slug}} both "
        f"metered as {label!r}"
    )


async def test_mounted_subapp_tails_are_bounded() -> None:
    """A ``Mount`` matches, so its caller-controlled tail is not "unmatched" either.

    ``app._api`` carries two mounts that core#896 does not mention and that no
    ``_SKIP_PREFIXES`` entry covers: Reflex mounts socket.io at ``/_event`` and a
    ``StaticFiles`` server at ``/_upload`` (``reflex/app.py``), and Apache routes
    both to the backend. ``Mount.matches`` sets ``endpoint`` and extends
    ``root_path``, then hands the remainder to a sub-application that reports
    nothing back — so every filename a caller asks for arrives at the metering
    code as a matched request with a distinct path.
    """
    landed = {await _label_for(f"/_files/{n}/report-{n}.csv") for n in range(50)}

    assert len(landed) == 1, (
        f"50 distinct tails under one mount landed on {len(landed)} label values. "
        f"Sample: {sorted(str(v) for v in landed)[:3]}"
    )
    (label,) = landed
    assert label is not None, "a mounted request was not metered at all"
    assert "report-" not in label, (
        f"the mounted tail reached the label verbatim: {label!r}. The tail is "
        "unauthenticated input and the sub-application does not tell us what it "
        "matched, so nothing about it may become a label value."
    )


async def test_a_value_equal_to_its_own_routes_literal_never_rotates_the_template() -> None:
    """core#1020 AC1/AC3, named rather than counted.

    ``_redact_path_params`` walked left to right and replaced the **first** segment equal
    to a parameter's value. When that value equals a literal appearing *earlier in the same
    route*, the literal was redacted and the parameter's own segment emitted verbatim —
    minting ``/api/auth/sso/:org_slug/login`` and three more rotations of the same
    template, all caller-chosen and unauthenticated.

    ⚠️ **This asserts something narrower than core#1020's AC1, deliberately, and the
    difference is the fix that was chosen.** AC1 asks for *"the same label as any other
    value for that route"*, which requires knowing which segment the router matched. We do
    not know: Starlette 0.52.1 leaves ``scope["route"]`` unset (see ``_normalize_path``),
    so the position is recovered by searching for the value, and **no fixed search
    direction is universally correct**. Leftmost mislabels this case; rightmost mislabels
    ``/api/v1/orgs/{org_id}/members`` with ``org_id=members``, the case the shipped
    docstring is about. So the fix refuses to guess: an ambiguous placement returns
    ``<other>``, which is the honest answer rather than a confident wrong one. Exact
    recovery needs an endpoint → template index and is filed separately.

    What must hold, and does:
      * no rotated template is ever produced — that is the defect;
      * the value lands in the already-bounded set;
      * a value that is *not* a literal still gets the route's own label (the control,
        without which a fix that sends everything to ``<other>`` would pass here).
    """
    ordinary = await _label_for("/api/auth/sso/login/acme")
    if ordinary != "/api/auth/sso/login/:org_slug":
        raise HarnessError(
            f"an ordinary slug produced {ordinary!r}, not the route's template. The "
            "collision assertion below would be measuring something else."
        )

    for path in ("/api/auth/sso/login/login", "/api/auth/sso/login/sso", "/api/auth/sso/login/api"):
        label = await _label_for(path)
        assert label != ordinary.replace("login/:org_slug", ":org_slug/login"), (
            f"{path} produced the rotated template {label!r} — a second, caller-chosen "
            "series for a route that already has one, and a template that is not a route."
        )
        assert label in {ordinary, UNMATCHED_PATH_LABEL}, (
            f"{path} produced {label!r}, which is neither the route's own template nor "
            f"the bounded {UNMATCHED_PATH_LABEL!r} bucket."
        )


async def test_the_producible_label_set_is_bounded_by_the_route_table() -> None:
    """core#896 AC3, expressed where pytest can assert it: a checked-in ceiling.

    AC3 as filed —
    ``count(count_over_time(http_request_duration_seconds_bucket[30d]))`` against
    a ceiling — is a statement about the production TSDB and is scored by
    ``scripts/slo_report.py``. The claim *underneath* it is checkable here and is
    the one that actually has to hold: **every label value comes from the route
    table, so the route table is the bound.** An adversary picking paths cannot
    raise it.

    This is the assertion that catches a *future* unbounded surface — a new
    ``Mount``, a new free-form parameter, a new sub-router — which the
    case-by-case tests above cannot, because they each name a shape somebody
    already thought of.
    """
    collisions = _literal_collision_paths()
    corpus = (
        list(SCANNER_PATHS)
        + [f"/api/auth/sso/login/tenant-{n}" for n in range(30)]
        + [f"/api/auth/login/provider-{n}" for n in range(30)]
        + [f"/_files/{n}/report-{n}.csv" for n in range(30)]
        + [f"/api/v1/pipelines/{n}" for n in range(30)]
        + [f"/api/v1/connections/{UUID_A[:-1]}{c}" for c in "0123456789abcdef"]
        + [f"/api/v1/runs/{ULID_A[:-1]}{c}" for c in "0123456789ABCDEF"]
        # core#1020: the shape the corpus never contained.
        + collisions
    )
    if len(corpus) < 200:
        raise HarnessError(
            f"the corpus holds {len(corpus)} paths; a ceiling asserted over a "
            "handful of requests is satisfied by arithmetic rather than by the fix"
        )
    if "/api/auth/sso/login/login" not in collisions:
        raise HarnessError(
            f"the derived collision corpus is {collisions!r}; it does not contain the "
            "path core#1020 was filed on, so the ceiling is not being asked the "
            "question that motivated it"
        )

    landed = {await _label_for(path) for path in corpus}
    ceiling = DISTINCT_ROUTE_TEMPLATES + 1  # + the single unmatched bucket

    assert len(landed) <= ceiling, (
        f"{len(corpus)} adversarially chosen paths produced {len(landed)} distinct "
        f"label values against a ceiling of {ceiling} (one per route template in "
        f"ROUTES, plus '<other>'). Each costs ~14 Prometheus series once the "
        f"histogram is counted. Sample of the excess: "
        f"{sorted(str(v) for v in landed)[:5]}"
    )
    assert len(landed) > 1, (
        "everything collapsed into a single value, which passes every cardinality "
        "assertion and destroys the metric. See "
        "test_control_permissive_distinct_routes_stay_distinct."
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

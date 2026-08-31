"""SaaS connectors must follow pagination cursors (core#823).

**The defect.** ``_build_saas_source`` handed every SaaS connector to
``_rest_api_fallback``, which had no ``paginator`` parameter at all — so all 14
REST-fallback connectors depended on dlt's *runtime* auto-detection. Measured on
production: a Stripe account holding 15 customers landed **10**, and the run
reported ``success`` with a plausible row count. Stripe answered
``"has_more": true`` and nothing followed the cursor.

**Why it survived.** Every symptom of this bug is indistinguishable from a small
account. The Shopify capture taken an hour earlier loaded completely — because
that store held 17 products and genuinely fit in one page. Same loader, same
missing paginator, different dataset size. A connector smoke test on a fixture
account *cannot* detect this class, which is exactly why the load-bearing test
here serves **more rows than fit in one page** and asserts the count.

**What is asserted, and why structurally weaker forms are useless:**

* ``TestStripePagesThroughEveryCustomer`` drives the real production path over a
  real socket and asserts all 15 rows arrive. A test asserting "the run succeeds
  and lands rows" passes *right now*, with 10 of 15.
* ``test_control_without_a_paginator_stops_at_page_one`` is the negative control
  — the same server, the same code, the paginator withheld. It reproduces the
  production bug exactly (10 rows) and is what proves the test above is not
  passing for some unrelated reason. Without it, a server that quietly returned
  everything on page one would make the assertion above vacuous.
* ``TestDltConfigPaginatorIsHonoured`` — ``"paginator"`` was already an accepted
  ``dlt_config`` key (``dlt_runner.INTERNAL_CONFIG_KEYS``) that
  ``_build_saas_source`` never read, so the documented user-side workaround
  turned the run green having changed nothing. The assertion is *behavioural*
  (the override changes which rows arrive), because an assertion that the key is
  merely present is satisfied by the bug.
* ``TestABrokenPaginatorConfigIsRejected`` — dlt's own ``create_paginator``
  returns **None** for a dict with no ``"type"`` key, silently falling back to
  auto-detection. That turns a typo into the original bug wearing a fix's
  clothes, so it has to raise.
* ``TestEverySaasConnectorHasAPaginatorDecision`` is the durable guard: a new
  SaaS connector cannot be added without either a paginator or an explicit,
  reasoned exemption.
"""

import http.server
import json
import threading

import pytest

import datanika.services.dlt_runner as dlt_runner
from datanika.services.dlt_runner import DltRunnerError, DltRunnerService

#: Total customers the fake Stripe account holds. Must exceed PAGE_SIZE — a
#: fixture that fits in one page cannot fail, which is how the real bug lived.
TOTAL_CUSTOMERS = 15

#: Stripe's default page size, and the number that landed in production.
PAGE_SIZE = 10

#: A broken paginator that never advances would request forever. Bound it, so a
#: regression is a failed assertion rather than a hung suite.
MAX_REQUESTS = 12


class _StripeHandler(http.server.BaseHTTPRequestHandler):
    """A faithful-enough Stripe list endpoint: cursor + ``has_more``.

    Page size is fixed at ``PAGE_SIZE`` **regardless of any ``limit``**, which
    is what makes this exercise the paginator rather than the page-size bump.
    Real APIs cap page size the same way; here the cap is simply lower.
    """

    customers = [{"id": f"cus_{i:03d}", "name": f"Customer {i}"} for i in range(TOTAL_CUSTOMERS)]
    requests_seen: list[str] = []

    def do_GET(self):  # noqa: N802 - stdlib callback name
        type(self).requests_seen.append(self.path)
        if len(type(self).requests_seen) > MAX_REQUESTS:
            self.send_error(508, "paginator did not terminate")
            return

        start = 0
        if "starting_after=" in self.path:
            cursor = self.path.split("starting_after=")[1].split("&")[0]
            ids = [c["id"] for c in self.customers]
            start = ids.index(cursor) + 1 if cursor in ids else 0

        page = self.customers[start : start + PAGE_SIZE]
        body = json.dumps(
            {
                "object": "list",
                "url": "/v1/customers",
                "has_more": start + PAGE_SIZE < len(self.customers),
                "data": page,
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def stripe_api():
    _StripeHandler.requests_seen = []
    server = http.server.HTTPServer(("127.0.0.1", 0), _StripeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def local_egress(monkeypatch):
    """The egress guard blocks loopback by design; it has its own tests."""
    monkeypatch.setattr(dlt_runner, "validate_egress_host", lambda url: None)
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    # core#405: the guarded session pins the address it resolves, so loopback
    # has to come back from that call too.
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
    )


@pytest.fixture
def redirect_to(monkeypatch):
    """Point a connector's hard-coded vendor URL at the local server.

    Only the *target* moves. ``_build_saas_source`` → ``_rest_api_fallback`` →
    ``_rest_api_from_parts`` all run for real, including the paginator argument
    whose absence is the bug — so this seam cannot mask the defect under test.
    """
    real = DltRunnerService._rest_api_from_parts

    def _redirect(base_url):
        def _patched(_ignored_base_url, resources, **kwargs):
            return real(base_url, resources, **kwargs)

        monkeypatch.setattr(DltRunnerService, "_rest_api_from_parts", staticmethod(_patched))

    return _redirect


def _stripe_rows(dlt_config=None):
    source = DltRunnerService()._build_saas_source(
        "stripe", {"api_key": "sk_test_not_a_real_key"}, dlt_config or {}
    )
    return list(source.resources["customers"])


class TestStripePagesThroughEveryCustomer:
    def test_all_fifteen_customers_arrive(self, stripe_api, local_egress, redirect_to):
        """The production reproduction: 15 in the account, 15 must land.

        Against the unfixed loader this yields 10 — a green run, a plausible
        row count, and five customers that are never mentioned.
        """
        redirect_to(stripe_api)
        rows = _stripe_rows()

        assert len(rows) == TOTAL_CUSTOMERS, (
            f"loaded {len(rows)} of {TOTAL_CUSTOMERS} customers — "
            f"requests issued: {_StripeHandler.requests_seen}"
        )
        assert {r["id"] for r in rows} == {c["id"] for c in _StripeHandler.customers}

    def test_the_oldest_customers_are_the_ones_that_go_missing(
        self, stripe_api, local_egress, redirect_to
    ):
        """Names the production symptom, so a regression reads as the incident.

        Product found the four *oldest* customers absent; page one carries the
        newest. Asserting on a row that cannot appear on page one is what makes
        this fail on exactly the bug being prevented.
        """
        redirect_to(stripe_api)
        ids = {r["id"] for r in _stripe_rows()}

        beyond_page_one = {c["id"] for c in _StripeHandler.customers[PAGE_SIZE:]}
        assert beyond_page_one <= ids, (
            f"never advanced past page one; missing {beyond_page_one - ids}"
        )

    def test_the_walk_terminates(self, stripe_api, local_egress, redirect_to):
        """Following a cursor forever is the other way to get this wrong.

        Stripe's ``has_more`` is *not* the stop condition here and cannot be:
        ``rest_api_source``'s config schema rejects ``has_more_path``, even
        though ``create_paginator`` accepts it. So the walk ends when a page
        comes back empty and ``data.[-1].id`` resolves to nothing — three
        requests for fifteen rows at a page size of ten, the third empty.

        Asserting the exact count is the point: a paginator that never
        terminated would be cut off by ``MAX_REQUESTS`` and surface as a 508
        rather than as a hung suite.
        """
        redirect_to(stripe_api)
        _stripe_rows()

        assert len(_StripeHandler.requests_seen) == 3, _StripeHandler.requests_seen


def test_control_without_a_paginator_stops_at_page_one(
    stripe_api, local_egress, redirect_to, monkeypatch
):
    """Negative control — proves the tests above discriminate.

    With the paginator table emptied, the code takes exactly the pre-fix path
    and dlt's auto-detection is all that is left. If this *passed* with 15 rows,
    the server would be handing everything over on page one and every assertion
    above would be vacuous.

    It also records what auto-detection actually does with Stripe's response
    shape: ``has_more``/``starting_after`` is not a scheme it recognises, so it
    settles on ``SinglePagePaginator`` — one page, no error, run green.
    """
    redirect_to(stripe_api)
    monkeypatch.setattr(dlt_runner, "SAAS_PAGINATORS", {})
    rows = _stripe_rows()

    assert len(rows) == PAGE_SIZE, (
        f"expected the unpaginated bug ({PAGE_SIZE} rows), got {len(rows)} — "
        "the fixture no longer reproduces core#823 and the suite above proves nothing"
    )


class TestDltConfigPaginatorIsHonoured:
    """``dlt_config["paginator"]`` was accepted and discarded (core#823).

    It is in ``INTERNAL_CONFIG_KEYS``, so the **Use raw JSON config** escape
    hatch accepted one and the run went green having changed nothing — the
    aggravating shape of core#532, where here it is the *repair* that is
    silently dropped.
    """

    def test_an_override_changes_which_rows_arrive(self, stripe_api, local_egress, redirect_to):
        """Behavioural on purpose.

        Asserting the key reaches the builder would be satisfied by a builder
        that reads it and ignores it. Forcing ``single_page`` must visibly cost
        rows.
        """
        redirect_to(stripe_api)
        rows = _stripe_rows({"paginator": {"type": "single_page"}})

        assert len(rows) == PAGE_SIZE, f"override ignored — got {len(rows)} rows"

    def test_an_override_can_repair_a_connector_we_got_wrong(
        self, stripe_api, local_egress, redirect_to, monkeypatch
    ):
        """The escape hatch has to work where our own default is missing.

        This is the case the issue calls out: a user hits a vendor scheme we did
        not anticipate and repairs it from the UI. With the table emptied, only
        the user's paginator can reach 15 rows.
        """
        redirect_to(stripe_api)
        monkeypatch.setattr(dlt_runner, "SAAS_PAGINATORS", {})
        rows = _stripe_rows(
            {
                "paginator": {
                    "type": "cursor",
                    "cursor_path": "data.[-1].id",
                    "cursor_param": "starting_after",
                }
            }
        )

        assert len(rows) == TOTAL_CUSTOMERS, f"user paginator ignored — got {len(rows)} rows"


class TestABrokenPaginatorConfigIsRejected:
    """A wrong paginator must fail, not quietly become the original bug.

    ``dlt.sources.rest_api.config_setup.create_paginator`` returns ``None`` for
    a dict carrying no ``"type"`` key. dlt then falls back to auto-detection, so
    a typo'd override reproduces core#823 while the user believes it fixed.
    """

    @pytest.mark.parametrize(
        "bad,because",
        [
            ({"cursor_path": "data.[-1].id"}, "no 'type' key — dlt returns None and auto-detects"),
            ({"type": "not_a_real_paginator"}, "unknown type"),
            ({"type": "offset"}, "'offset' requires a limit"),
        ],
    )
    def test_it_raises_instead_of_silently_falling_back(self, bad, because, local_egress):
        with pytest.raises(DltRunnerError) as exc:
            DltRunnerService()._build_saas_source(
                "stripe", {"api_key": "sk_test_x"}, {"paginator": bad}
            )
        assert "paginator" in str(exc.value).lower(), because

    def test_a_valid_override_is_not_rejected(self, local_egress):
        """Negative control for the rejection: the guard must not refuse good input."""
        source = DltRunnerService()._build_saas_source(
            "stripe", {"api_key": "sk_test_x"}, {"paginator": {"type": "single_page"}}
        )
        assert source is not None


class TestEverySaasConnectorHasAPaginatorDecision:
    """Derived from the source, so a new connector cannot skip the decision.

    The original defect was not a wrong paginator — it was fourteen connectors
    for which nobody had asked the question. A hand-written list of the
    connectors we happen to remember would let the fifteenth repeat it.
    """

    def test_no_rest_fallback_connector_is_undecided(self):
        undecided = sorted(
            dlt_runner.REST_FALLBACK_SAAS_TYPES
            - set(dlt_runner.SAAS_PAGINATORS)
            - set(dlt_runner.SAAS_PAGINATION_EXEMPT)
        )
        assert not undecided, (
            f"{undecided} reach _rest_api_fallback with no paginator and no recorded "
            "reason. Add an entry to SAAS_PAGINATORS, or to SAAS_PAGINATION_EXEMPT "
            "with the measurement that justifies it."
        )

    def test_the_two_tables_do_not_overlap(self):
        both = set(dlt_runner.SAAS_PAGINATORS) & set(dlt_runner.SAAS_PAGINATION_EXEMPT)
        assert not both, f"{sorted(both)} is both configured and exempt — one of them is a lie"

    def test_every_configured_paginator_is_one_rest_api_source_accepts(self):
        """Validate against the REAL consumer — ``rest_api_source`` itself.

        🚨 This test was originally written against
        ``dlt.sources.rest_api.config_setup.create_paginator`` and **passed on a
        table production rejects.** The two dlt entry points disagree: the
        factory builds a paginator carrying ``has_more_path``, and
        ``rest_api_source``'s config schema refuses the same dict because
        ``JSONResponseCursorPaginatorConfig`` declares no such field. Every
        Stripe upload would have died on a ``DictValidationException`` — the fix
        for a silent truncation turned into a loud outage — with a green suite.

        ``rest_api_source`` builds without performing any I/O; extraction only
        happens on iteration.
        """
        from dlt.common.exceptions import DictValidationException
        from dlt.sources.rest_api import rest_api_source

        for name, spec in dlt_runner.SAAS_PAGINATORS.items():
            try:
                rest_api_source(
                    {
                        "client": {"base_url": "https://api.example.com/", "paginator": dict(spec)},
                        "resources": [{"name": "probe", "endpoint": {"path": "probe"}}],
                    }
                )
            except (DictValidationException, TypeError, ValueError) as exc:
                pytest.fail(f"SAAS_PAGINATORS[{name!r}] = {spec!r} — production refuses it: {exc}")

    def test_that_check_can_actually_reject(self):
        """Negative control for the test above.

        A validation helper that accepts everything passes every table. This is
        the config that shipped in the first draft — it must be refused.
        """
        from dlt.common.exceptions import DictValidationException
        from dlt.sources.rest_api import rest_api_source

        with pytest.raises(DictValidationException):
            rest_api_source(
                {
                    "client": {
                        "base_url": "https://api.example.com/",
                        "paginator": {
                            "type": "cursor",
                            "cursor_path": "data.[-1].id",
                            "cursor_param": "starting_after",
                            "has_more_path": "has_more",
                        },
                    },
                    "resources": [{"name": "probe", "endpoint": {"path": "probe"}}],
                }
            )

    def test_the_tables_only_name_real_connectors(self):
        known = dlt_runner.REST_FALLBACK_SAAS_TYPES
        for table in ("SAAS_PAGINATORS", "SAAS_PAGINATION_EXEMPT"):
            unknown = sorted(set(getattr(dlt_runner, table)) - known)
            assert not unknown, f"{table} names {unknown}, which reach no REST fallback"

    def test_the_connector_list_is_re_derived_from_the_source(self):
        """``REST_FALLBACK_SAAS_TYPES`` is hand-written; this proves it honest.

        Without this, the guard above is circular: a fifteenth connector added
        to ``_build_saas_source`` and *not* to the constant is absent from both
        sides of the subtraction, so ``test_no_rest_fallback_connector_is_undecided``
        reports nothing wrong. That is the shape of the original bug — a
        question nobody was asked — reproduced in the thing built to prevent it.

        Derived by walking the AST for ``connection_type == "<literal>"``
        branches whose body reaches ``_rest_api_fallback``.
        """
        import ast
        import inspect
        import textwrap

        tree = ast.parse(textwrap.dedent(inspect.getsource(DltRunnerService._build_saas_source)))

        found = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.If):
                continue
            test = node.test
            if not (
                isinstance(test, ast.Compare)
                and isinstance(test.left, ast.Name)
                and test.left.id == "connection_type"
                and len(test.ops) == 1
                and isinstance(test.ops[0], ast.Eq)
                and isinstance(test.comparators[0], ast.Constant)
            ):
                continue
            reaches_fallback = any(
                isinstance(n, ast.Call)
                and isinstance(n.func, ast.Attribute)
                and n.func.attr == "_rest_api_fallback"
                for n in ast.walk(node)
            )
            if reaches_fallback:
                found.add(test.comparators[0].value)

        assert found, "the AST walk matched nothing — this test would pass on any table"
        assert found == set(dlt_runner.REST_FALLBACK_SAAS_TYPES), (
            "REST_FALLBACK_SAAS_TYPES disagrees with _build_saas_source. "
            f"in source only: {sorted(found - set(dlt_runner.REST_FALLBACK_SAAS_TYPES))}; "
            f"in constant only: {sorted(set(dlt_runner.REST_FALLBACK_SAAS_TYPES) - found)}"
        )

    def test_forgetting_a_paginator_at_a_new_call_site_is_a_type_error(self):
        """``paginator`` is keyword-only with no default, and that is the fix.

        The original defect was reachable because ``_rest_api_fallback`` simply
        had no such parameter, so fourteen call sites silently declined to pass
        one. A default of ``None`` would restore that for connector fifteen.
        This asserts the signature keeps forgetting impossible.
        """
        import inspect

        sig = inspect.signature(DltRunnerService._rest_api_fallback)
        param = sig.parameters["paginator"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY, param.kind
        assert param.default is inspect.Parameter.empty, (
            "paginator has acquired a default — a call site that forgets it is "
            "silently back to loading page one only (core#823)"
        )

        with pytest.raises(TypeError):
            DltRunnerService._rest_api_fallback(
                "https://api.example.com/", None, [{"name": "x", "endpoint": {"path": "x"}}]
            )

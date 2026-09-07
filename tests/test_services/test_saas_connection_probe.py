"""`Test Connection` must make a request before it claims success (core#821).

**The defect.** For 20 connector types `test_connection` returned
``(True, "Test not applicable for this type")`` after making **no network call
at all**, and `True` drives the green styling. Product proved it on production
with a fabricated token (``shpat_THIS_TOKEN_IS_NOT_REAL_000000``) against a
store that does not exist: green. Two credentials sitting on disk — a Pipedrive
key returning **401** and a Freshdesk key returning **403 account_suspended** —
would both also have gone green.

The affected set is exactly the connectors whose credentials expire, get revoked
or get mistyped, and this button is the only pre-run validation the UI offers.

**The invariant, and why it is the whole test.**
``test_the_verdict_is_never_success_when_no_request_can_succeed`` cuts the
network and sweeps every non-SQL type: none may answer ``True``. That single
assertion fails 20 times against the unfixed service and cannot be satisfied by
a probe that exists but is never called — which a per-type "does it have a
probe" test would be.

**The trap this fix could have walked into.** Several of these APIs answer
**HTTP 200 with a failure in the body**. Slack returns ``{"ok": false,
"error": "invalid_auth"}`` with a 200; Pipedrive has a ``success`` flag. A probe
that checks only ``response.status_code`` reports green for a dead token — the
reported bug, rebuilt inside its own fix. ``TestVendorsThatFailWithHttp200``
is that case, with a passing control beside it so it cannot go green by
refusing everything.
"""

import json
import threading
from http import server as http_server
from unittest.mock import patch

import pytest
import requests

from datanika.models.connection import ConnectionType
from datanika.services import connection_service as cs
from datanika.services.connection_service import ConnectionService

#: Types handled by a real test long before the SaaS branch — they have their
#: own coverage and are not what core#821 is about.
_ALREADY_TESTED = {ConnectionType.MONGODB} | cs._FILE_TYPES


def _saas_types():
    return sorted(cs._NON_DB_TYPES - _ALREADY_TESTED, key=lambda t: t.value)


#: One config carrying every credential field any probe reads, so each type gets
#: past its "missing field" guard and as far as the network. Values are
#: deliberately fake — nothing here may authenticate anywhere.
_PROBE_CONFIG = {
    "api_key": "not-a-real-key",
    "access_token": "not-a-real-token",
    "api_token": "not-a-real-token",
    "bot_token": "xoxb-not-a-real-token",
    "stripe_secret_key": "sk_test_not_real",
    "instance_url": "https://example-instance.my.salesforce.com",
    "store": "no-such-store-datanika-probe",
    "domain": "no-such-domain-datanika-probe",
    "subdomain": "no-such-subdomain-datanika-probe",
    "email": "probe@example.com",
    "base_id": "appNotReal",
    "account_id": "act_000",
    "property_id": "000",
    "service_account_json": "{}",
    "customer_id": "000-000-0000",
    "base_url": "https://api.example.com/",
    "bootstrap_servers": "broker.invalid:9092",
}


class _DeadSession(requests.Session):
    """A session that cannot reach anything. Records that it was asked to try."""

    def __init__(self):
        super().__init__()
        self.attempts = []

    def request(self, method, url, **kwargs):  # noqa: A003
        self.attempts.append((method, url))
        raise requests.ConnectionError("probe: network is cut")


class TestTheReportedDefect:
    def test_the_verdict_is_never_success_when_no_request_can_succeed(self):
        """The invariant: no green without evidence.

        With the network cut, a *correct* verdict is either failure or an
        explicit "not tested" — never success. Against the unfixed service this
        fails for all twenty types at once, which is the right shape: the bug
        was never about one connector.
        """
        session = _DeadSession()
        lying = []
        with patch.object(cs, "build_guarded_session", return_value=session):
            for ct in _saas_types():
                ok, msg = ConnectionService.test_connection(dict(_PROBE_CONFIG), ct)
                if ok is True:
                    lying.append(f"{ct.value}: (True, {msg!r})")

        assert not lying, "these types report SUCCESS having verified nothing:\n  " + "\n  ".join(
            lying
        )

    def test_a_type_we_cannot_probe_is_neither_success_nor_failure(self):
        """ "Not tested" has to be its own answer.

        Rendering it as failure is a second lie — the connection may be
        perfectly good — and rendering it as success is the bug. ``None`` is
        falsy, so any caller doing ``if ok:`` degrades to "not success" rather
        than to a wrong green.
        """
        for ct in _saas_types():
            if ct.value not in cs.SAAS_PROBE_EXEMPT:
                continue
            ok, msg = ConnectionService.test_connection(dict(_PROBE_CONFIG), ct)
            assert ok is None, f"{ct.value} returned {ok!r} ({msg!r}), expected None"
            assert msg, f"{ct.value} returned an empty message"

    def test_a_probeable_type_actually_issues_a_request(self):
        """A probe that exists but is never called is the same bug with a table.

        Distinct from the sweep above: an implementation could return a
        pessimistic ``False`` for everything and satisfy that one.
        """
        session = _DeadSession()
        with patch.object(cs, "build_guarded_session", return_value=session):
            ConnectionService.test_connection(dict(_PROBE_CONFIG), ConnectionType.SHOPIFY)

        assert session.attempts, "shopify returned a verdict without attempting a request"


class _VendorHandler(http_server.BaseHTTPRequestHandler):
    """Answers with whatever the test parked in ``_VendorHandler.reply``."""

    reply: tuple[int, dict] = (200, {})

    def do_GET(self):  # noqa: N802 - stdlib callback name
        status, payload = type(self).reply
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def vendor_api(monkeypatch):
    """A local stand-in for a vendor API, with the egress guard relaxed.

    The guard blocks loopback by design and has its own tests
    (``tests/test_security/test_egress_guarded_session.py``).
    """
    srv = http_server.HTTPServer(("127.0.0.1", 0), _VendorHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    monkeypatch.setattr(cs, "validate_egress_host", lambda url: None)
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
    )
    try:
        yield f"http://127.0.0.1:{srv.server_port}"
    finally:
        srv.shutdown()
        srv.server_close()


def _probe_against(base: str, connection_type: ConnectionType, monkeypatch):
    """Run one type's probe against the local server instead of the vendor."""
    real = cs._saas_probe_url

    monkeypatch.setattr(cs, "_saas_probe_url", lambda ct, cfg: (real(ct, cfg), base)[1])
    return ConnectionService.test_connection(dict(_PROBE_CONFIG), connection_type)


class TestVendorsThatFailWithHttp200:
    """Slack answers ``200 {"ok": false}`` for a dead token (Pipedrive is similar).

    A probe reading only ``status_code`` calls that a pass — the reported bug,
    rebuilt inside its own fix. This is the reason the probe table carries a
    body predicate at all.
    """

    def test_slack_invalid_auth_is_a_failure_despite_the_200(self, vendor_api, monkeypatch):
        _VendorHandler.reply = (200, {"ok": False, "error": "invalid_auth"})
        ok, msg = _probe_against(vendor_api, ConnectionType.SLACK, monkeypatch)

        assert ok is False, f"200-with-ok:false read as success ({msg!r})"
        assert "invalid_auth" in msg, msg

    def test_slack_valid_auth_still_passes(self, vendor_api, monkeypatch):
        """Control. Without this, refusing everything would satisfy the test above."""
        _VendorHandler.reply = (200, {"ok": True, "team": "datanika", "user": "bot"})
        ok, msg = _probe_against(vendor_api, ConnectionType.SLACK, monkeypatch)

        assert ok is True, f"a healthy Slack token was rejected ({msg!r})"

    def test_pipedrive_success_false_is_a_failure_despite_the_200(self, vendor_api, monkeypatch):
        _VendorHandler.reply = (200, {"success": False, "error": "unauthorized access"})
        ok, msg = _probe_against(vendor_api, ConnectionType.PIPEDRIVE, monkeypatch)

        assert ok is False, f"200-with-success:false read as success ({msg!r})"

    def test_pipedrive_success_true_still_passes(self, vendor_api, monkeypatch):
        _VendorHandler.reply = (200, {"success": True, "data": {"id": 1}})
        ok, _ = _probe_against(vendor_api, ConnectionType.PIPEDRIVE, monkeypatch)

        assert ok is True


class TestHttpStatusIsHonoured:
    """The two dead credentials Product found on disk, as status codes."""

    @pytest.mark.parametrize(
        "status,fragment",
        [(401, "401"), (403, "403"), (404, "404"), (500, "500")],
    )
    def test_an_error_status_is_a_failure(self, vendor_api, monkeypatch, status, fragment):
        _VendorHandler.reply = (status, {"error": "nope"})
        ok, msg = _probe_against(vendor_api, ConnectionType.SHOPIFY, monkeypatch)

        assert ok is False, f"HTTP {status} read as success"
        assert fragment in msg, msg

    def test_an_auth_failure_is_named_as_one_and_a_server_error_is_not(
        self, vendor_api, monkeypatch
    ):
        """401/403 mean *your token is wrong*; 500 means *the vendor is down*.

        Telling a user their credentials were rejected during a vendor outage
        sends them off to re-issue a token that was fine — the opposite of the
        actionable verdict this whole change exists to produce.

        🔑 **This test exists because mutation testing found the branch was dead
        weight.** Removing `if response.status_code in (401, 403)` entirely left
        the suite green, since every status >= 400 already returns False and no
        assertion looked at the wording. A branch no test can distinguish from
        its neighbour is not covered, however many tests pass through it.
        """
        _VendorHandler.reply = (401, {"error": "nope"})
        _, unauthorized = _probe_against(vendor_api, ConnectionType.SHOPIFY, monkeypatch)
        assert "rejected these credentials" in unauthorized, unauthorized

        _VendorHandler.reply = (500, {"error": "boom"})
        _, server_error = _probe_against(vendor_api, ConnectionType.SHOPIFY, monkeypatch)
        assert "rejected these credentials" not in server_error, (
            f"a vendor outage was reported as a credential failure: {server_error!r}"
        )
        assert "500" in server_error, server_error

    def test_a_2xx_is_a_success(self, vendor_api, monkeypatch):
        """Control: the check above must not be refusing everything."""
        _VendorHandler.reply = (200, {"shop": {"name": "Test Store"}})
        ok, msg = _probe_against(vendor_api, ConnectionType.SHOPIFY, monkeypatch)

        assert ok is True, msg


class TestEverySaasTypeHasAProbeDecision:
    """Derived from ``_NON_DB_TYPES``, so a new connector cannot skip the question."""

    def test_no_saas_type_is_undecided(self):
        undecided = sorted(
            {t.value for t in _saas_types()} - set(cs.SAAS_PROBES) - set(cs.SAAS_PROBE_EXEMPT)
        )
        assert not undecided, (
            f"{undecided} reach Test Connection with no probe and no recorded reason. "
            "Add an entry to SAAS_PROBES, or to SAAS_PROBE_EXEMPT with the reason."
        )

    def test_the_two_tables_do_not_overlap(self):
        both = set(cs.SAAS_PROBES) & set(cs.SAAS_PROBE_EXEMPT)
        assert not both, f"{sorted(both)} is both probeable and exempt — one is a lie"

    def test_the_tables_only_name_real_types(self):
        known = {t.value for t in _saas_types()}
        for table in ("SAAS_PROBES", "SAAS_PROBE_EXEMPT"):
            unknown = sorted(set(getattr(cs, table)) - known)
            assert not unknown, f"{table} names {unknown}, which is not a non-SQL connector type"

    def test_every_exemption_carries_a_reason(self):
        for name, reason in cs.SAAS_PROBE_EXEMPT.items():
            assert len(reason) > 40, f"{name}: {reason!r} is not a reason"

    def test_every_probe_builds_a_url_from_the_probe_config(self):
        """The table is claims about other people's APIs; at least check it runs.

        Each entry must produce an absolute https URL from a config carrying the
        credential fields — a lambda raising KeyError would otherwise surface as
        a crashed event handler, which is core#608 all over again.
        """
        for name in cs.SAAS_PROBES:
            ct = ConnectionType(name)
            url = cs._saas_probe_url(ct, dict(_PROBE_CONFIG))
            assert url.startswith("https://"), f"{name}: {url!r}"


#: Config keys a probe reads WITHOUT requiring — an optional override that has a
#: default, not a credential (core#860).
#:
#: 🔑 This table is why `test_every_field_the_probe_reads_is_a_field_it_requires`
#: is not simply `read <= declared`. `facebook_ads` builds its URL from
#: `_first(c, "api_version") or _FACEBOOK_API_VERSION`, so declaring `api_version`
#: would make an optional override MANDATORY and break every Facebook connection
#: that (correctly) omits it. A guard that cannot tell those apart forces the wrong
#: fix, which is worse than no guard.
_OPTIONAL_PROBE_READS: dict[str, set[str]] = {
    "facebook_ads": {"api_version"},
}

#: Loader fields no probe group covers — core#992's territory, not core#860's.
#:
#: These are **scope selectors, not credentials**: which base, which ad account,
#: which repo. The probe deliberately hits an identity endpoint (`/whoami`, `/me`,
#: `/user`) that does not need them, so a green Test Connection genuinely does not
#: imply a runnable pipeline — but the remedy is a probe that exercises the
#: resource, which is core#992, not field parity.
#:
#: Pinned EXACTLY rather than as a floor: a new entry means a connector grew a
#: credential the probe never checks, and that IS core#860's defect. A removed
#: entry means core#992 covered one and this note should shrink with it.
_LOADER_FIELDS_NO_PROBE_COVERS: dict[str, set[str]] = {
    "airtable": {"base_id"},
    "facebook_ads": {"account_id"},
    "github": {"owner", "repo"},
    # Read by the loader but used only on the `dlt init` verified-source branch,
    # which `_build_saas_source`'s own docstring records as dead in every
    # deployment; the live REST fallback authenticates with Bearer, exactly as
    # the probe does. Listed so its absence from the probe is a recorded fact
    # rather than an oversight.
    "zendesk": {"email"},
}


def _loader_config_reads() -> dict[str, set[str]]:
    """`connection_type` -> config keys `_build_saas_source` reads for it."""
    import ast
    import inspect
    import textwrap

    from datanika.services import dlt_runner

    src = textwrap.dedent(inspect.getsource(dlt_runner.DltRunnerService._build_saas_source))
    branches: dict[str, set[str]] = {}
    for node in ast.walk(ast.parse(src)):
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if not (
            isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Name)
            and test.left.id == "connection_type"
            and test.comparators
            and isinstance(test.comparators[0], ast.Constant)
        ):
            continue
        keys = {
            n.args[0].value
            for n in ast.walk(node)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "get"
            and isinstance(n.func.value, ast.Name)
            and n.func.value.id == "config"
            and n.args
            and isinstance(n.args[0], ast.Constant)
        }
        branches.setdefault(test.comparators[0].value, set()).update(keys)
    return branches


def _probe_lambda_reads() -> dict[str, set[str]]:
    """`connection_type` -> config keys the probe's OWN lambdas read.

    `fields` is only the precondition; the request is built by `url`/`headers`/
    `params`, and those are where a probe can quietly depend on something it never
    declares. That is how `jira` came to read `email` and require nothing.
    """
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(cs))
    entry = next(
        (
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", "") == "SAAS_PROBES"
        ),
        None,
    )
    assert entry is not None, "SAAS_PROBES is no longer a module-level annotated assignment"

    out: dict[str, set[str]] = {}
    for key, value in zip(entry.keys, entry.values, strict=True):
        reads: set[str] = set()
        for inner_key, inner_value in zip(value.keys, value.values, strict=True):
            if inner_key.value == "fields":
                continue
            for n in ast.walk(inner_value):
                if (
                    isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Name)
                    and n.func.id == "_first"
                ):
                    reads |= {a.value for a in n.args[1:] if isinstance(a, ast.Constant)}
                if (
                    isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Attribute)
                    and n.func.attr == "get"
                    and n.args
                    and isinstance(n.args[0], ast.Constant)
                ):
                    reads.add(n.args[0].value)
        out[key.value] = reads
    return out


def _declared(name: str) -> set[str]:
    return {f for group in cs.SAAS_PROBES[name]["fields"] for f in group}


class TestTheProbeAndTheLoaderReadTheSameCredentials:
    """core#860 — nothing else couples `SAAS_PROBES` to `_build_saas_source`.

    They read credentials out of the same `config`, they are ~1,400 lines apart in
    different modules, and a divergence is silent in both directions: a green Test
    Connection on a connection that cannot run, or a red one on a connection that can.

    🚨 **Every assertion here is paired with an anti-vacuity check on its own AST walk.**
    A rename of `connection_type`, or a `match` statement replacing the `if` chain, makes
    every extracted set empty — at which point every difference vanishes and all three
    tests pass while measuring nothing. That is the exact shape this file's own docstring
    was written about, one level up.
    """

    def test_the_ast_walks_actually_matched_something(self):
        """The arming check. If this fails, the three below prove nothing."""
        loader = _loader_config_reads()
        probes = _probe_lambda_reads()

        # 🔴 This assertion was originally `len(loader) >= 14`, and a mutation proved it
        # BLIND: breaking a single `if connection_type == "stripe":` branch left 15 others
        # matching, so the arming check passed while one connector's field set had silently
        # become empty. A threshold over a total cannot see a partial failure — and a
        # partial failure is the likelier one, because refactors land one branch at a time.
        # The property that actually matters is per-connector, so assert it per connector.
        unseen = sorted(name for name in cs.SAAS_PROBES if not loader.get(name))
        assert not unseen, (
            f"the loader AST walk found no config reads for {unseen}. Either "
            "_build_saas_source no longer has a `connection_type == ...` branch for them, "
            "or the walk has been refactored out from under this guard. Repair it before "
            "trusting anything below — an empty set makes every difference vanish."
        )
        assert set(probes) == set(cs.SAAS_PROBES), (
            "the SAAS_PROBES AST walk and the imported table disagree on membership: "
            f"{sorted(set(probes) ^ set(cs.SAAS_PROBES))}"
        )
        assert all(probes[n] for n in probes), (
            f"these probes read no config key at all, which cannot be right: "
            f"{sorted(n for n in probes if not probes[n])}"
        )

    def test_every_probe_reads_fields_the_loader_also_reads(self):
        """A connection that tests must be a connection that can run.

        This is core#860's stated acceptance. If a probe authenticates with a field the
        loader never looks at, Test Connection is certifying a credential the pipeline
        will not use.
        """
        loader = _loader_config_reads()
        drift = {
            name: sorted(_declared(name) - loader.get(name, set()))
            for name in cs.SAAS_PROBES
            if _declared(name) - loader.get(name, set())
        }
        assert not drift, (
            f"these probes require credential fields the loader never reads: {drift}. "
            "A green Test Connection would not imply a runnable connection."
        )

    def test_every_field_the_probe_reads_is_a_field_it_requires(self):
        """A probe that reads a field it does not require gets a 401 it blames on
        something else.

        `fields` is the precondition; `url`/`headers`/`params` build the request. When
        they disagree, the probe sends a malformed credential and reports failure against
        whichever field it *did* require — telling the user the wrong thing about the
        wrong field. `jira` did exactly this with `email` (core#860).
        """
        undeclared = {}
        for name, reads in _probe_lambda_reads().items():
            gap = reads - _declared(name) - _OPTIONAL_PROBE_READS.get(name, set())
            if gap:
                undeclared[name] = sorted(gap)
        assert not undeclared, (
            f"these probes read config keys they do not declare in `fields`: {undeclared}. "
            "Either add the field (it is required) or add it to _OPTIONAL_PROBE_READS with "
            "the default it falls back to."
        )

    def test_the_optional_table_names_only_genuinely_optional_reads(self):
        """An entry here must still be READ, or the table is stale cover."""
        reads = _probe_lambda_reads()
        for name, keys in _OPTIONAL_PROBE_READS.items():
            assert name in reads, f"_OPTIONAL_PROBE_READS names {name}, which has no probe"
            stale = sorted(keys - reads[name])
            assert not stale, (
                f"_OPTIONAL_PROBE_READS[{name!r}] exempts {stale}, which the probe no longer "
                "reads. Remove the entry — an exemption outliving its reason silently widens "
                "the guard."
            )
            declared = sorted(keys & _declared(name))
            assert not declared, (
                f"{name} both requires and exempts {declared} — one of the two is wrong."
            )

    def test_the_loader_only_reads_what_the_probe_covers_or_a_recorded_exception(self):
        """The other direction: a credential the run needs that the test never exercises.

        Pinned EXACTLY, not as a floor. A NEW entry is core#860's defect arriving from the
        far side; a DISAPPEARED one means core#992 covered it and the table should shrink.
        Both must be noticed, so equality is the assertion.
        """
        loader = _loader_config_reads()
        uncovered = {
            name: sorted(loader.get(name, set()) - _declared(name))
            for name in cs.SAAS_PROBES
            if loader.get(name, set()) - _declared(name)
        }
        expected = {k: sorted(v) for k, v in _LOADER_FIELDS_NO_PROBE_COVERS.items()}
        assert uncovered == expected, (
            "the set of loader fields no probe covers has changed.\n"
            f"  now     : {uncovered}\n"
            f"  recorded: {expected}\n"
            "A NEW entry means a connector grew a credential Test Connection never checks "
            "(core#860). A REMOVED one means core#992 covered it — shrink the table and say "
            "so. Do not widen the table to make this pass without deciding which it is."
        )


class TestMissingCredentialsAreReportedNotProbed:
    def test_an_empty_config_is_a_failure_before_any_request(self):
        session = _DeadSession()
        with patch.object(cs, "build_guarded_session", return_value=session):
            ok, msg = ConnectionService.test_connection({}, ConnectionType.SHOPIFY)

        assert ok is False, msg
        assert not session.attempts, "sent a request with no credentials"

    def test_a_missing_required_field_names_the_field(self):
        session = _DeadSession()
        with patch.object(cs, "build_guarded_session", return_value=session):
            ok, msg = ConnectionService.test_connection({"api_key": "x"}, ConnectionType.SHOPIFY)

        assert ok is False
        assert "store" in msg.lower(), msg

"""Regression tests for core#608 — Test Connection must always return a verdict.

**The defect.** Clicking **Test Connection** for `bigquery` or `databricks` on
production produced Reflex's generic unhandled-exception toast — *"An error
occurred. Contact the website administrator."* — rather than any message of
ours. The handler had crashed, so `test_message` was never assigned.

`ConnectionService.test_connection` guarded `create_engine` with a single
`except ImportError`. SQLAlchemy raises **`NoSuchModuleError`** when a URL names
a dialect that is not registered, and `NoSuchModuleError` is an `ArgumentError`,
not an `ImportError` — so it escaped the service, escaped the event handler, and
reached Reflex. Everything *below* that line already had a broad
`except Exception` that degraded gracefully; the one line that built the engine
did not.

**Why the sweep, rather than two cases.** Two connector families were affected
and one of them (`bigquery`) could not be reproduced off production, so a test
written to the two known symptoms would assert less than we actually need. The
invariant is the useful thing: *`test_connection` returns `(bool, str)` for
every `ConnectionType`, whatever goes wrong underneath.* That is what was
missing, and it catches both reported types at once plus the next one.

**Why the failure is injected rather than provoked.** Reaching the real
exception needs either a missing dialect (true for `databricks` today, and a
fixed dependency list could quietly make it false) or a network round trip. Both
make the test describe the environment instead of the code. Patching
`create_engine` states the invariant directly and runs offline in milliseconds —
and `test_databricks_...` below still exercises the genuine, unpatched path.
"""

import pytest
from sqlalchemy.exc import NoSuchModuleError

from datanika.models.connection import ConnectionType
from datanika.services.connection_service import ConnectionService

#: A config that gets every type past its early returns and as far as the code
#: under test. Values are deliberately unroutable — nothing here should open a
#: socket, and `create_engine` is patched in the sweeps besides.
#:
#: 🚨 **Do not add SaaS credential fields here** (`api_key`, `access_token`,
#: `api_token`, `bot_token`, `store`, `domain`, `subdomain`, `instance_url`).
#: Since core#821 the SaaS branch issues a **real HTTP request** when the
#: credentials are present, so adding one turns this offline sweep into a unit
#: suite that calls Stripe, Slack and GitHub on every run — slow, flaky, and
#: leaking fabricated tokens to third parties. The types stop at their
#: missing-field guard precisely because these keys are absent, and
#: `test_the_probe_config_cannot_reach_a_vendor` keeps it that way.
_PROBE_CONFIG = {
    "host": "127.0.0.1",
    "port": "1",
    "user": "u",
    "password": "p",
    "database": "d",
    "project": "proj",
    "dataset": "ds",
    "account": "acct",
    "http_path": "/sql/1.0/w/x",
    "token": "t",
    "catalog": "main",
    "path": ":memory:",
}


def test_databricks_returns_a_verdict_instead_of_raising():
    """The literal reproduction from the issue — real code path, no patching.

    No SQLAlchemy dialect for Databricks ships (`dbt-databricks` pins
    `sqlalchemy<2.0` and is excluded from the dependency set), so this reaches
    `NoSuchModuleError` for real, offline, in a few milliseconds. Before the fix
    it raised; that raise is what the user saw as "Contact the website
    administrator".
    """
    ok, message = ConnectionService.test_connection(
        {
            "host": "dbc-adc397cf-f20a.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/x",
            "token": "dapi-not-a-real-token",
            "catalog": "main",
        },
        ConnectionType.DATABRICKS,
    )
    assert ok is False
    assert "databricks" in message.lower(), (
        f"the message should name the connector the user chose; got {message!r}"
    )
    assert "administrator" not in message.lower(), (
        "this is our message, not Reflex's generic unhandled-exception toast"
    )


@pytest.mark.parametrize("connection_type", list(ConnectionType), ids=lambda t: t.value)
def test_a_missing_dialect_is_a_verdict_for_every_type(connection_type, monkeypatch):
    """`NoSuchModuleError` from `create_engine` must never escape, for any type."""
    monkeypatch.setattr(
        "datanika.services.connection_service.create_engine",
        lambda *a, **kw: (_ for _ in ()).throw(
            NoSuchModuleError("Can't load plugin: sqlalchemy.dialects:whatever")
        ),
    )
    monkeypatch.setattr(
        "pymongo.MongoClient",
        lambda *a, **kw: (_ for _ in ()).throw(NoSuchModuleError("no mongo here")),
    )

    result = ConnectionService.test_connection(dict(_PROBE_CONFIG), connection_type)

    assert isinstance(result, tuple) and len(result) == 2, (
        f"{connection_type.value}: expected a (success, message) pair, got {result!r}"
    )
    ok, message = result
    assert ok is None or isinstance(ok, bool), f"{connection_type.value}: {ok!r} is not a verdict"
    assert isinstance(message, str) and message, (
        f"{connection_type.value}: message must be a non-empty string, got {message!r}"
    )


@pytest.mark.parametrize("connection_type", list(ConnectionType), ids=lambda t: t.value)
def test_an_unexpected_engine_error_is_also_a_verdict(connection_type, monkeypatch):
    """The guard must be about *escaping*, not about one exception class.

    Widening `except ImportError` to `except (ImportError, NoSuchModuleError)`
    would pass the test above and still leave the next surprise uncaught — which
    is the shape of the original bug, one exception class at a time. `bigquery`
    is the live example: its dialect *does* ship, so whatever crashed it on
    production was something else again.
    """
    monkeypatch.setattr(
        "datanika.services.connection_service.create_engine",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("something unforeseen")),
    )
    monkeypatch.setattr(
        "pymongo.MongoClient",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("something unforeseen")),
    )

    ok, message = ConnectionService.test_connection(dict(_PROBE_CONFIG), connection_type)

    assert ok is None or isinstance(ok, bool)
    assert isinstance(message, str) and message


def test_the_sweep_actually_covers_the_reported_types():
    """Anti-vacuity: the parametrisation must include both types from the issue.

    A sweep over an enum is only as good as the enum, and `bigquery`/`databricks`
    are the two the user actually hit.
    """
    values = {t.value for t in ConnectionType}
    assert {"bigquery", "databricks"} <= values


def test_the_probe_config_cannot_reach_a_vendor():
    """The offline sweeps must stay offline (core#821).

    `test_connection` now makes a real request for SaaS types once credentials
    are present. `_PROBE_CONFIG` is shared by both sweeps above and by anything
    added later, so a well-meaning `"api_key": "x"` would silently turn this
    file into a client of fourteen third-party APIs — on every CI run, with
    fabricated tokens. Nothing else would fail; the suite would just get slow
    and occasionally red.
    """
    from datanika.services import connection_service as cs

    credential_fields = set()
    for probe in cs.SAAS_PROBES.values():
        for group in probe["fields"]:
            credential_fields.update(group)

    present = sorted(credential_fields & set(_PROBE_CONFIG))
    assert not present, (
        f"_PROBE_CONFIG now carries SaaS credential field(s) {present}, so the "
        "offline sweeps in this file will issue real requests to those vendors. "
        "Remove them, or patch build_guarded_session in the sweeps."
    )

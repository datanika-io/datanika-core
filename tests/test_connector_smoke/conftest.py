"""Gate for the live-connector smoke suite.

These tests hit real third-party APIs with sandbox credentials. They
are skipped unless ``DATANIKA_CONNECTOR_SMOKE=1`` — the nightly CI
workflow sets it; regular PR CI does not, so adding these tests does
not slow the PR feedback loop.

Per-connector creds come from env vars (see individual test modules for
the vars each expects). In CI, the workflow decodes the
``QA_CONNECTOR_CREDENTIALS`` GitHub secret (base64-encoded env file)
and exports all ``*`` pairs before pytest runs. Locally:

    set -a && source secrets/qa-connectors.env && set +a
    DATANIKA_CONNECTOR_SMOKE=1 uv run pytest tests/test_connector_smoke/ -v

## Missing dependencies FAIL — they do not skip

Once the gate is on, a missing credential or an un-importable client
library is a **hard failure**, not a skip.

That is deliberate, and it is the whole point of a monitoring probe. A
probe that skips when its dependency is missing reports green while
testing nothing — strictly worse than one that fails, because the
matrix looks healthiest exactly when it has stopped watching.

Real instance (core#407): the nightly pinned ``kafka-python>=2.0``, but
2.0.x cannot import on Python 3.12 (``kafka.vendor.six.moves`` is
gone). The Kafka probe would have died at import and its
``except ImportError: pytest.skip`` would have turned a completely
broken connector into a passing nightly.

The failure this guards against is mundane and therefore likely:
someone rotates a credential and renames its var, or the base64 bundle
decodes partially. Under skip-on-missing, that connector silently drops
out of the matrix and nobody finds out until a customer does.

Running a subset locally without every credential is the one legitimate
reason to skip instead, so that is an explicit opt-in:

    DATANIKA_CONNECTOR_SMOKE=1 DATANIKA_CONNECTOR_SMOKE_LENIENT=1 pytest ...

Note the direction: lenient mode must be switched **on**. Dropping or
misspelling any env var can only make the suite stricter, never
quieter — so a typo in the workflow surfaces as a red nightly rather
than a silent one. Do not invert this.
"""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from typing import Any

import pytest

_GATE_ENV = "DATANIKA_CONNECTOR_SMOKE"
_LENIENT_ENV = "DATANIKA_CONNECTOR_SMOKE_LENIENT"


# --------------------------------------------------------------------------- #
# Awaiting-provisioning tier (core#944)
# --------------------------------------------------------------------------- #
#
# ## Why a tier exists at all
#
# A probe against a vendor account that no longer exists is not testing our
# software. It is testing whether somebody's free trial is still alive, which no
# code change can restore. Left in the gating tier it makes the job red every
# night for ever — and a permanently-red job teaches everyone to read red as
# normal, which returns us to core#827 by the opposite route. The nine probes
# that DO watch live connectors then go unwatched behind it.
#
# ## The rule that stops this being muting
#
# `strict=True`. A pinned probe that starts PASSING is an XPASS, which pytest
# reports as a failure — so the job goes red **in both directions**: red if a
# fourth account dies (an unpinned probe fails normally), red if a pinned
# account comes back (the pin is stale). A pin that can only ever be satisfied
# is the thing this file exists to refuse.
#
# ## 🚨 Tier by ACCOUNT, never by test colour — measured, not assumed
#
# The obvious pin is "the tests that are red today". That is wrong, and it was
# wrong here. On 2026-09-03, one credential, one minute, one suspended Freshdesk
# tenant:
#
#     GET /api/v2/agents/me  -> 403 account_suspended
#     GET /api/v2/tickets    -> 403 account_suspended
#     GET /api/v2/agents     -> 200, real rows
#
# `test_freshdesk_extract_load_assert` extracts `agents`, so our *strongest*
# Freshdesk probe — a full extract -> load -> assert round-trip through the real
# connector — was **green on a dead account**, sitting inside the tier we were
# about to declare trustworthy. Tickets, which is what the Freshdesk connector
# exists to move, had been 403 the whole time.
#
# Both of that account's probes are therefore pinned, and the extract probe now
# asserts account liveness before it extracts (see
# `test_wave1_connectors_extract_load.py`) so it fails for the stated reason
# rather than passing for an incidental one.
#
# ## `raises=` is the second half of the guard
#
# Without it an xfail is satisfied by ANY failure, so a genuine regression in our
# connector code hides behind a dead account. `raises` is set wherever the
# expected exception is a builtin. It is deliberately left unset for the two
# probes whose expected exception lives in a vendor package (`dlt`,
# `kafka-python`) — importing those here would break collection of this whole
# directory on any machine without the nightly's extra dependencies, which is a
# worse failure than the one it would prevent. The residual risk is covered:
# `test_asana_extract_load_assert` drives the *same* `DltRunnerService.execute()`
# path against a live account and is in the gating tier.


@dataclass(frozen=True)
class AwaitingProvisioning:
    """One probe whose vendor account is gone. See the block comment above."""

    vendor: str
    evidence: str
    measured_on: str
    tracking: str
    raises: type[BaseException] | None


_PIPEDRIVE = {
    "vendor": "Pipedrive",
    "evidence": "GET /v1/users/me -> HTTP 401 'unauthorized access'; free trial lapsed",
    "measured_on": "2026-09-03",
    "tracking": "cloud#160",
}
_FRESHDESK = {
    "vendor": "Freshdesk",
    "evidence": (
        "GET /api/v2/agents/me and /api/v2/tickets -> HTTP 403 account_suspended "
        "(while /api/v2/agents still returns 200 — which is why this account is "
        "pinned by account and not by test colour)"
    ),
    "measured_on": "2026-09-03",
    "tracking": "cloud#160",
}
_KAFKA = {
    "vendor": "Redpanda Serverless",
    "evidence": "KafkaTimeoutError: unable to bootstrap; trial org reclaimed ~2026-08-20",
    "measured_on": "2026-09-03",
    "tracking": "cloud#160",
}

#: test function name -> why it cannot run. Keyed by name because that is what
#: pytest gives us on the item, and because the static guard in
#: ``tests/test_deploy/test_nightly_smoke_tiers.py`` resolves every one of these
#: names against the real source by AST. A pin naming a test that no longer
#: exists would silently stop covering anything AND silently stop being checked
#: — a hand-maintained list coupled to nothing is the failure mode here.
AWAITING_PROVISIONING: dict[str, AwaitingProvisioning] = {
    "test_pipedrive_auth_and_current_user": AwaitingProvisioning(
        **_PIPEDRIVE, raises=AssertionError
    ),
    # dlt wraps the 401 in PipelineStepFailed — a vendor type, see the note above.
    "test_pipedrive_extract_load_assert": AwaitingProvisioning(**_PIPEDRIVE, raises=None),
    "test_freshdesk_auth_and_current_agent": AwaitingProvisioning(
        **_FRESHDESK, raises=AssertionError
    ),
    # Green until 2026-09-03; now fails on its own liveness precondition.
    "test_freshdesk_extract_load_assert": AwaitingProvisioning(**_FRESHDESK, raises=AssertionError),
    # kafka.errors.KafkaTimeoutError — a vendor type, see the note above.
    "test_kafka_auth_and_list_topics": AwaitingProvisioning(**_KAFKA, raises=None),
}


# --------------------------------------------------------------------------- #
# Unobservable-state register (core#992)
# --------------------------------------------------------------------------- #
#
# ## The defect this exists to stop recurring
#
# `AWAITING_PROVISIONING` above handles a probe that CANNOT PASS. This register
# handles the opposite and nastier case: a probe that **passes while claiming
# something it never checked**. Nobody investigates a green.
#
# `test_databricks_auth_and_list_warehouses` said, in its own docstring,
# *"Catches: ... trial expiry"*, and asserted `warehouses` non-empty with the
# message *"No SQL warehouses found - trial workspace may have expired"*. The
# trial HAD expired. The warehouse row survives the expiry, so that assertion
# could never fire for the reason it names.
#
# ## Why Databricks is not simply pinned instead
#
# A pin is `xfail(strict=True)`, so it requires the probe to FAIL. Both Databricks
# probes pass, and they pass for real reasons - the PAT authenticates and both API
# scopes answer. Pinning them would report a permanent STALE PIN, which is a false
# alarm rather than a fix. What is wrong is the CLAIM, so the claim is what this
# register governs.
#
# ## Measured, 2026-09-03 (Engineering) - 30 read-only endpoints, GET only
#
# No read-only endpoint on the workspace discriminates an INACTIVE (trial-expired)
# workspace from a live one. The control plane is entirely intact:
#
#     GET /api/2.0/sql/warehouses            -> 200, 1 warehouse, state=STOPPED
#     GET /api/2.0/sql/warehouses/{id}       -> 200, no health field either way
#     GET /api/2.1/unity-catalog/catalogs    -> 200, real catalog
#     GET /api/2.0/preview/scim/v2/Me        -> 200, entitlements still include
#                                               allow-cluster-create
#     GET /api/2.0/sql/config/warehouses     -> 200, enable_serverless_compute=true
#     GET /api/2.0/clusters/list             -> 200  |  /jobs/list, /pipelines -> 200
#
# Only the data plane is gated, and only by a write:
#
#     POST /api/2.0/sql/warehouses/{id}/start -> 400 denyReason=INACTIVE,
#                                                domain=resource-gatekeeper
#
# ⚠️ **That call must not be made from a probe.** On a *live* workspace it starts
# billable serverless compute, and agents do not authorise spend (WORKFLOW_RULES
# 7a; core#992 AC1 forbids it explicitly). So Databricks compute liveness is not
# observable within our constraints - and saying so is the fix.
#
# Two candidate signals were examined and REJECTED, because each reads identically
# on a healthy free workspace and would be a red that means nothing:
#   * `GET /api/2.0/clusters/list-zones` -> 400 "No such workerEnvironment
#     'serverless-...'" - an artifact of a serverless-only workspace, not of expiry.
#   * `GET /api/2.0/ip-access-lists` -> 404 FEATURE_DISABLED "not available in the
#     pricing tier" - a tier statement, true of any free workspace.
#
# ## The rule
#
# **A probe may not name, in its docstring, a vendor state it cannot observe.** It
# must instead say so explicitly. `tests/test_deploy/test_probe_claims.py` reads
# the real docstrings by AST and enforces both halves - the forbidden phrase must
# be absent AND the disclaimer must be present, so a docstring that merely goes
# quiet does not satisfy it.


#: Marker every registered probe's docstring must carry. A positive artifact:
#: deleting the false claim without stating the gap leaves the reader with the
#: same wrong impression and no sentence to argue with.
UNOBSERVABLE_MARKER = "Does NOT catch:"


@dataclass(frozen=True)
class Unobservable:
    """One vendor state a passing probe does not, and cannot, observe."""

    vendor: str
    #: The vendor state, in the words the docstring must use.
    state: str
    #: What was measured, and what it would take to observe the state properly.
    evidence: str
    measured_on: str
    tracking: str
    #: Phrases that must NOT appear in the probe's docstring. These are the
    #: claims that were actually made and were actually false - not a wishlist.
    forbidden_claims: tuple[str, ...]


_DATABRICKS_INACTIVE = {
    "vendor": "Databricks",
    "state": "trial expiry / compute gating",
    "evidence": (
        "POST /api/2.0/sql/warehouses/{id}/start -> 400 denyReason=INACTIVE "
        "domain=resource-gatekeeper (cloud#124, 2026-08-31), while 30 read-only "
        "endpoints all answer 200 as on a live workspace - warehouses, UC catalogs, "
        "SCIM entitlements, clusters, jobs, sql/config. Observing the gate needs the "
        "start call, which on a live workspace is billable compute, so it is not "
        "available to a probe"
    ),
    "measured_on": "2026-09-03",
    "tracking": "cloud#124",
    "forbidden_claims": ("trial expiry", "may have expired", "trial workspace"),
}

UNOBSERVABLE_STATES: dict[str, Unobservable] = {
    "test_databricks_auth_and_list_warehouses": Unobservable(**_DATABRICKS_INACTIVE),
    "test_databricks_auth_and_list_uc_catalogs": Unobservable(**_DATABRICKS_INACTIVE),
    "test_bigquery_auth_and_list_datasets": Unobservable(
        vendor="BigQuery",
        state="billing disabled on the project",
        evidence=(
            "production run 9 failed with 403 'Billing has not been enabled for this "
            "project. DML queries are not allowed in the free tier' (cloud#124), while "
            "list_datasets on the same project returns 200. Metadata reads are free; "
            "only DML is billed, so no metadata call can see the difference"
        ),
        measured_on="2026-09-03",
        tracking="cloud#124",
        forbidden_claims=(),
    ),
}


def _awaiting_provisioning_reason(pin: AwaitingProvisioning) -> str:
    return (
        f"{pin.vendor} account is not provisioned: {pin.evidence} "
        f"(measured {pin.measured_on}, tracked in {pin.tracking}). "
        f"This probe is in the AWAITING-PROVISIONING tier — it is no longer testing our "
        f"software, so it does not fail the nightly. strict=True: if it PASSES, the "
        f"account is back and this pin is STALE — delete its entry from "
        f"AWAITING_PROVISIONING in tests/test_connector_smoke/conftest.py. Do not "
        f"re-pin it to make the job green."
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip the whole directory unless the gate env var is set, then apply tiers.

    Done at collection time so the skip reason is visible in `pytest --collect-only`
    and doesn't count toward "slow test" budget in normal PR runs.

    This gate is the one skip that stays a skip: PR CI legitimately has no
    sandbox credentials. The nightly workflow guards the corresponding risk
    — the gate silently being off there — by failing the job if *any* test
    reports skipped. See `.github/workflows/nightly-connector-smoke.yml`.

    When the gate IS on, every probe named in ``AWAITING_PROVISIONING`` is marked
    ``xfail(strict=True)``. A skip and an xfail are deliberately different things
    here: a skip counts toward nothing and trips the workflow's skip alarm, while
    an xfail is a recorded, named, reversible statement that we know exactly why
    this probe cannot pass and will be told the moment that stops being true.
    """
    if os.environ.get(_GATE_ENV) != "1":
        skip_marker = pytest.mark.skip(
            reason=f"Live connector smoke tests skipped. Set {_GATE_ENV}=1 to enable."
        )
        for item in items:
            if "test_connector_smoke" in str(item.fspath):
                item.add_marker(skip_marker)
        return

    for item in items:
        if "test_connector_smoke" not in str(item.fspath):
            continue
        pin = AWAITING_PROVISIONING.get(item.name)
        if pin is None:
            continue
        item.add_marker(
            pytest.mark.xfail(
                reason=_awaiting_provisioning_reason(pin),
                strict=True,
                raises=pin.raises,
            )
        )


def _test_name(nodeid: str) -> str:
    """`tests/x.py::test_foo[param]` -> `test_foo`."""
    return nodeid.rsplit("::", 1)[-1].split("[", 1)[0]


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:  # noqa: ARG001
    """Emit one machine-readable tier line, derived from the pin itself.

    🚨 **The workflow used to parse pytest's English summary line for this, and
    that could not work.** Measured 2026-09-03 on pytest 9.0.2: a strict XPASS is
    reported as ``failed`` and the summary line contains **no ``xpassed`` token at
    all** (``2 failed, 1 passed, 1 xfailed``). A grep for ``N xpassed`` therefore
    has exactly one possible answer — the stale-pin alarm would have been a check
    that could never fire, inside the guard written to prevent precisely that.

    The reliable discriminators, read off the reporter rather than the prose:

    ==============================  ==================================================
    correct xfail (account dead)    ``stats['xfailed']``, ``outcome='skipped'``, ``wasxfail`` set
    stale pin (account alive)       ``stats['failed']``, ``longrepr`` starts ``[XPASS(strict)]``
    wrong exception behind the pin  ``stats['failed']``, ``longrepr`` does **not**
    deselected                      ``stats['deselected']`` — holds items, not reports
    ==============================  ==================================================

    Emitting a **positive artifact** matters as much as the counts: the workflow
    fails when this line is *absent*, so a run in which the gate was off, the
    conftest was not loaded, or this hook stopped firing cannot be mistaken for a
    clean one. An absence is never evidence that nothing was wrong.
    """
    if os.environ.get(_GATE_ENV) != "1":
        return

    stats = terminalreporter.stats

    def _calls(key: str) -> list:
        return [r for r in stats.get(key, []) if getattr(r, "when", "call") == "call"]

    xfailed = _calls("xfailed")
    failed = _calls("failed")
    passed = _calls("passed")
    skipped = _calls("skipped")
    deselected = list(stats.get("deselected", []))

    pinned = set(AWAITING_PROVISIONING)
    ran = {_test_name(getattr(r, "nodeid", "")) for r in (*xfailed, *failed, *passed, *skipped)}

    stale, unexpected = [], []
    for rep in failed:
        name = _test_name(getattr(rep, "nodeid", ""))
        if name not in pinned:
            continue
        if str(getattr(rep, "longrepr", "")).startswith("[XPASS(strict)]"):
            stale.append(name)
        else:
            unexpected.append(name)

    pinned_xfailed = [n for r in xfailed if (n := _test_name(getattr(r, "nodeid", ""))) in pinned]
    not_run = sorted(pinned - ran)

    w = terminalreporter.write_line
    w(
        "[tier] "
        f"pinned={len(pinned)} "
        f"xfailed={len(pinned_xfailed)} "
        f"stale_pins={len(stale)} "
        f"unexpected_failure={len(unexpected)} "
        f"not_run={len(not_run)} "
        f"gating_passed={len(passed)} "
        f"gating_failed={len(failed) - len(stale) - len(unexpected)} "
        f"deselected={len(deselected)} "
        f"skipped={len(skipped)}"
    )
    for name in stale:
        pin = AWAITING_PROVISIONING[name]
        w(
            f"[tier] STALE PIN: {name} PASSED while pinned as awaiting-provisioning. "
            f"The {pin.vendor} account is alive again — delete its entry from "
            f"AWAITING_PROVISIONING and close the matching item on {pin.tracking}."
        )
    for name in unexpected:
        pin = AWAITING_PROVISIONING[name]
        w(
            f"[tier] UNEXPECTED FAILURE behind a pin: {name} failed with something other "
            f"than the {pin.vendor} outage this pin expects ({pin.raises}). A regression in "
            f"our own code can hide behind a dead account — this is why raises= is set."
        )
    for name in not_run:
        w(
            f"[tier] PIN COVERS NOTHING: {name} is pinned but did not run. It was "
            f"deselected or renamed; a pin naming a test that no longer executes stops "
            f"covering anything AND stops being checked."
        )


def _missing_dependency(reason: str) -> None:
    """Fail on a missing dependency — or skip, if lenient mode is on.

    See the module docstring for why failing is the default. Never call
    ``pytest.skip`` directly from a probe on a missing dependency: that
    is precisely the silent-green bug this helper exists to prevent.
    """
    if os.environ.get(_LENIENT_ENV) == "1":
        pytest.skip(f"{reason} [lenient mode]")
    pytest.fail(
        f"{reason}\n\n"
        f"Failing rather than skipping is intentional: a smoke probe that skips "
        f"on a missing dependency reports green while testing nothing, so the "
        f"matrix looks healthy exactly when it has stopped watching.\n"
        f"If this is a deliberate partial run on a machine without every "
        f"credential, set {_LENIENT_ENV}=1 to downgrade this to a skip."
    )


def _require_env(*names: str) -> dict[str, str]:
    """Fetch required env vars, or fail loudly if any are absent."""
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        _missing_dependency(f"Missing env vars: {', '.join(missing)}")
    return {n: os.environ[n] for n in names}


def _require_import(module: str, attr: str | None = None) -> Any:
    """Import a connector client library, or fail loudly if it is absent.

    Use this instead of ``try: import x / except ImportError: pytest.skip``.
    An un-importable client library means the probe *cannot run*, which is
    a broken probe — not an absent one, and not something to pass over.
    """
    try:
        mod = importlib.import_module(module)
    except ImportError as exc:
        _missing_dependency(f"Cannot import {module!r} ({exc}). Is it in the nightly's deps?")
    return getattr(mod, attr) if attr else mod


@pytest.fixture
def require_env():
    """Fixture wrapper so tests read `env = require_env('FOO', 'BAR')`."""
    return _require_env


@pytest.fixture
def require_import():
    """Fixture wrapper so tests read `Client = require_import('mod', 'Client')`."""
    return _require_import

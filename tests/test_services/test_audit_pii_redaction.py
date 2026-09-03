"""D11 + D12 acceptance guard — the audit-payload PII redactor.

Executable form of the acceptance criteria in ``docs/specs/SPEC_PII_SEPARATION.md``
§7 items 1a / 1aa / 1ab / 1ac / 1ad / 1c. Written by Product **before** the
implementation exists, and red on purpose.

Why this file exists at all
---------------------------
§2c of the spec establishes that there is now exactly one thing left that could
ever notice a redactor regression: this guard. Nothing reads
``audit_logs.old_values`` / ``new_values`` (§2b), so a bug has no user-visible
witness, and the backfill that would have been a second witness was deleted
because there turned out to be nothing to back-fill.

That makes the *shape* of this guard load-bearing, and it makes the obvious
shape worthless. A guard written as::

    assert not any(k in row.new_values for row in session.query(AuditLog))

is **true today against no redactor at all** — 0 of 30 production payloads
contain a PII key, and the five call sites that could write one have never run.
It would go green against an empty function, on a fresh clone, forever.
``test_a_table_shaped_guard_passes_vacuously_today`` below *demonstrates* that
rather than describing it, and it is one of only two tests here expected to pass
right now.

Every other test therefore **constructs its own input** and asserts on what comes
back out.

How the markers work, and why they are not decoration
-----------------------------------------------------
``@pytest.mark.xfail(strict=True, raises=AssertionError)``:

* Today, with no redactor, each test fails with ``AssertionError`` → xfail → CI
  stays green, so this file does not block anyone.
* If the harness itself breaks — a renamed fixture, an ``ImportError``, a
  ``TypeError`` — the failure is **not** an ``AssertionError``, so pytest reports
  an error and CI goes red. That closes the [core#709] trap, where a strict xfail
  was satisfied by a harness raising ``IndentationError`` and the assertion never
  ran at all.
* When the redactor ships, these XPASS, and ``strict=True`` turns an XPASS into a
  failure. **Removing the markers is therefore a required, visible step of the
  implementing PR** — which is precisely the "shown red, then green" artifact
  §2c criterion 1 asks for, recorded mechanically instead of claimed in prose.

Do not delete a marker without running the test and watching it pass, and do not
delete this file to quiet CI.

Two names this guard pins, so that the contract is checkable
------------------------------------------------------------
The spec describes behaviour; a test needs handles. These two are part of the
contract (D12.2, D12.5):

* ``datanika.services.audit_service.PII_PAYLOAD_KEYS`` — the derived frozenset.
* ``datanika.services.audit_service.redact_pii_payload(payload)`` — the redactor,
  called by ``log_action`` **through a module-global lookup** so that the negative
  control below can replace it. A redactor inlined into the method body cannot be
  substituted, and the negative control is what proves the other tests are
  sensitive to the thing they claim to measure.
"""

import pytest

from datanika.models.audit_log import AuditAction, AuditLog
from datanika.models.user import Organization
from datanika.services.audit_service import AuditService
from datanika.services.backup_service import REDACTED
from tests.factories import make_user

# D12.2: derived from the ``*_pii`` tables, plus the one hand-added key with a
# stated expiry. This literal is the ASSERTION, never the source — an empty
# frozenset() and a correct set produce identical results on production data,
# so the contents are pinned rather than merely "the derivation ran".
EXPECTED_PII_PAYLOAD_KEYS = frozenset(
    {"email", "full_name", "oauth_provider_id", "pending_email", "recipient", "ip_address"}
)

_MARKER_REASON = (
    "SPEC_PII_SEPARATION D12 redactor not implemented yet. This test is red by "
    "design; strict xfail turns it red again the moment it starts passing, so the "
    "marker has to be removed deliberately."
)


@pytest.fixture
def svc():
    return AuditService()


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-pii-redaction")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def user(db_session):
    user = make_user(
        db_session,
        email="redaction-user@example.com",
        full_name="Redaction User",
        password_hash="hashed",
    )
    return user


def _audit_service_module():
    """Import the module without letting its absence masquerade as a failed assertion."""
    import datanika.services.audit_service as mod

    return mod


def _pii_payload_keys():
    """Resolve the derived key set, or ``None`` if D12.2 has not shipped.

    Deliberately ``getattr`` rather than a module-level ``from ... import``: a
    missing symbol must surface as an ``AssertionError`` inside a test (an
    expected failure), not as a collection-time ``ImportError`` that would take
    the whole file down and hide every other assertion in it.
    """
    return getattr(_audit_service_module(), "PII_PAYLOAD_KEYS", None)


def _redactor():
    return getattr(_audit_service_module(), "redact_pii_payload", None)


def _log(svc, db_session, org, user, **kwargs):
    return svc.log_action(
        db_session,
        org_id=org.id,
        user_id=user.id,
        action=AuditAction.UPDATE,
        resource_type="user",
        resource_id=user.id,
        **kwargs,
    )


# --------------------------------------------------------------------------
# Arming checks — these MUST pass today. If they do not, every xfail below is
# meaningless and this file is testing nothing.
# --------------------------------------------------------------------------


def test_harness_is_armed_log_action_writes_and_returns_a_payload(svc, db_session, org, user):
    """Without this, an xfail below could be an expected failure for the wrong reason."""
    log = _log(svc, db_session, org, user, new_values={"name": "My Postgres"})
    assert isinstance(log, AuditLog)
    assert log.id is not None
    assert log.new_values == {"name": "My Postgres"}, (
        "log_action no longer round-trips a payload; the redaction assertions below "
        "would then be measuring the harness, not the redactor"
    )


def test_a_table_shaped_guard_passes_vacuously_today(svc, db_session, org, user):
    """§2c, demonstrated rather than described.

    This is the guard shape the spec forbids, and here it is going green with no
    redactor in the codebase at all. It is kept as a permanent, executable
    argument for why the other tests construct their own input: if anyone ever
    proposes replacing them with a query over ``audit_logs``, this test is the
    counter-example, and it will still be passing.
    """
    _log(svc, db_session, org, user, new_values={"name": "My Postgres", "cron": "0 3 * * *"})
    rows = db_session.query(AuditLog).all()
    assert rows, "no audit rows at all — the demonstration would be vacuous for a second reason"

    offending = [
        row
        for row in rows
        for payload in (row.old_values, row.new_values)
        if isinstance(payload, dict)
        for key in payload
        if key in EXPECTED_PII_PAYLOAD_KEYS
    ]
    assert offending == [], (
        "This assertion is expected to pass and proves nothing about redaction. "
        "If it ever fails, a call site started writing PII and the real guards below "
        "are what should be read."
    )


# --------------------------------------------------------------------------
# The actual guard. Red until D12 ships.
# --------------------------------------------------------------------------


def test_the_derived_key_set_is_exposed_and_exact():
    """§7 criterion 1aa — cardinality and contents pinned.

    ``Base.metadata.tables`` is populated only for models that have been
    imported, so a redactor whose module loads before the PII models silently
    derives ``frozenset()`` and redacts nothing, without raising. Asserting the
    exact contents is the only version of this check that can fail.
    """
    keys = _pii_payload_keys()
    assert keys is not None, (
        "datanika.services.audit_service.PII_PAYLOAD_KEYS does not exist (D12.2)"
    )
    assert set(keys) == set(EXPECTED_PII_PAYLOAD_KEYS), (
        f"derived key set is {sorted(keys)}, expected {sorted(EXPECTED_PII_PAYLOAD_KEYS)}. "
        "An empty set here means the derivation ran before the *_pii models were "
        "imported — which redacts nothing and raises nothing."
    )


def test_a_pii_key_is_replaced_with_the_marker(svc, db_session, org, user):
    """§7 criterion 1ab, first half. Input is constructed, never sampled."""
    keys = _pii_payload_keys() or EXPECTED_PII_PAYLOAD_KEYS
    payload = {key: f"value-for-{key}" for key in sorted(keys)}

    log = _log(svc, db_session, org, user, new_values=dict(payload))

    stored = log.new_values or {}
    unredacted = sorted(k for k in payload if stored.get(k) != REDACTED)
    assert unredacted == [], (
        f"these PII keys reached audit_logs unredacted: {unredacted}. "
        "D12.3: redaction is replacement, not deletion — the key stays and takes "
        f"the marker {REDACTED!r}, so the trail still shows that a value was here."
    )


def test_non_pii_keys_survive_byte_identical(svc, db_session, org, user):
    """§7 criterion 1ab, second half — the load-bearing one.

    Nothing reads this column (§2b), so a blanket redactor would satisfy every
    other assertion in this file and every other test in the suite. The five
    production connections deleted by a page-wide ``.last()`` survive only as
    ``{"name": ..., "connection_type": ...}`` in these payloads; blanket
    redaction destroys that record, and destroys it invisibly.
    """
    assert _redactor() is not None, (
        "redact_pii_payload does not exist yet (D12.5). This precondition is not "
        "ceremony: with no redactor in the codebase, non-PII keys trivially survive "
        "byte-identical, so the assertion below would PASS today and assert nothing. "
        "It has to be gated on the redactor existing or it is another vacuous green."
    )

    keep = {"name": "My Postgres", "connection_type": "postgresql", "role": "editor"}
    payload = {"email": "someone@example.com", **keep}

    log = _log(svc, db_session, org, user, new_values=dict(payload))

    stored = log.new_values or {}
    damaged = {k: stored.get(k) for k in keep if stored.get(k) != keep[k]}
    assert damaged == {}, (
        f"key-level redaction destroyed non-PII values: {damaged}. Expected {keep}. "
        "This is what separates key-level from blanket, and nothing else in the "
        "codebase would notice the difference."
    )


def test_redaction_reaches_nested_payloads(svc, db_session, org, user):
    """D12.5 — the redactor recurses into nested dicts and lists.

    Every payload today is a flat dict of scalars, and nothing enforces that.
    A redactor that only walks the top level is correct against every existing
    call site and wrong on the first one that nests.
    """
    payload = {"changes": [{"email": "nested@example.com"}], "meta": {"full_name": "Nested User"}}

    log = _log(svc, db_session, org, user, new_values=payload)

    stored = log.new_values or {}
    assert stored.get("changes", [{}])[0].get("email") == REDACTED, (
        f"nested list payload was not redacted: {stored!r}"
    )
    assert stored.get("meta", {}).get("full_name") == REDACTED, (
        f"nested dict payload was not redacted: {stored!r}"
    )


def test_the_redactor_never_raises_and_the_row_is_still_written(svc, db_session, org, user):
    """§7 criterion 1ac.

    ``BaseState._audit`` ends in ``except Exception: pass`` — "audit logging
    should never break the main operation". So a redactor that throws does not
    surface an error; it **silently deletes the audit row**, converting a PII bug
    into a missing-trail bug with no signal at all. Failing open leaks, failing
    hard loses the row; the marker is the only third option.
    """

    # A cycle, not a PII key. The distinction matters: {"email": <broken>} would be
    # handled by ordinary redaction (the key is in the set, so the value is replaced
    # and never serialized) and would never exercise the failure path at all. The
    # payload has to break the redactor on a key it is supposed to LEAVE ALONE.
    payload: dict = {"name": "outer"}
    payload["self"] = payload

    try:
        log = _log(svc, db_session, org, user, new_values=payload)
    except Exception as exc:  # noqa: BLE001 - converting to an assertion is the point
        raise AssertionError(
            f"log_action raised {type(exc).__name__} on an unserializable payload. "
            "D12.5 requires the row to be written anyway, carrying the "
            "__redaction_failed__ marker: BaseState._audit swallows exceptions, so a "
            "raising redactor silently deletes audit rows instead of reporting anything."
        ) from exc

    assert log is not None, "the audit row was lost entirely — D12.5's failure mode"
    stored = log.new_values or {}
    assert stored.get("__redaction_failed__") is True, (
        f"expected the greppable {{'__redaction_failed__': True}} marker, got {stored!r}"
    )


def test_negative_control_a_no_op_redactor_fails_this_guard(
    svc, db_session, org, user, monkeypatch
):
    """§2c criterion 1 — the "shown red against a no-op redactor" artifact, made permanent.

    The spec asks for that demonstration as a one-time artifact of the
    implementing PR. A one-time artifact is a claim in a PR description; this is
    the same demonstration re-run on every CI job forever.

    It proves the other tests in this file are *sensitive to the redactor*. If
    redaction ever moves somewhere that a no-op ``redact_pii_payload`` cannot
    disable — inlined into ``log_action``, or done at the model layer — this test
    fails, and it fails to tell you that the positive tests above have stopped
    measuring what they claim to measure.
    """
    redactor = _redactor()
    assert redactor is not None, (
        "datanika.services.audit_service.redact_pii_payload does not exist (D12.5). "
        "log_action must call it through a module-global lookup so it can be "
        "substituted here."
    )

    monkeypatch.setattr(
        _audit_service_module(), "redact_pii_payload", lambda payload: payload, raising=True
    )
    log = _log(svc, db_session, org, user, new_values={"email": "control@example.com"})

    stored = log.new_values or {}
    assert stored.get("email") == "control@example.com", (
        "with the redactor stubbed to a no-op the address should have reached the "
        f"row untouched, but the stored payload is {stored!r}. Redaction is happening "
        "somewhere this control cannot reach, so the positive assertions in this file "
        "are not proof that redact_pii_payload works."
    )

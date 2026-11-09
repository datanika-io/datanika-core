"""The links between the places that independently decide "this key is a credential".

There are **four** such places, not three:

1. ``connection_schemas.CONFIG_SCHEMAS`` — per-connector, marks a field with
   ``"format": "password"``. This is the one a connector author edits.
2. ``connection_service.SECRET_CONFIG_KEYS`` — what gets stripped out of a
   driver exception before it is shown to a user.
3. ``backup_service.SENSITIVE_KEYS`` — what gets redacted out of an export.
4. 🆕 **the connection form's own ``secret=True`` marker** — what renders as a password
   input. This is the *other* thing a connector author edits, and it is the one with no
   link (core#1603 AC3).

(2) and (3) were separate literals, and drifted: (3) held 4 keys against (2)'s
12, and (2) itself was missing 5 keys that (1) already marked sensitive. Both
gaps are silent — a key absent from a redaction set produces no error, just a
credential in a place it should not be.

So the tests below assert the *links*, not the contents of any one list. A new
connector that adds a sensitive field under a new name turns this file red, and
the fix is to add the key to the canonical set. Asserting on a list's contents
would have caught none of it, which is exactly why it sat.

Why (4) needed its own link, stated precisely
---------------------------------------------

``SECRET_CONFIG_KEYS``'s own comment says *"only ONE direction is asserted (schema field →
this set)"*. **Every** other guard in this repository starts from ``CONFIG_SCHEMAS`` —
this file's first section, ``test_run_text_redaction_covers_every_connector.py``'s sweep,
and that file's ``_UNDECLARED_FORM_KEYS`` ratchet. So a connector author who adds a
credential field, marks it ``secret=True`` in the form, declares it in the schema and
**forgets ``format: password``** moves nothing red anywhere:

* the schema→set link never sees it (no flag),
* the ``_UNDECLARED_FORM_KEYS`` ratchet never sees it (it *is* declared),
* and the run-text sweep classifies it as an *ordinary* field, whose control **asserts
  that it survives redaction**.

The form knew it was a credential. Nothing asked the form.

⚠️ **The invariant holds today — measured, 0 uncovered keys — so this section is green on
arrival.** That is the point of writing it now rather than after an incident, and it is
also the reason every assertion below was seen failing on a planted mutation: a guard that
has only ever been green on correct code has never been observed working.

🔑 **The assertion is behavioural, not set membership.** There are three routes by which a
key can be covered — the flag, ``SECRET_CONFIG_KEYS``, and ``SECRET_CONTAINER_KEYS`` (which
covers ``auth``, whose inner key names the product does not choose) — and a test that
enumerated the routes would go red the day a fourth is added correctly. So the sentinel is
planted where the form writes it and the **real redactors** are asked whether it survives.

🔴 **My own first instrument consulted only ``SECRET_CONFIG_KEYS`` and reported ``auth`` as
uncovered.** It is covered, by ``SECRET_CONTAINER_KEYS``, whose docstring had already
reasoned through the exact ``http_basic``-stores-a-token-under-``username`` case. A negative
reading from an instrument that does not consult every mechanism is **void, not a finding** —
and this one was one sentence away from a second false leak premise on a public issue.
"""

from __future__ import annotations

import ast
import base64
import json
import pathlib
import re
from urllib.parse import quote, quote_plus

import pytest

from datanika.services import audit_service as audit_service_module
from datanika.services.backup_service import SENSITIVE_KEYS, BackupService
from datanika.services.connection_schemas import CONFIG_SCHEMAS
from datanika.services.connection_service import (
    RUN_TEXT_WITHHELD,
    SECRET_CONFIG_KEYS,
    redact_run_text,
)
from datanika.ui.state.connection_state import _fill_openapi_auth


def _schema_password_fields() -> dict[str, set[str]]:
    """{config key -> {connection types that mark it sensitive}}."""
    found: dict[str, set[str]] = {}
    for conn_type, schema in CONFIG_SCHEMAS.items():
        for key, prop in schema.get("properties", {}).items():
            if isinstance(prop, dict) and prop.get("format") == "password":
                found.setdefault(key, set()).add(conn_type)
    return found


class TestSecretKeyCoverage:
    def test_the_probe_finds_password_fields_at_all(self):
        """Guard the guard: a broken extractor would make every test below vacuous."""
        fields = _schema_password_fields()
        assert len(fields) >= 10, f"only found {len(fields)} sensitive schema fields"
        assert "password" in fields
        assert "postgres" in fields["password"]

    def test_every_schema_password_field_is_in_the_canonical_secret_set(self):
        missing = {
            k: sorted(v)
            for k, v in _schema_password_fields().items()
            if k not in SECRET_CONFIG_KEYS
        }
        assert not missing, (
            "config keys marked `format: password` in CONFIG_SCHEMAS but absent from "
            f"connection_service.SECRET_CONFIG_KEYS — their values can reach a user-facing "
            f"error message verbatim: {missing}"
        )

    def test_backup_redaction_derives_from_the_canonical_set(self):
        assert SENSITIVE_KEYS == SECRET_CONFIG_KEYS, (
            "backup_service.SENSITIVE_KEYS must be the canonical set, not a second copy of it. "
            f"only-in-backup={sorted(set(SENSITIVE_KEYS) - set(SECRET_CONFIG_KEYS))} "
            f"only-in-canonical={sorted(set(SECRET_CONFIG_KEYS) - set(SENSITIVE_KEYS))}"
        )

    def test_canonical_set_is_immutable(self):
        """A mutable set here would let one caller's edit silently change every caller's."""
        assert isinstance(SECRET_CONFIG_KEYS, frozenset)


# =============================================================================================
# Link 4 (core#1603 AC3): the form's own credential marker -> the redaction machinery
# =============================================================================================

_UI = pathlib.Path(audit_service_module.__file__).resolve().parents[1] / "ui"
_FORM_FIELDS = _UI / "components" / "connection_config_fields.py"
_CONNECTION_STATE = _UI / "state" / "connection_state.py"

#: The factories that render a config input. ``config_text_area`` is deliberately absent: it
#: takes no ``secret`` flag, and ``secure_input.py`` says why (Chrome's password manager fills
#: ``<input type="password">`` and nothing else). Its credential-bearing uses are covered
#: below by :data:`_TEXTAREA_CREDENTIAL_FIELDS`, which exists because of that absence.
_INPUT_FACTORIES = frozenset({"config_input", "labelled_config_input"})

#: Long enough that a redactor removes it rather than withholding the whole text
#: (``_MIN_REDACTABLE_SECRET``), and carrying ``@``, ``/`` and a space **on purpose**: without
#: them ``quote_plus`` and ``quote(safe="")`` return the value unchanged, the spellings collapse
#: into one, and a redactor that had stopped covering an encoding would still read clean.
_SENTINEL = "SentinelQ7x@a/b cEnd"

#: A second, structurally identical marker for a key that is **not** a credential. Without it,
#: "the sentinel did not survive" is equally explained by a redactor that shreds everything.
_INNOCENT = "InnocentK2v@a/b cEnd"

#: Config keys whose value the form supplies through a ``secret=True`` control, where the write
#: is ``config["k"] = self.form_x`` — so the credential is the whole value.
#:
#: **Derived at read time** from two ASTs; nothing here is listed. The literal below is the
#: *ratchet* on the population, not the population.
#:
#: Read 2026-09-26: 8 controls, 11 keys, 10 direct and 1 structured.
_CREDENTIAL_FORM_FIELDS = frozenset(
    {
        "api_key",
        "aws_secret_access_key",
        "client_secret",
        "developer_token",
        "password",
        "refresh_token",
        "sasl_plain_password",
        "token",
    }
)

#: Credential keys the form writes through a **builder call** rather than as a bare value, so
#: the credential sits somewhere inside a structure whose shape only that builder knows.
#:
#: ``auth`` is the only one: ``_fill_openapi_auth`` puts the token under ``api_key``,
#: ``username`` or ``token`` depending on the detected scheme. The test drives that builder
#: rather than guessing the shape.
#:
#: 🔑 **Release condition — this is a ratchet, not a snapshot.** A new structured credential
#: write must be added here *together with* the builder this test drives, because a structure
#: whose shape nothing knows cannot be probed and would pass vacuously. A red here means
#: "somebody added a structured credential"; the fix is to teach the probe its builder, never
#: to widen the literal.
_STRUCTURED_CREDENTIAL_KEYS = frozenset({"auth"})

#: Credential-bearing ``config_text_area`` fields. A textarea carries no ``secret`` flag, so
#: these cannot be derived the way the inputs are, and the list is asserted **against the real
#: textarea call sites** so a new textarea forces a decision rather than arriving unexamined.
#:
#: Read 2026-09-26: 5 textareas — ``api_key``, ``extra_headers``, ``keyfile_json``,
#: ``openapi_spec``, ``service_account_json``. ``openapi_spec`` is a public API description and
#: ``api_key``/``extra_headers`` reach the config under other keys.
_TEXTAREA_CREDENTIAL_FIELDS = frozenset({"keyfile_json", "service_account_json"})

#: A key the form writes from an ordinary control, for the selectivity control.
_NOT_A_CREDENTIAL_KEY = "host"


def _call_name(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _first_string_arg(call: ast.Call) -> str | None:
    for arg in call.args:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return arg.value
    return None


def _secret_marked_controls() -> dict[str, str]:
    """``{ConnectionState var -> field slug}`` for every control marked ``secret=True``."""
    tree = ast.parse(_FORM_FIELDS.read_text(encoding="utf-8"))
    found: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _call_name(node) not in _INPUT_FACTORIES:
            continue
        kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
        flag = kwargs.get("secret")
        if not (isinstance(flag, ast.Constant) and flag.value is True):
            continue
        value = kwargs.get("value")
        slug = _first_string_arg(node)
        assert slug is not None, "a secret control with no field slug — the probe cannot name it"
        assert isinstance(value, ast.Attribute), (
            f"the secret control for {slug!r} does not bind a ConnectionState var through "
            "`value=`, so this probe cannot tell which config key carries its credential"
        )
        found[value.attr] = slug
    return found


def _textarea_fields() -> set[str]:
    tree = ast.parse(_FORM_FIELDS.read_text(encoding="utf-8"))
    return {
        slug
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and _call_name(node) == "config_text_area"
        and (slug := _first_string_arg(node)) is not None
    }


def _build_config_writes() -> dict[str, tuple[frozenset[str], bool]]:
    """``{config key -> (state vars its value reads, is the value wrapped in a call)}``."""
    tree = ast.parse(_CONNECTION_STATE.read_text(encoding="utf-8"))
    func = next(
        (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_build_config"),
        None,
    )
    assert func is not None, "no _build_config in connection_state.py — this probe read nothing"
    out: dict[str, tuple[frozenset[str], bool]] = {}
    for node in ast.walk(func):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not (
                isinstance(target, ast.Subscript)
                and isinstance(target.value, ast.Name)
                and target.value.id == "config"
                and isinstance(target.slice, ast.Constant)
                and isinstance(target.slice.value, str)
            ):
                continue
            key = target.slice.value
            reads = frozenset(
                n.attr
                for n in ast.walk(node.value)
                if isinstance(n, ast.Attribute)
                and isinstance(n.value, ast.Name)
                and n.value.id == "self"
            )
            wrapped = not isinstance(node.value, ast.Attribute)
            previous = out.get(key)
            if previous is None:
                out[key] = (reads, wrapped)
            else:
                out[key] = (previous[0] | reads, previous[1] or wrapped)
    return out


def _credential_config_keys() -> dict[str, bool]:
    """``{config key -> is it written through a builder call}``, for credential-fed keys."""
    secret_vars = set(_secret_marked_controls())
    return {
        key: wrapped
        for key, (reads, wrapped) in _build_config_writes().items()
        if reads & secret_vars
    }


def _spellings(value: str) -> list[str]:
    """The renderings a driver can quote, written out independently of ``_secret_spellings``.

    Importing the private helper would make this assert that the redactor agrees with itself.
    """
    encoded = base64.b64encode(value.encode()).decode("ascii")
    return [value, quote_plus(value), quote(value, safe=""), encoded, quote_plus(encoded)]


def _echo(*markers: str) -> str:
    lines = ["could not connect to the server: authentication failed"]
    lines += [f"  arg={spelling}" for m in markers for spelling in _spellings(m)]
    return "\n".join(lines)


def _readable(text: str | None, marker: str) -> bool:
    return text is not None and any(s in text for s in _spellings(marker))


def _configs_carrying_the_sentinel(key: str, wrapped: bool) -> list[dict]:
    """Every config shape the form can write for *key* with the sentinel as the credential.

    For a direct write the credential is the whole value. For a structured write the shape is
    the builder's, so the **real builder** is driven — a hand-written shape here would assert
    that the redactor agrees with my model of the builder rather than with the builder.
    """
    if not wrapped:
        return [{key: _SENTINEL, _NOT_A_CREDENTIAL_KEY: _INNOCENT}]
    assert key in _STRUCTURED_CREDENTIAL_KEYS, (
        f"config[{key!r}] is written through a call and is not in "
        "_STRUCTURED_CREDENTIAL_KEYS, so this probe does not know where inside the value the "
        "credential lands and would pass vacuously. Teach it the builder."
    )
    schemes = [
        {"type": "api_key", "name": "X-Api-Key", "location": "header"},
        {"type": "http_basic"},
        {"type": "bearer"},
    ]
    return [
        {key: _fill_openapi_auth(scheme, _SENTINEL), _NOT_A_CREDENTIAL_KEY: _INNOCENT}
        for scheme in schemes
    ]


CREDENTIAL_KEYS = sorted(_credential_config_keys())


class TestTheProbesCanSee:
    """§5b rule 1: a zero is a reading about the instrument until the instrument is proven."""

    def test_the_secret_control_scan_finds_the_known_population(self):
        controls = _secret_marked_controls()
        assert len(controls) >= 8, f"only {len(controls)} secret=True controls found"
        assert {"form_password", "form_api_key", "form_token"} <= set(controls), sorted(controls)

    def test_the_secret_control_population_is_asserted_exactly(self):
        """The ratchet. Without it, deleting a ``secret=True`` silently shrinks every test below.

        A red here is a decision to make, not a number to update: either a credential field
        gained or lost its password marker, or a new connector arrived.
        """
        slugs = frozenset(_secret_marked_controls().values())
        assert slugs == _CREDENTIAL_FORM_FIELDS, (
            f"new={sorted(slugs - _CREDENTIAL_FORM_FIELDS)} "
            f"gone={sorted(_CREDENTIAL_FORM_FIELDS - slugs)}"
        )

    def test_the_build_config_scan_finds_the_known_population(self):
        writes = _build_config_writes()
        assert len(writes) >= 55, f"the scan found only {len(writes)} keys the form writes"
        assert {"password", "api_key", "auth", _NOT_A_CREDENTIAL_KEY} <= set(writes), sorted(writes)

    def test_the_composition_finds_more_than_one_credential_key(self):
        keys = _credential_config_keys()
        assert len(keys) >= 10, f"only {len(keys)} credential-fed config keys found: {keys}"
        assert keys.get("password") is False, "password should be a direct write"
        assert keys.get("auth") is True, "auth should be a builder-call write"

    def test_the_structured_write_population_is_asserted_exactly(self):
        wrapped = frozenset(k for k, w in _credential_config_keys().items() if w)
        assert wrapped == _STRUCTURED_CREDENTIAL_KEYS, (
            f"new={sorted(wrapped - _STRUCTURED_CREDENTIAL_KEYS)} "
            f"gone={sorted(_STRUCTURED_CREDENTIAL_KEYS - wrapped)} — see the release condition "
            "on _STRUCTURED_CREDENTIAL_KEYS"
        )

    def test_the_textarea_population_is_asserted_exactly(self):
        """The residual: a textarea takes no ``secret`` flag, so it cannot be derived."""
        rendered = _textarea_fields()
        assert rendered >= _TEXTAREA_CREDENTIAL_FIELDS, sorted(
            _TEXTAREA_CREDENTIAL_FIELDS - rendered
        )
        assert len(rendered) == 5, f"the textarea population changed: {sorted(rendered)}"

    def test_a_sentinels_spellings_do_not_collapse(self):
        """Guard the guard: a tame marker collapses the spellings and blinds this section."""
        for marker in (_SENTINEL, _INNOCENT):
            raw, plus, pct, encoded, _ = _spellings(marker)
            assert len({raw, plus, pct, encoded}) == 4, (marker, raw, plus, pct)

    def test_the_search_reads_back_a_sentinel_with_nothing_held(self):
        """Control: with no config held, both markers must be readable in the echo."""
        text = redact_run_text(_echo(_SENTINEL, _INNOCENT), [])
        assert _readable(text, _SENTINEL), (
            "the probe cannot read back its own sentinel, so every 'it did not survive' "
            "reading below is VOID rather than clean"
        )
        assert _readable(text, _INNOCENT)


class TestEveryCredentialTheFormCollectsIsRedacted:
    """The link core#1603 AC3 asked for, asserted against the real redactors.

    Deliberately **not** ``key in SECRET_CONFIG_KEYS``: three declarations cover these keys
    (the schema flag, that set, and ``SECRET_CONTAINER_KEYS``), and a test that enumerated
    them would go red the day a fourth is added correctly. What must hold is that the
    credential does not reach a user-visible message or an export — so ask the code that
    decides that.
    """

    @pytest.mark.parametrize("key", CREDENTIAL_KEYS)
    def test_it_does_not_survive_into_a_runs_stored_text(self, key):
        wrapped = _credential_config_keys()[key]
        for config in _configs_carrying_the_sentinel(key, wrapped):
            text = redact_run_text(_echo(_SENTINEL, _INNOCENT), [config])
            if text == RUN_TEXT_WITHHELD:
                continue  # withholding the whole text also satisfies the property
            assert not _readable(text, _SENTINEL), (
                f"a value the connection form collects through a password-marked control and "
                f"writes to config[{key!r}] can reach a run's stored text verbatim. No "
                f"declaration covers it: not the schema's `format: password`, not "
                f"SECRET_CONFIG_KEYS, not SECRET_CONTAINER_KEYS."
            )

    @pytest.mark.parametrize("key", CREDENTIAL_KEYS)
    def test_it_does_not_survive_into_an_exported_backup(self, key):
        wrapped = _credential_config_keys()[key]
        for config in _configs_carrying_the_sentinel(key, wrapped):
            redacted = json.dumps(BackupService._redact(config))
            assert _SENTINEL not in redacted, (
                f"config[{key!r}] carries a form-collected credential into an export in clear "
                "text — and an export is the one artifact that leaves the deployment"
            )

    @pytest.mark.parametrize("key", CREDENTIAL_KEYS)
    def test_an_ordinary_field_beside_it_survives(self, key):
        """The selectivity control, on both consumers.

        Without this, a redactor that blanked everything would pass both tests above while
        making a run's diagnosis worthless and an export unimportable.
        """
        wrapped = _credential_config_keys()[key]
        for config in _configs_carrying_the_sentinel(key, wrapped):
            text = redact_run_text(_echo(_SENTINEL, _INNOCENT), [config])
            assert text != RUN_TEXT_WITHHELD, f"{key}: the whole text was withheld"
            assert "authentication failed" in text, f"{key}: the diagnosis did not survive"
            assert _readable(text, _INNOCENT), (
                f"{key}: an ordinary field's value was removed too, so 'the sentinel did not "
                "survive' proves nothing about the sentinel"
            )
            assert _INNOCENT in json.dumps(BackupService._redact(config)), (
                f"{key}: the export blanked an ordinary field as well"
            )


# =============================================================================================
# Link 4b (core#1603 AC5): the sentence that produced a wrong acceptance criterion
# =============================================================================================

#: Any "<N> keys" phrase, in any of the spellings prose uses for it.
_COUNT_IN_PROSE = re.compile(r"(\d+)\s+keys\b")


def _audit_docstring() -> str:
    doc = audit_service_module._derive_pii_payload_keys.__doc__
    assert doc, "the docstring this section is about is gone — verdict VOID, not clean"
    return doc


class TestTheDocstringThatDescribesTheCredentialSet:
    """``audit_service`` explains why ``SECRET_CONFIG_KEYS`` is the wrong source for PII keys.

    The explanation is right and load-bearing; two facts inside it were wrong, and they are
    what produced core#1603's original acceptance criterion — a security premise built on
    prose instead of on code. It called the set *"derived from CONFIG_SCHEMAS"* (it is coupled
    by a test, not derived) and *"17 keys"* (there are 18).

    🔑 Asserted as claims about what the sentence must **name**, per ``WORKFLOW_RULES`` §5b
    rule 3 — never as a ban on a wrong word, which the corrected sentence would trip over
    while explaining itself.
    """

    def test_it_names_the_test_that_couples_the_two(self):
        """Positive form: a reader asking *how* they relate must be sent somewhere real."""
        doc = _audit_docstring()
        assert "test_secret_key_coverage" in doc, (
            "the docstring describes the relationship between SECRET_CONFIG_KEYS and "
            "CONFIG_SCHEMAS without naming the test that is the relationship. 'Derived' was "
            "the wrong word for it and cost a round; a pointer cannot be the wrong word."
        )

    def test_any_count_it_states_is_the_real_one(self):
        """A count in prose beside the data it describes is the shape that keeps going stale."""
        for stated in _COUNT_IN_PROSE.findall(_audit_docstring()):
            assert int(stated) == len(SECRET_CONFIG_KEYS), (
                f"the docstring says {stated} keys; there are {len(SECRET_CONFIG_KEYS)}. Either "
                "correct it or drop the number — the argument it makes does not need one."
            )

    def test_the_count_check_can_see_a_count(self):
        """The control: with a broken pattern the test above passes on any prose at all."""
        assert _COUNT_IN_PROSE.findall("a set of 17 keys, such as password") == ["17"]
        assert _COUNT_IN_PROSE.findall("a set of 18 keys") == ["18"]
        assert _COUNT_IN_PROSE.findall("no number here") == []

    def test_the_reason_the_set_is_the_wrong_source_is_still_stated(self):
        """§5a: correcting two facts must not take the argument out with them.

        The paragraph exists because ``SECRET_CONFIG_KEYS`` was *proposed* as the PII source.
        A redactor built on it would be derived, superset-tested, green, and would redact zero
        personal data — that sentence is the whole reason this function derives from ``*_pii``
        columns instead, and it must outlive any correction to the facts around it.

        🔑 A prose guard can only pin phrases, so it pins the two **claims** rather than the
        paragraph: that none of these keys is a PII key, and that a redactor built on them
        would redact zero personal data. Reword freely — but if you rephrase either claim,
        change this test in the same commit and say what the new wording is. A red here that
        you fix by deleting the assertion takes the reasoning with it, which is the failure
        mode ``WORKFLOW_RULES`` §5a rule 2 is about.

        ⚠️ **Two weaker versions of this assertion were measured surviving the mutation that
        deletes the claim, and both are the same defect.** ``"PII" in doc`` is satisfied by
        ``*_pii`` and by three other sentences. ``"PII key" in doc`` is satisfied by the
        *nominal* caveat two paragraphs up — *"it cannot see personal data stored under a
        non-PII key"* — which is a sentence about a **different** thing. §5b rule 3, exactly:
        presence of the phrase is not presence of the claim, so match the claim.
        """
        doc = _audit_docstring()
        assert "SECRET_CONFIG_KEYS" in doc
        assert re.search(r"one of them is a PII key", doc), (
            "the docstring no longer states that none of these keys is a PII key — the reason "
            "this function derives from `*_pii` columns instead"
        )
        assert "zero personal data" in doc, (
            "the docstring no longer states the consequence of using the wrong set, which is "
            "what makes the alternative sound plausible and green"
        )

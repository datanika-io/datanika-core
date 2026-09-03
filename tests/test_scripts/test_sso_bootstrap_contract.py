"""The SSO fixture's contract with the SP (core#830, core#768).

``e2e-sso`` has been red on **every** run since it was re-enabled on 2026-08-31,
and unmeasured since 2026-07-17 before that. Six defects, all in the fixture and
none in the application, and they fire in a fixed order so each masks the next:

===  ==========================================  ============================================
 #   fixture defect                              refusal it produces
===  ==========================================  ============================================
 1   ``sp_binding: "redirect"``                  ``Missing SAMLResponse``
 2   no ``signing_kp`` on the SAML provider      ``The Assertion ... is not signed``
 3   no ``property_mappings`` -> empty           ``Not match the saml-schema-protocol-2.0.xsd``
     ``<saml:AttributeStatement/>``
 4   fixture writes no ``idp_cert``              ``SAML IdP certificate not configured``
 5   NameID is an opaque hash, not an email      a user provisioned with a 64-hex "address"
 6   ``log`` writes to stdout, so a failed     ``JSONDecodeError`` at char 1, hiding
     POST is returned as the created object    the HTTP 400 that actually happened
===  ==========================================  ============================================

Defect 6 is the reason 1-5 could not be *observed* being fixed: it made the
bootstrap abort with a json traceback before any of them was exercised, and it
discarded the API's own explanation on the way. ``TestAFailedCallIsNotMistaken
ForSuccess`` executes the real helper against a stubbed API and is the most
load-bearing thing in this file.

Defects 1-3 are demonstrated against the real captured staging payload in
``tests/test_services/test_sso_saml_binding.py``. This file guards the *fixture*
side, and its own defect is worth stating: **a static read of a bash script is a
weak instrument.** `docs/QA_RULES.md` and WORKFLOW_RULES both warn that a guard
matching a literal template "would go green while the real thing stays a coin
flip." So the tests below are ordered by how much they actually prove:

* ``TestTheSeederPassesTheTrustAnchor`` — **behavioural.** Calls the real
  ``saml_config`` and asserts on what it returns. This is the strongest test here
  and it exists because the function was extracted from ``main()`` to make it
  possible; an inline dict is only reachable by running the seeder against a real
  Postgres, which is exactly why defect 4 had no guard.
* ``TestEveryAuthentikObjectIsCreatedOrUpdated`` — **structural.** Asserts the
  script contains no hand-rolled ``POST ... || true`` + GET-fallback, which is
  the shape that made three months of fixture edits inert. This one is a source
  read, and it is the honest limit of what can be checked without an authentik.
* ``TestTheSamlProviderBody`` — **textual**, and the weakest. It is here because
  the specific values are load-bearing security settings, not because reading
  them off the script proves they reached the box. The script now asserts that
  itself, against the API, after the PATCH — see ``SAML provider verified``.
"""

import importlib.util
import re
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_BOOTSTRAP = _REPO / "e2e" / "scripts" / "bootstrap-authentik.sh"
_SEEDER = _REPO / "e2e" / "scripts" / "seed-sso-configs.py"


def _bootstrap_source() -> str:
    assert _BOOTSTRAP.is_file(), f"{_BOOTSTRAP} is missing — this guard reads nothing"
    return _BOOTSTRAP.read_text(encoding="utf-8")


def _source_outside_the_helper() -> str:
    """The script with ``ensure_object``'s own body removed.

    The helper *deliberately* swallows its POST — that is the whole point, since
    the PATCH is what follows. Scanning the raw file would therefore flag the fix
    as the defect, and the obvious repair (narrow the pattern until the helper
    stops matching) is how a guard gets narrowed into uselessness. Excising the
    one function by name is the honest version, and it fails loudly if the helper
    is renamed rather than silently scanning nothing.
    """
    source = _bootstrap_source()
    start = source.index("ensure_object() {")
    end = source.index("\n}\n", start) + 3
    body = source[start:end]
    assert "api PATCH" in body, (
        "the excised region is not ensure_object's body — the excision is "
        "hiding real call sites from this guard"
    )
    return source[:start] + source[end:]


def _load_seeder():
    """Import ``seed-sso-configs.py`` by path (the hyphens make it un-importable).

    ⚠️ Imports at module scope pull in ``datanika.config`` and friends, which is
    fine, but the file also does ``sys.path.insert``. Loading it under a private
    module name keeps that contained.
    """
    spec = importlib.util.spec_from_file_location("_e2e_seed_sso_configs", _SEEDER)
    assert spec and spec.loader, _SEEDER
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_VALID_FIXTURE = {
    "saml": {
        "idp_metadata_url": "http://localhost:9000/application/saml/x/metadata/",
        "idp_entity_id": "http://localhost:9000/application/saml/x/sso/binding/post/",
        "idp_sso_url": "http://localhost:9000/application/saml/x/sso/binding/post/",
        "idp_cert": "MIIBogIBAAKCAQEA" + "A" * 200,
        "sp_entity_id": "datanika",
    }
}


class TestTheSeederPassesTheTrustAnchor:
    """Defect 4, behaviourally. The strongest guard in this file."""

    def test_the_seeded_config_carries_the_idp_cert(self):
        """``SSOService.create_sso_config`` reads exactly this key.

        The key name matters as much as its presence: ``create_sso_config`` does
        ``config.get("idp_cert", "")``, so a fixture spelling it ``saml_idp_cert``
        or ``certificate`` would seed an empty trust anchor just as silently as
        omitting it did.
        """
        config = _load_seeder().saml_config(_VALID_FIXTURE)

        assert config["idp_cert"] == _VALID_FIXTURE["saml"]["idp_cert"]

    def test_a_fixture_with_no_cert_raises_instead_of_seeding_an_empty_anchor(self):
        """The failure has to be loud, and this is why.

        Seeding succeeds either way — an empty ``saml_idp_cert`` is a perfectly
        valid column value. The consequence only appears later, as a 401 from an
        endpoint whose log line (until ``db83fc24``) said nothing about which of
        six things went wrong. Six weeks of red is what that costs.
        """
        no_cert = {"saml": {**_VALID_FIXTURE["saml"], "idp_cert": ""}}

        with pytest.raises(ValueError, match="idp_cert"):
            _load_seeder().saml_config(no_cert)

    def test_a_whitespace_only_cert_is_also_refused(self):
        """``create_sso_config`` stores whatever it is given; ``_saml_parse``
        checks ``(sso.saml_idp_cert or "").strip()``. A cert of spaces therefore
        passes a naive truthiness check here and is refused there — the two ends
        must agree on what counts as absent."""
        blank = {"saml": {**_VALID_FIXTURE["saml"], "idp_cert": "   \n  "}}

        with pytest.raises(ValueError, match="idp_cert"):
            _load_seeder().saml_config(blank)

    def test_the_seeder_still_passes_the_other_four_saml_keys(self):
        """Anti-vacuity. A ``saml_config`` that returned only ``idp_cert`` would
        satisfy every test above and break the connector."""
        config = _load_seeder().saml_config(_VALID_FIXTURE)

        assert set(config) == {
            "idp_metadata_url",
            "idp_entity_id",
            "idp_sso_url",
            "idp_cert",
            "sp_entity_id",
        }


class TestEveryAuthentikObjectIsCreatedOrUpdated:
    """The trap that made every previous fixture fix inert.

    ``POST ... 2>/dev/null || true`` followed by a GET fallback creates the
    object on a fresh container and, on a box where it already exists, fetches it
    **unchanged**. Staging's authentik always already has these objects. So a
    change to a creation body took effect on exactly the machines nobody was
    testing on, and was a no-op on the one that reported the verdict.
    """

    @staticmethod
    def _swallowed_posts(source: str) -> list[str]:
        """Every ``api POST`` whose own failure is discarded with ``|| true``.

        ⚠️ **A regex is the wrong instrument and this started as one.** The first
        version matched across arbitrary newlines, so an ``api POST`` that
        correctly checks its result was joined to a ``|| true`` on an unrelated
        ``docker exec`` five lines later and reported as an offender — a FALSE
        RED, which is the direction people believe and act on. The second
        version, narrowed to fix that, went green against the defect written
        with a variable body.

        So this walks statements instead: from each ``api POST``, accumulate
        lines until the brackets opened since the start are balanced, which is
        where a shell statement carrying a JSON body ends. Crude, but it answers
        the actual question — *does THIS command swallow ITS OWN failure* —
        rather than a question about line proximity.
        """
        offenders, lines = [], source.splitlines()
        i = 0
        while i < len(lines):
            if "api POST" not in lines[i]:
                i += 1
                continue
            statement, depth = "", 0
            while i < len(lines):
                statement += lines[i] + "\n"
                depth += sum(lines[i].count(c) for c in "({[")
                depth -= sum(lines[i].count(c) for c in ")}]")
                i += 1
                if depth <= 0:
                    break
            if "|| true" in statement:
                offenders.append(statement.strip())
        return offenders

    def test_no_creation_swallows_its_own_failure(self):
        """The specific idiom, banned by shape rather than by name.

        ⚠️ **This assertion was measured too narrow and widened.** Its first form
        anchored on the exact closing bytes of the original call
        (``}" 2>/dev/null || true)``), and mutation testing showed it staying
        GREEN while the idiom was reintroduced with a variable body — caught only
        by the call-site count below. A guard that matches one spelling of a
        defect is the thing WORKFLOW_RULES calls "a checker with only one
        possible answer"; the negative control underneath now pins the width.
        """
        offenders = self._swallowed_posts(_source_outside_the_helper())

        assert not offenders, (
            "an object is created with a POST whose failure is swallowed, so "
            "editing its body is a no-op wherever it already exists — which is "
            f"staging, the only box the suite runs against: {offenders}"
        )

    @pytest.mark.parametrize(
        "spelling",
        [
            'X=$(api POST /providers/saml/ -d "{\\"a\\": 1}" 2>/dev/null || true)',
            'X=$(api POST /providers/saml/ -d "$BODY" 2>/dev/null || true)',
            'api POST /core/applications/ -d "$B" > /dev/null 2>&1 || true',
            'X=$(api POST /p/ -d "{\n  \\"a\\": 1\n}" 2>/dev/null || true)',
        ],
    )
    def test_the_swallowed_post_pattern_sees_every_spelling(self, spelling):
        """Negative control, one case per way the defect has actually been
        written in this file's history. Without these the assertion above can be
        narrowed back to inertness by anyone tidying the regex."""
        assert self._swallowed_posts(spelling), spelling

    def test_the_pattern_does_not_fire_on_a_legitimate_post(self):
        """The other side of the control: a POST that checks its own result is
        fine, and a guard that also bans those would be turned off."""
        fine = 'api POST "/core/users/${SSO_USER_ID}/set_password/" -d \'{"p": "x"}\' > /dev/null'

        assert not self._swallowed_posts(fine)

    def test_the_script_defines_a_create_or_update_helper(self):
        """Anti-vacuity for the test above: it also passes on a script that
        creates nothing at all."""
        source = _bootstrap_source()

        assert "ensure_object()" in source, "no create-or-update helper is defined"
        assert "api PATCH" in source, (
            "ensure_object never PATCHes, so it is the same silent fallback with a better name"
        )

    def test_both_providers_and_both_applications_go_through_it(self):
        """Four call sites, and the count is asserted so a fifth cannot be added
        by the old idiom without failing here."""
        source = _bootstrap_source()

        call_sites = re.findall(r"ensure_object (/\S+) (\S+)", source)

        assert sorted(call_sites) == sorted(
            [
                ("/core/users/", "sso-user"),
                ("/providers/oauth2/", "datanika-oidc-e2e"),
                ("/core/applications/", "datanika-oidc-e2e"),
                ("/providers/saml/", "datanika-saml-e2e"),
                ("/core/applications/", "datanika-saml-e2e"),
            ]
        ), call_sites


class TestTheSamlProviderBody:
    """Weakest tier, and load-bearing anyway: these are security settings.

    Reading them out of the script does not prove they reached authentik — the
    script asserts that itself, against the API, after the PATCH. What these pin
    is that nobody *reverts* them to make a red job go green, which is the
    tempting move when ``e2e-sso`` has been red for weeks.
    """

    def test_the_response_binding_is_post(self):
        """core#830. A redirect-bound Response puts the assertion in a URL —
        proxy logs, browser history, replayable from either — which is why the
        spec puts Responses on POST and why the SP must not be widened to accept
        one."""
        source = _bootstrap_source()

        assert '\\"sp_binding\\": \\"post\\"' in source, (
            "the SAML provider is not configured for POST binding"
        )
        assert '\\"sp_binding\\": \\"redirect\\"' not in source

    def test_the_provider_signs_its_assertions(self):
        """core#768. ``wantAssertionsSigned`` is the fix for the 2026-07-20
        auth-bypass; an unsigned assertion is that vulnerability."""
        source = _bootstrap_source()

        assert '\\"signing_kp\\": \\"${SIGNING_KEY_PK}\\"' in source

    def test_an_absent_signing_key_is_fatal_rather_than_empty(self):
        """``r[0]['pk'] if r else ''`` used to hand ``signing_kp: ""`` to the API,
        which is the quiet path to an unsigned assertion with the script exiting
        0."""
        source = _bootstrap_source()

        assert re.search(r'if \[ -z "\$\{SIGNING_KEY_PK\}" \][\s\S]{0,400}?exit 1', source), (
            "an empty signing key no longer aborts the bootstrap"
        )

    def test_the_provider_has_property_mappings_and_a_nameid_mapping(self):
        """Defects 3 and 5. Without mappings the AttributeStatement is empty and
        schema-invalid; without a NameID mapping the subject is a 64-hex digest
        that SSO would provision as the user's email address."""
        source = _bootstrap_source()

        assert '\\"property_mappings\\": ${SAML_MAPPING_PKS}' in source
        assert '\\"name_id_mapping\\": \\"${SAML_NAMEID_PK}\\"' in source

    def test_the_fixture_file_carries_the_certificate(self):
        """Defect 4, on the writing side. The behavioural half is
        ``TestTheSeederPassesTheTrustAnchor``; this pins that something puts a
        value there for it to read."""
        source = _bootstrap_source()

        assert '"idp_cert": "${SAML_IDP_CERT}"' in source
        assert "view_certificate" in source, "nothing fetches the certificate"

    def test_nothing_still_names_a_redirect_binding_endpoint_url(self):
        """The Issuer and SSO url are compared during validation, so a stale
        ``/sso/binding/redirect/`` in either place fails the Issuer check even
        after ``sp_binding`` is corrected — a sixth defect waiting to be the next
        single-thing-changed red.

        ⚠️ **Match the endpoint, not the phrase.** The first version of this test
        asserted ``"binding/redirect" not in source`` and went red against the
        *fixed* script — because the script's own self-check contains the string
        in order to **reject** it (``if 'binding/redirect' in d[...]: sys.exit``).
        That is WORKFLOW_RULES §4's counting trap exactly: a file corrected to
        deny an old behaviour still contains the old phrase, so a substring
        search over-counts. The URL form (leading ``sso/``, trailing ``/``)
        appears only in a real endpoint reference.
        """
        source = _bootstrap_source()

        endpoints = re.findall(r"\S*sso/binding/redirect/\S*", source)

        assert not endpoints, f"the script still points at redirect-binding endpoints: {endpoints}"

    def test_the_endpoint_guard_can_see_a_real_redirect_reference(self):
        """Negative control for the test above, and it is not decoration.

        The first version of that assertion was *too wide* and the obvious repair
        is to narrow it — at which point the risk inverts and it can become too
        narrow to see anything. Running the corrected pattern against a line in
        the shape the defect actually took is what distinguishes "narrow" from
        "inert".
        """
        was_the_defect = (
            '  \\"issuer\\": \\"${AUTHENTIK_URL}'
            '/application/saml/datanika-saml-e2e/sso/binding/redirect/\\",'
        )

        assert re.findall(r"\S*sso/binding/redirect/\S*", was_the_defect), (
            "the pattern no longer matches the literal line this test exists to "
            "catch, so it would pass against the original defect"
        )


# --------------------------------------------------------------------------
# Defect 6: the helper reports success when every call failed (core#830).
# --------------------------------------------------------------------------

_BASH = shutil.which("bash")


def _bash_block(name: str) -> str:
    """Pull a multi-line function definition out of the REAL script.

    Extraction rather than transcription is the whole point: a copy of the
    function in this file would keep passing after the script changed, which is
    the failure mode ``_source_outside_the_helper`` already exists to avoid.
    """
    m = re.search(rf"^{re.escape(name)}\(\) \{{\n.*?^\}}$", _bootstrap_source(), re.M | re.S)
    assert m, f"could not find `{name}()` in {_BOOTSTRAP.name} — this guard reads nothing"
    return m.group(0)


def _log_block() -> str:
    m = re.search(r"^log\(\) \{.*\}$", _bootstrap_source(), re.M)
    assert m, f"could not find `log()` in {_BOOTSTRAP.name} — this guard reads nothing"
    return m.group(0)


def _run_bash(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [_BASH, "-c", script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
    )


@pytest.mark.skipif(
    _BASH is None, reason="bash not on PATH (CI runs ubuntu; dev boxes have Git Bash)"
)
class TestAFailedCallIsNotMistakenForSuccess:
    """``log`` must not write to stdout, because stdout is the data channel.

    ``ensure_object`` captures ``api``'s output: ``created=$(api POST ... )``.
    ``api`` reports an HTTP >= 400 by calling ``log``, and ``log`` used ``echo``
    — i.e. **stdout**. So a failed POST returned two lines of log text *as the
    created object*. Three consequences, in increasing order of severity:

    1. ``2>/dev/null`` on that POST suppressed nothing, since nothing was ever
       written to stderr.
    2. ``[ -n "$created" ]`` was satisfied **by the error message**, so the
       helper echoed log text and returned 0 — reporting success for a call that
       failed.
    3. The PATCH fallback therefore became unreachable on exactly the path it
       was written for. The comment above ``ensure_object`` explains that a
       swallowed POST made "every edit to a creation body inert where it
       mattered"; the replacement reintroduced that, one channel lower down.

    Downstream this surfaced as ``JSONDecodeError: Expecting value: line 1
    column 2 (char 1)`` — ``json.load`` consuming the ``[`` of
    ``[bootstrap-authentik] ERROR: ...``. The API's own explanation of the 400
    was captured into a shell variable and never printed, so the one diagnostic
    that would have named the real cause was destroyed by the same bug.

    These tests execute the real functions with a stubbed ``api``; no authentik,
    no network, no python3.
    """

    _STUB = (
        'api() { log "ERROR: API $1 $2 -> HTTP 400"; '
        'log "Response: {\\"detail\\":\\"stub refusal\\"}"; return 1; }\n'
        # `py` would only be reached on the fallback path; stub it so the test
        # needs no interpreter and no valid JSON.
        "py() { cat >/dev/null; printf ''; }\n"
    )

    def test_the_extraction_really_got_the_helper(self):
        """Negative control: every assertion below is vacuous on an empty block."""
        block = _bash_block("ensure_object")
        assert "PATCHing it to match" in block, (
            "the extracted block is not ensure_object's real body, so the "
            "behavioural tests below are running something else"
        )
        assert "log(" in _log_block()

    def test_log_does_not_write_to_stdout(self):
        script = _log_block() + '\nout=$(log "hello")\nprintf "OUT:[%s]" "$out"\n'
        result = _run_bash(script)
        assert result.stdout == "OUT:[]", (
            "`log` writes to stdout, so every `$(...)` capture in this script "
            "can swallow a log line as if it were data. It must redirect to "
            f"stderr (`>&2`). Captured: {result.stdout!r}"
        )

    def test_a_failing_post_does_not_return_zero(self):
        script = (
            "set -uo pipefail\n"
            + _log_block()
            + "\n"
            + _bash_block("ensure_object")
            + "\n"
            + self._STUB
            + "if out=$(ensure_object /providers/saml/ datanika-saml-e2e '{}'); "
            "then rc=0; else rc=$?; fi\n"
            'printf "RC:%s\n" "$rc"\n'
        )
        result = _run_bash(script)
        assert "RC:0" not in result.stdout, (
            "ensure_object reported SUCCESS while every API call failed. A "
            "caller then treats the error text as the created object, and the "
            "PATCH fallback never runs. stdout was: "
            f"{result.stdout!r}"
        )

    def test_a_failing_post_does_not_emit_log_text_as_the_object(self):
        script = (
            "set -uo pipefail\n"
            + _log_block()
            + "\n"
            + _bash_block("ensure_object")
            + "\n"
            + self._STUB
            + "out=$(ensure_object /providers/saml/ datanika-saml-e2e '{}') || true\n"
            'printf "OUT:[%s]" "$out"\n'
        )
        result = _run_bash(script)
        assert "[bootstrap-authentik]" not in result.stdout, (
            "ensure_object returned LOG TEXT on stdout as if it were the API "
            "object. The caller pipes this into `json.load`, which fails at "
            "char 1 on the leading '[' — a crash whose message names json, not "
            f"the HTTP error that actually happened. stdout was: {result.stdout!r}"
        )

    def test_a_successful_post_is_still_returned_verbatim(self):
        """The happy path must survive the fix.

        Guarding only the failure path would let a stricter success test ("must
        return non-zero") pass by breaking creation entirely.
        """
        script = (
            "set -uo pipefail\n"
            + _log_block()
            + "\n"
            + _bash_block("ensure_object")
            + "\n"
            + 'api() { printf \'{"pk": 7, "name": "datanika-saml-e2e"}\'; return 0; }\n'
            "py() { cat >/dev/null; printf ''; }\n"
            "if out=$(ensure_object /providers/saml/ datanika-saml-e2e '{}'); "
            "then rc=0; else rc=$?; fi\n"
            'printf "RC:%s OUT:%s" "$rc" "$out"\n'
        )
        result = _run_bash(script)
        assert result.stdout == 'RC:0 OUT:{"pk": 7, "name": "datanika-saml-e2e"}', (
            f"a successful POST must be echoed unchanged and report 0; got {result.stdout!r}"
        )

    def test_the_patch_fallback_is_now_reachable(self):
        """The point of the fix, stated as a test.

        Before it, a failing POST short-circuited with `return 0` and the PATCH
        below was dead code on the only box the suite runs against.
        """
        script = (
            "set -uo pipefail\n" + _log_block() + "\n" + _bash_block("ensure_object") + "\n"
            # POST fails; GET returns a collection; PATCH succeeds.
            'api() { case "$1 $2" in\n'
            '  "POST /providers/saml/") log "ERROR: API POST -> HTTP 400"; return 1 ;;\n'
            '  "GET /providers/saml/?search=datanika-saml-e2e") printf \'{"results":[]}\' ;;\n'
            '  "PATCH /providers/saml/9/") printf \'{"pk": 9, "patched": true}\' ;;\n'
            # No silent default: an unmatched call means the stub drifted from
            # the script, and returning 0 with no output would read as a pass.
            '  *) echo "STUB-MISS: $1 $2" >&2; return 99 ;;\n'
            "esac; }\n"
            # `py` runs twice with DIFFERENT scripts: once to pick the matching
            # object out of the collection, once to read its `pk`. One answer for
            # both makes `pk` the whole object and the PATCH URL nonsense.
            'py() { cat >/dev/null; case "$1" in '
            "*\"['pk']\"*) printf '9' ;; *) printf '{\"pk\": 9}' ;; esac; }\n"
            "if out=$(ensure_object /providers/saml/ datanika-saml-e2e '{}'); "
            "then rc=0; else rc=$?; fi\n"
            'printf "RC:%s OUT:%s" "$rc" "$out"\n'
        )
        result = _run_bash(script)
        assert result.stdout == 'RC:0 OUT:{"pk": 9, "patched": true}', (
            "a failed POST must fall through to the PATCH and return the "
            f"PATCHed object; got {result.stdout!r}"
        )
        assert "PATCHing it to match" in result.stderr, (
            "the PATCH branch did not announce itself on stderr, so the "
            "fallback may not have run at all"
        )

    def test_a_failed_call_that_still_printed_is_not_success(self):
        """The exit code is the verdict; stdout being non-empty is not.

        Redirecting ``log`` to stderr fixes today's bug on its own — with it,
        a failed ``api`` writes nothing to stdout, so ``[ -n "$created" ]``
        happens to be false. That coincidence is the whole problem: the helper
        would be reading the *right* answer off the *wrong* signal, and stays
        correct only for as long as no failure path ever writes to stdout.

        ``api`` echoing an error body, a curl progress line, or a partial
        response would each restore the original defect silently. This pins the
        contract instead of the coincidence, and is the only test here sensitive
        to the ``if created=$(...)`` exit-code check rather than to the channel.
        """
        script = (
            "set -uo pipefail\n" + _log_block() + "\n" + _bash_block("ensure_object") + "\n"
            # Fails, but writes a plausible error body to STDOUT as it goes.
            'api() { case "$1" in\n'
            '  POST) printf \'{"detail":"invalid field"}\'; return 1 ;;\n'
            '  *) echo "STUB-MISS: $1 $2" >&2; return 99 ;;\n'
            "esac; }\n"
            "py() { cat >/dev/null; printf ''; }\n"
            "if out=$(ensure_object /providers/saml/ datanika-saml-e2e '{}'); "
            "then rc=0; else rc=$?; fi\n"
            'printf "RC:%s OUT:%s" "$rc" "$out"\n'
        )
        result = _run_bash(script)
        assert not result.stdout.startswith("RC:0"), (
            "a POST that returned non-zero was treated as success because it "
            "happened to print something. The helper must test the exit code, "
            f"not whether the channel is empty. Got: {result.stdout!r}"
        )

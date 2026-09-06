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
import json
import re
import shutil
import subprocess
import sys
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


_SSO_ROUTES = _REPO / "datanika" / "services" / "sso_routes.py"

# authentik publishes one endpoint per SAML binding, under
# ``/application/saml/<slug>/sso/binding/<segment>/``. This mapping is the only
# fact in the section below that is restated rather than read out of a file, and
# it is a property of authentik's URL layout, not of our configuration.
_BINDING_URL_SEGMENT = {
    "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect": "redirect",
    "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST": "post",
}


def _sp_binding(service: str) -> str:
    """The binding the SP declares for one SAML service, read from the app.

    Derived rather than restated: if ``sso_routes.py`` ever changes how it dials
    the IdP, this moves with it instead of pinning a constant that has gone
    stale. ``singleSignOnService`` is the AuthnRequest leg (SP -> IdP);
    ``assertionConsumerService`` is the Response leg (IdP -> SP).
    """
    source = _SSO_ROUTES.read_text(encoding="utf-8")
    match = re.search(r'"' + service + r'"\s*:\s*\{[^}]*?"binding"\s*:\s*"([^"]+)"', source, re.S)
    assert match, (
        f"no {service}.binding found in {_SSO_ROUTES.name} — this guard is "
        "reading nothing, which is not the same as agreeing"
    )
    return match.group(1)


def _shell_assignment(name: str, source: str | None = None) -> str:
    """The right-hand side of a top-level ``NAME="..."`` in the bootstrap."""
    source = _bootstrap_source() if source is None else source
    match = re.search(r"^" + name + r'="([^"\n]+)"', source, re.M)
    assert match, f"{name} is not assigned in bootstrap-authentik.sh"
    return match.group(1)


def _fixture_url(key: str, source: str | None = None) -> str:
    """Resolve one ``saml`` key of the written fixture to its shell value.

    The heredoc writes ``"idp_sso_url": "${SAML_SSO_URL}"``, so the chain is
    fixture key -> shell variable -> value. Following it is what makes this
    guard see the defect it exists for: ``idp_entity_id`` and ``idp_sso_url``
    were the *same* variable, so correcting one silently moved the other.
    """
    source = _bootstrap_source() if source is None else source
    match = re.search(r'"' + key + r'"\s*:\s*"(\$\{([A-Z_]+)\})"', source)
    assert match, f'the fixture heredoc does not write "{key}" from a shell variable'
    return _shell_assignment(match.group(2), source)


def _binding_segment(url: str) -> str | None:
    match = re.search(r"/sso/binding/([a-z]+)/", url)
    return match.group(1) if match else None


_POST_URL = "http://idp:9000/application/saml/datanika-saml-e2e/sso/binding/post/"
_REDIRECT_URL = "http://idp:9000/application/saml/datanika-saml-e2e/sso/binding/redirect/"


def _fixture_self_check_block() -> str:
    """The `py "..."` block that re-reads the fixture the script just wrote.

    Extracted from the shipped script rather than transcribed, so the executable
    tests below cannot drift away from what actually runs in CI.
    """
    source = _bootstrap_source()
    marker = "d = json.load(open("
    at = source.index(marker)
    start = source.rindex('py "', 0, at) + len('py "')
    end = source.index('\n"\n', at)
    block = source[start:end]
    assert "sys.exit" in block, "the extracted region is not the fixture self-check"
    assert '"' not in block, (
        "the self-check now contains a double quote, which would terminate the "
        "shell string it lives in — this extraction, and the script, are broken"
    )
    return block


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


# --------------------------------------------------------------------------
# Defect 7: the two SAML legs are different bindings, and the fixture wrote
# both from one variable (core#830).
# --------------------------------------------------------------------------


class TestTheTwoSamlLegsAreNotConflated:
    """A SAML login crosses the wire twice, on two independently-chosen bindings.

    ===========================  ===========================  ====================
     leg                          who sends it                 binding
    ===========================  ===========================  ====================
     AuthnRequest (SP -> IdP)     ``_saml_login``: a 302 to     **HTTP-Redirect**
                                  ``{idp_sso_url}?SAMLRequest=``
     Response (IdP -> SP ACS)     authentik, per ``sp_binding``  **HTTP-POST**
    ===========================  ===========================  ====================

    Defect 1 of this file's table fixed the *Response* leg — correctly, and for a
    security reason that has not changed: an assertion in a URL is kept in proxy
    logs and browser history and is replayable from either. But the same change
    also pointed ``idp_sso_url`` at authentik's **POST**-binding endpoint, and
    added a self-check that rejected the word ``redirect`` anywhere in it. The
    two fields were literally one shell variable, ``SAML_SSO_URL``, so correcting
    the Issuer moved the SSO endpoint with it.

    Measured on run 34048476731 (`e9e5b510`, 2026-09-06), on both the attempt and
    the retry — authentik's own page, captured in Playwright's error context:

        heading "Bad Request"
        paragraph "The SAML request payload is missing."

    at ``.../sso/binding/post/?SAMLRequest=...&RelayState=...``. The POST-binding
    endpoint reads its payload from the request **body**; a query string is not
    one. Corroborated from the other end: the staging app logged
    ``SAML validation rejected`` **zero** times over the whole container
    lifetime, with WARNING lines from other loggers present as the control — so
    the assertion never reached ``_saml_parse`` at all, and this issue's standing
    description ("the callback is rejected") no longer describes the failure.

    ⚠️ The guard that used to live here banned ``/sso/binding/redirect/``
    anywhere in the script. Its *intent* was right for ``idp_entity_id`` — that
    one is compared against the assertion's Issuer — and it was applied
    file-wide to a property that is **per field**. Recorded rather than quietly
    deleted: a ban scoped wider than the property it protects will forbid the
    correct value somewhere else, and that is what happened.
    """

    def test_the_sso_endpoint_matches_the_binding_the_sp_dials_it_with(self):
        binding = _sp_binding("singleSignOnService")
        expected = _BINDING_URL_SEGMENT[binding]
        url = _fixture_url("idp_sso_url")
        segment = _binding_segment(url)

        assert segment is not None, f"idp_sso_url is not an authentik SAML SSO endpoint: {url}"
        assert segment == expected, (
            f"the SP sends its AuthnRequest on {binding.rsplit(':', 1)[-1]}, so "
            f"idp_sso_url must name authentik's '{expected}' endpoint; it names "
            f"'{segment}' ({url}). authentik answers a query-string payload on "
            "the post endpoint with 'Bad Request: The SAML request payload is "
            "missing' and the assertion never leaves the IdP (core#830)."
        )

    def test_the_entity_id_is_the_issuer_authentik_actually_stamps(self):
        """The half the old guard was right about, asserted positively.

        ``_saml_parse`` compares the Response's Issuer against
        ``idp.entityId``. authentik stamps the provider's ``issuer`` field, so
        these two must be the same string — and unlike the SSO endpoint, this one
        genuinely does name the POST-binding URL, because that is what authentik
        was configured with.

        ⚠️ Compares the two **values**, not their spellings. Either side may be a
        literal URL or a ``${VAR}`` reference, and a guard that demanded they match
        textually would go red on the correct edit that makes the provider body use
        ``SAML_ENTITY_ID`` instead of repeating the URL.
        """
        source = _bootstrap_source()
        match = re.search(r'\\"issuer\\":\s*\\"([^\\]+)\\"', source)
        assert match, "the SAML provider body no longer sets an issuer"

        issuer = match.group(1)
        reference = re.fullmatch(r"\$\{([A-Z_]+)\}", issuer)
        if reference:
            issuer = _shell_assignment(reference.group(1), source)

        assert _fixture_url("idp_entity_id") == issuer, (
            "the fixture's idp_entity_id and the provider's issuer have drifted "
            "apart; validation compares them and every assertion would be refused"
        )

    def test_the_scripts_own_fixture_check_pins_the_request_binding(self):
        """The script re-reads what it wrote and aborts on a mismatch.

        Asserted as the **presence of the right check**, not the absence of the
        old wording: a file corrected to deny an old behaviour still contains the
        old phrase (WORKFLOW_RULES §4), and the previous version of this guard
        was defeated exactly that way.
        """
        source = _bootstrap_source()
        expected = _BINDING_URL_SEGMENT[_sp_binding("singleSignOnService")]

        assert re.search(
            r"if\s+'sso/binding/" + expected + r"/'\s+not in\s+d\['saml'\]\['idp_sso_url'\]"
            r"[\s\S]{0,200}?sys\.exit",
            source,
        ), (
            "the fixture self-check no longer refuses an idp_sso_url on the "
            f"wrong binding ('{expected}' expected)"
        )

    @pytest.mark.parametrize(
        ("sso_url", "verdict"),
        [
            ("${AUTHENTIK_URL}/application/saml/x/sso/binding/redirect/", "green"),
            ("${AUTHENTIK_URL}/application/saml/x/sso/binding/post/", "red"),
            ("${AUTHENTIK_URL}/application/saml/x/metadata/", "red"),
        ],
    )
    def test_the_predicate_is_armed_against_a_synthetic_script(self, sso_url, verdict):
        """In-suite arming, so the discrimination runs every time CI does.

        A guard shown discriminating once by a hand-run harness is a claim about
        a past session. The middle row is the shape the real defect took; the
        last is the shape a careless de-duplication would take (pointing the SSO
        url at the metadata URL, which has no binding segment at all).
        """
        synthetic = f'SAML_SSO_URL="{sso_url}"\n  "idp_sso_url": "${{SAML_SSO_URL}}",\n'
        expected = _BINDING_URL_SEGMENT[_sp_binding("singleSignOnService")]
        url = _fixture_url("idp_sso_url", synthetic)
        agrees = _binding_segment(url) == expected

        assert agrees is (verdict == "green"), (
            f"the predicate answered {agrees} for {sso_url!r}, expected "
            f"{verdict}: it cannot tell the defect from the fix"
        )

    @pytest.mark.parametrize(
        ("entity_id", "sso_url", "must_exit"),
        [
            (_POST_URL, _REDIRECT_URL, False),  # the corrected fixture
            (_POST_URL, _POST_URL, True),  # defect 7 as it shipped
            (_REDIRECT_URL, _REDIRECT_URL, True),  # the same collapse, other way
            (_REDIRECT_URL, _REDIRECT_URL.replace("redirect", "post"), True),
        ],
    )
    def test_the_scripts_own_check_executes_and_refuses_each_shape(
        self, tmp_path, entity_id, sso_url, must_exit
    ):
        """Behavioural, and the reason it exists is defect 6.

        The self-check is the script's last line of defence and it runs as
        ``python3 -c`` inside a double-quoted shell string, where a stray ``$``
        or backtick is executed rather than quoted. A block that raises a
        ``SyntaxError`` aborts the bootstrap and takes the whole tier down —
        which is exactly how ``Run SSO specs`` came to be ``skipped`` for weeks.
        Reading it would not catch that; running it does.

        The block is **extracted from the real script**, not transcribed, so it
        cannot drift away from what ships.
        """
        block = _fixture_self_check_block()
        fixture = tmp_path / "sso-fixture.json"
        fixture.write_text(
            json.dumps(
                {
                    "saml": {
                        "idp_metadata_url": "http://idp/metadata/",
                        "idp_entity_id": entity_id,
                        "idp_sso_url": sso_url,
                        "idp_cert": "MIIB-not-a-real-cert",
                        "sp_entity_id": "datanika",
                    }
                }
            ),
            encoding="utf-8",
        )

        result = subprocess.run(
            [sys.executable, "-c", block.replace("${FIXTURE_FILE}", fixture.as_posix())],
            capture_output=True,
        )
        stderr = result.stderr.decode("utf-8", "replace")

        assert "SyntaxError" not in stderr and "Traceback" not in stderr, (
            f"the self-check does not even run: {stderr}"
        )
        assert (result.returncode != 0) is must_exit, (
            f"entity_id={entity_id!r} sso_url={sso_url!r} -> rc={result.returncode}, "
            f"expected {'refusal' if must_exit else 'acceptance'}. {stderr}"
        )

    def test_the_two_legs_use_different_bindings_in_the_app(self):
        """Positive control for the derivation itself.

        If both reads returned the same value — or if the regexes silently
        matched nothing and the mapping lookup happened to agree — every
        assertion above would be satisfied by an instrument that is not reading
        anything. The app declares Redirect for the request and POST for the
        response, one line apart, and the whole point of this section is that
        those are different.
        """
        request_leg = _sp_binding("singleSignOnService")
        response_leg = _sp_binding("assertionConsumerService")

        assert request_leg in _BINDING_URL_SEGMENT
        assert response_leg in _BINDING_URL_SEGMENT
        assert request_leg != response_leg, (
            "sso_routes.py now declares the same binding for both SAML legs; if "
            "that is deliberate, this section's premise needs rewriting"
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


# --------------------------------------------------------------------------
# The bootstrap's inline Python must survive shell expansion (core#854's shape).
# --------------------------------------------------------------------------


def _py_blocks() -> list[str]:
    """Every ``py "…"`` argument in the script, as bash will tokenise it.

    ``py() { python3 -c "$1"; }``, so each of these is handed to an interpreter
    verbatim. A quoting slip inside one is not a wrong answer — it is a
    ``SyntaxError`` that aborts the bootstrap under ``set -e``, skips steps 10-14 of
    ``e2e-sso`` and leaves the tier **unmeasured rather than failing**. That is
    [core#854], and it cost the whole SSO tier once.

    The self-check block is separately *executed* by
    ``TestTheScriptsOwnFixtureCheck``, which is stronger. This is the other thirteen.

    Walks the escapes rather than matching a regex: the blocks carry ``\\"`` on
    almost every line, so anything ending at the first quote reads a fragment and
    compiles it happily.
    """
    source = _bootstrap_source()
    blocks: list[str] = []
    i = 0
    needle = 'py "'
    while (start := source.find(needle, i)) != -1:
        j = start + len(needle)
        out: list[str] = []
        while j < len(source):
            ch = source[j]
            if ch == "\\" and j + 1 < len(source):
                out.append(source[j + 1])  # bash drops the backslash
                j += 2
                continue
            if ch == '"':
                break
            out.append(ch)
            j += 1
        blocks.append("".join(out))
        i = j + 1
    return blocks


@pytest.mark.skipif(
    _BASH is None, reason="bash not on PATH (CI runs ubuntu; dev boxes have Git Bash)"
)
class TestTheInlinePythonCompiles:
    def test_the_scanner_finds_the_blocks(self):
        """Positive control. A scanner returning nothing passes every test below
        while proving nothing — the shape this file keeps finding."""
        blocks = _py_blocks()

        assert len(blocks) >= 8, f"only found {len(blocks)} py blocks; the scanner has drifted"
        assert any("SAML provider verified" in b for b in blocks), (
            "the provider read-back block was not collected, so the assertion this "
            "test exists to protect is not being compiled"
        )

    @pytest.mark.parametrize("index", range(len(_py_blocks())))
    def test_each_block_is_valid_python_after_expansion(self, index):
        """Expand the block the way bash does, then compile it.

        Shell expansion is part of the artifact: ``${SAML_ENTITY_ID}`` inside a
        double-quoted argument is substituted before python sees it, and an f-string
        that was valid in the file can stop being valid once a value lands inside it.
        """
        block = _py_blocks()[index]
        assert "$(" not in block and "`" not in block, (
            "this block performs command substitution; expanding it here would "
            "execute it. Refusing rather than running (WORKFLOW_RULES §13)."
        )

        names = sorted(set(re.findall(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?", block)))
        assigns = "".join(f"{n}=x{n}\n" for n in names)
        expanded = _run_bash(assigns + 'printf %s "' + block.replace('"', '\\"') + '"')
        assert expanded.returncode == 0, expanded.stderr

        try:
            compile(expanded.stdout, "<bootstrap py block>", "exec")
        except SyntaxError as exc:  # pragma: no cover - the failure message is the point
            pytest.fail(f"block {index} does not compile after expansion: {exc}\n{expanded.stdout}")


class TestTheIssuerIsReadBackFromTheApi:
    """The provider read-back asserts every setting that matters — except one.

    ``sp_binding``, ``signing_kp``, ``sign_assertion``, ``property_mappings`` and
    ``name_id_mapping`` are all re-read from the API after ``ensure_object``, because
    *"a PATCH can be accepted and ignored (an unknown field name, a read-only
    attribute)"* — the script's own words.

    ``issuer`` was not, and it is the one value here that **nothing checks until the
    SP validates an assertion**: everything else has a symptom inside the bootstrap,
    while a wrong Issuer surfaces as a generic ``SAML validation failed`` minutes
    later, in another job step, on the tier that has never been green.
    """

    def test_the_read_back_asserts_the_issuer(self):
        source = _bootstrap_source()

        assert re.search(r"p\.get\('issuer'\)\s*!=\s*'\$\{SAML_ENTITY_ID\}'", source), (
            "the provider read-back does not assert the issuer landed, so a PATCH "
            "that was accepted and ignored would show up only as a refused assertion"
        )

    def test_the_provider_stamps_the_same_variable_the_fixture_reads(self):
        """Anti-vacuity for the test above.

        Comparing the read-back against ``${SAML_ENTITY_ID}`` says nothing unless the
        provider body is built from that variable — otherwise the assertion compares
        the API's answer to a value the provider was never sent.
        """
        source = _bootstrap_source()

        assert '\\"issuer\\": \\"${SAML_ENTITY_ID}\\"' in source, (
            "the provider body no longer builds its issuer from SAML_ENTITY_ID, so "
            "the read-back above is comparing against an unrelated variable"
        )


@pytest.mark.skipif(_BASH is None, reason="bash not on PATH")
class TestTheScriptsOwnCheckAcceptsTheValuesItWillWrite:
    """The missing arm of ``TestTheScriptsOwnFixtureCheck``, found by a mutation.

    That class executes the extracted self-check against **four synthetic URL pairs**
    and proves the block runs and refuses the collapses. It never runs it against the
    values the script will actually write.

    🔑 The gap was not visible until a change closed the accident that hid it.
    ``test_the_entity_id_is_the_issuer_authentik_actually_stamps`` compared the
    fixture's resolved ``idp_entity_id`` against the provider's ``issuer`` **text** —
    so while ``issuer`` was a literal URL and the fixture was a variable, moving
    ``SAML_ENTITY_ID`` alone went red *incidentally*. Building the provider body from
    the same variable is correct (one definition, and the read-back then asserts what
    the provider was actually sent) and it removes that accident: both sides now move
    together. Mutating ``SAML_ENTITY_ID`` to the redirect endpoint went **GREEN** —
    predicted RED — and this test is what that row bought. `ENGINEERING_RULES` §43.
    """

    def test_the_real_entity_id_and_sso_url_pass_the_scripts_own_rule(self, tmp_path):
        block = _fixture_self_check_block()
        fixture = tmp_path / "sso-fixture.json"
        fixture.write_text(
            json.dumps(
                {
                    "saml": {
                        "idp_metadata_url": "http://idp/metadata/",
                        "idp_entity_id": _fixture_url("idp_entity_id"),
                        "idp_sso_url": _fixture_url("idp_sso_url"),
                        "idp_cert": "MIIB-not-a-real-cert",
                        "sp_entity_id": "datanika",
                    }
                }
            ),
            encoding="utf-8",
        )

        result = subprocess.run(
            [sys.executable, "-c", block.replace("${FIXTURE_FILE}", fixture.as_posix())],
            capture_output=True,
        )
        stderr = result.stderr.decode("utf-8", "replace")

        assert result.returncode == 0, (
            "the script's own fixture check REFUSES the values this script writes, so "
            f"the bootstrap would abort at the last step: {stderr}"
        )

    def test_the_two_real_values_are_not_the_same_endpoint(self):
        """Anti-vacuity, and the property the mutation actually broke.

        The test above passes on any pair the self-check accepts; this names the one
        thing defect 7 was.
        """
        entity_id = _shell_assignment("SAML_ENTITY_ID")
        sso_url = _shell_assignment("SAML_SSO_URL")

        assert _binding_segment(entity_id) != _binding_segment(sso_url), (
            f"SAML_ENTITY_ID and SAML_SSO_URL both name the "
            f"{_binding_segment(entity_id)!r} binding — that is defect 7"
        )

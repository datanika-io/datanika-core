"""CI regression probe for the SAML authentication-bypass (fixed 2026-07-20; now public).

The old ``sso_routes._saml_parse`` base64-decoded the SAMLResponse and trusted the
``<NameID>`` with **no** signature validation → any org's SSO could be forged for
account takeover. The fix validates every Response via python3-saml/xmlsec (XML-DSig
against the org's IdP cert, ``wantAssertionsSigned``, audience, destination,
conditions, InResponseTo). ``sso_callback`` returns 401 on any validation failure.

This probe pins the security invariant at PR time so the bug class can't silently
return: **a forged SAMLResponse never authenticates — the callback returns 401 and
issues no token.** The Redis request-id gate is patched OPEN so the forgery is
rejected by the SAML *validator* (the fix), not the replay defence — a future
regression that weakened signature validation would flip this red.

python3-saml/xmlsec is a **main** dependency, so this imports it unconditionally
rather than via ``pytest.importorskip``. That is deliberate: a security regression
probe must never be able to stop guarding quietly. ``xmlsec`` is a compiled binding
that installs cleanly and then fails to import when its system libs are missing
(``libxmlsec1.so.1: cannot open shared object file``) — under ``importorskip`` that
would silently skip this probe and leave CI green while the auth-bypass invariant
went unchecked. Exactly the failure mode that nearly hid a broken Kafka probe in
core#407. A hard import turns the same situation into a red build, which is the
only safe direction for this particular test.
"""

import base64
import logging
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

# Import guard, deliberately mirroring the production import path: sso_routes
# imports OneLogin_Saml2_Auth *inside* the callback (not at module scope), so
# importing sso_routes below would NOT surface a broken xmlsec. Importing it
# here means a runtime-broken dependency fails collection instead of quietly
# un-guarding the auth-bypass invariant.
from onelogin.saml2.auth import OneLogin_Saml2_Auth  # noqa: F401
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services.sso_routes import _sign_state, sso_routes


def _self_signed_idp_cert() -> str:
    """A real throwaway self-signed cert (bare base64 body, as ``saml_idp_cert`` is
    stored) so OneLogin settings load cleanly — the point of the probe is that the
    forged Response is NOT signed by the matching private key."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import Encoding
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "idp.example.com")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(days=1))
        .not_valid_after(datetime.now(UTC) + timedelta(days=3650))
        .sign(key, hashes.SHA256())
    )
    pem = cert.public_bytes(Encoding.PEM).decode()
    return "".join(line for line in pem.splitlines() if "CERTIFICATE" not in line)


_IDP_CERT = _self_signed_idp_cert()
_ACS = "http://localhost:8000/api/auth/sso/callback"  # settings.oauth_redirect_base_url default


def _forged_unsigned_response(email: str, in_response_to: str) -> str:
    """A structurally-valid but UNSIGNED SAML Response asserting ``email`` — everything
    plausible (matching Destination / Audience / InResponseTo / current conditions)
    except the XML-DSig signature the attacker cannot produce. base64-encoded."""
    now = datetime.now(UTC)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    xml = (
        '<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"'
        ' xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"'
        f' ID="_resp{uuid.uuid4().hex}" Version="2.0" IssueInstant="{now.strftime(fmt)}"'
        f' Destination="{_ACS}" InResponseTo="{in_response_to}">'
        "<saml:Issuer>https://idp.example.com</saml:Issuer>"
        "<samlp:Status><samlp:StatusCode"
        ' Value="urn:oasis:names:tc:SAML:2.0:status:Success"/></samlp:Status>'
        f'<saml:Assertion ID="_ass{uuid.uuid4().hex}" Version="2.0"'
        f' IssueInstant="{now.strftime(fmt)}">'
        "<saml:Issuer>https://idp.example.com</saml:Issuer>"
        "<saml:Subject>"
        '<saml:NameID Format="urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress">'
        f"{email}</saml:NameID>"
        '<saml:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">'
        f'<saml:SubjectConfirmationData InResponseTo="{in_response_to}" Recipient="{_ACS}"'
        f' NotOnOrAfter="{(now + timedelta(minutes=5)).strftime(fmt)}"/>'
        "</saml:SubjectConfirmation></saml:Subject>"
        f'<saml:Conditions NotBefore="{(now - timedelta(minutes=5)).strftime(fmt)}"'
        f' NotOnOrAfter="{(now + timedelta(minutes=5)).strftime(fmt)}">'
        "<saml:AudienceRestriction><saml:Audience>datanika</saml:Audience>"
        "</saml:AudienceRestriction></saml:Conditions>"
        f'<saml:AuthnStatement AuthnInstant="{now.strftime(fmt)}"'
        f' SessionIndex="_sess{uuid.uuid4().hex}">'
        "<saml:AuthnContext><saml:AuthnContextClassRef>"
        "urn:oasis:names:tc:SAML:2.0:ac:classes:Password"
        "</saml:AuthnContextClassRef></saml:AuthnContext></saml:AuthnStatement>"
        "</saml:Assertion></samlp:Response>"
    )
    return base64.b64encode(xml.encode()).decode()


class TestSAMLForgeryRejection:
    """``sso_callback`` must 401 a forged assertion — the auth-bypass regression guard.

    On the old (vulnerable) ``_saml_parse`` these forgeries would have returned the
    attacker's NameID and the callback would issue a session token (302). Asserting
    401 pins the fix.
    """

    _STATE = "forgery-probe-state"
    _ORG = "victim-org"
    _REQ_ID = "_authnreq_forgery"

    def _mock_saml_sso(self) -> MagicMock:
        sso = MagicMock()
        sso.is_active = True
        sso.protocol.value = "saml"
        sso.saml_idp_cert = _IDP_CERT
        sso.saml_idp_entity_id = "https://idp.example.com"
        sso.saml_idp_sso_url = "https://idp.example.com/saml/sso"
        sso.saml_sp_entity_id = "datanika"
        return sso

    def _signed_state_cookie(self) -> str:
        return f"{self._ORG}:{self._STATE}:{_sign_state(self._STATE)}"

    def _post_callback(self, form: dict):
        app = Starlette(routes=sso_routes)
        with (
            patch("datanika.services.sso_routes._get_session") as mock_sess,
            patch("datanika.services.sso_routes._sso_service") as mock_svc,
            # Replay gate OPEN → the forgery must be stopped by the SAML validator itself.
            patch(
                "datanika.services.sso_routes._consume_saml_request_id",
                return_value=self._REQ_ID,
            ),
        ):
            mock_sess.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_sess.return_value.__exit__ = MagicMock(return_value=False)
            mock_svc.return_value.get_sso_config_by_org_slug.return_value = self._mock_saml_sso()
            client = TestClient(app, raise_server_exceptions=False)
            client.cookies.set("sso_state", self._signed_state_cookie())
            return client.post("/api/auth/sso/callback", data=form, follow_redirects=False)

    def test_unsigned_forged_assertion_is_rejected_401(self):
        resp = self._post_callback(
            {"SAMLResponse": _forged_unsigned_response("attacker@victim.example.com", self._REQ_ID)}
        )
        assert resp.status_code == 401
        # Not the success path: no auth token is issued (that path is a 302 to /auth/complete).
        assert resp.status_code != 302
        assert "token=" not in resp.headers.get("location", "")

    def test_garbage_samlresponse_is_rejected_401(self):
        resp = self._post_callback(
            {"SAMLResponse": base64.b64encode(b"<not-a-saml-response/>").decode()}
        )
        assert resp.status_code == 401

    def test_missing_samlresponse_is_rejected_401(self):
        resp = self._post_callback({})
        assert resp.status_code == 401


class TestARefusalSaysWhichRefusalItWas:
    """The callback's WARNING discarded the reason, so a red named no cause (core#830).

    ``_saml_parse`` raises ``SamlValidationError`` from **six** distinct places
    — counted from source; both core#830 and core#768 say "at least four" — and

        logger.warning("SAML validation rejected the callback for org %s", org_slug)

    rendered every one of them identically:

    ==================================  =====================================
    raise site                          means
    ==================================  =====================================
    ``Missing SAMLResponse``            nothing arrived in the request body
    ``SAML IdP certificate ...``        no trust anchor — core#768's diagnosis
    ``... possible replay``             the AuthnRequest id was already consumed
    ``... could not be processed``      python3-saml threw before validating
    ``SAML validation failed: <why>``   signature, audience, clock, conditions
    ``... did not contain a NameID``    validated, but carries no subject
    ==================================  =====================================

    So the one signal core#830 tells an operator to start from is compatible
    with every hypothesis either issue considered and can discriminate none.
    Two investigations reasoned from it and reached different wrong answers.
    That is this project's signature defect running backwards: not a green that
    proves nothing, but **a red that names the layer and withholds the cause**.

    ⚠️ Two of the six (``could not be processed`` and ``validation failed``)
    already emit a *second* line inside ``_saml_parse`` carrying detail. The
    other four emit nothing but the uniform sentence above, and the uniform
    sentence is the one both issues point at — which is why ``_reason_for``
    filters on exactly that line and not on "any warning".

    🔑 The load-bearing assertion is that the reasons are **pairwise distinct**.
    Asserting each individually would pass on an implementation that logged one
    generic string for all of them, as long as it happened to contain the right
    substring.

    Harness duplicated from ``TestSAMLForgeryRejection`` rather than inherited
    on purpose: subclassing would make pytest re-collect that class's P0
    auth-bypass regression tests under a second name, and a guard that runs
    twice under two names is harder to reason about than fifteen duplicated
    lines.
    """

    _STATE = "refusal-reason-probe"
    _ORG = "victim-org"
    _REQ_ID = "_authnreq_reason"

    def _mock_saml_sso(self) -> MagicMock:
        sso = MagicMock()
        sso.is_active = True
        sso.protocol.value = "saml"
        sso.saml_idp_cert = _IDP_CERT
        sso.saml_idp_entity_id = "https://idp.example.com"
        sso.saml_idp_sso_url = "https://idp.example.com/saml/sso"
        sso.saml_sp_entity_id = "datanika"
        return sso

    def _signed_state_cookie(self) -> str:
        return f"{self._ORG}:{self._STATE}:{_sign_state(self._STATE)}"

    def _reason_for(self, form: dict, caplog, *, cert=_IDP_CERT, request_id=None) -> str:
        """Drive the real callback and return the refusal sentence it logged."""
        caplog.clear()
        sso = self._mock_saml_sso()
        sso.saml_idp_cert = cert
        app = Starlette(routes=sso_routes)
        with (
            caplog.at_level(logging.WARNING, logger="datanika.services.sso_routes"),
            patch("datanika.services.sso_routes._get_session") as mock_sess,
            patch("datanika.services.sso_routes._sso_service") as mock_svc,
            patch(
                "datanika.services.sso_routes._consume_saml_request_id",
                return_value=self._REQ_ID if request_id is None else request_id,
            ),
        ):
            mock_sess.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_sess.return_value.__exit__ = MagicMock(return_value=False)
            mock_svc.return_value.get_sso_config_by_org_slug.return_value = sso
            client = TestClient(app, raise_server_exceptions=False)
            client.cookies.set("sso_state", self._signed_state_cookie())
            resp = client.post("/api/auth/sso/callback", data=form, follow_redirects=False)

        assert resp.status_code == 401, f"expected a refusal, got {resp.status_code}"
        rejected = [
            r.getMessage() for r in caplog.records if "rejected the callback" in r.getMessage()
        ]
        # Exactly one, or the harness is measuring something other than it claims.
        assert len(rejected) == 1, f"expected one rejection line, got {rejected}"
        return rejected[0]

    def _all_reasons(self, caplog) -> dict[str, str]:
        """Five of the six raise sites. The sixth (``did not contain a NameID``)
        needs a validly *signed* assertion carrying no subject, which this
        fixture's self-signed IdP cannot mint; it is covered by the pairwise
        assertion only insofar as the other five are."""
        return {
            "missing response": self._reason_for({}, caplog),
            "no trust anchor": self._reason_for(
                {"SAMLResponse": base64.b64encode(b"<x/>").decode()}, caplog, cert=""
            ),
            "replay": self._reason_for(
                {"SAMLResponse": base64.b64encode(b"<x/>").decode()}, caplog, request_id=""
            ),
            # Base64 of NON-XML, deliberately. `<not-a-saml-response/>` looks
            # like the obvious input here and is the wrong one: python3-saml
            # parses it happily and fails *validation* with "Unsupported SAML
            # version", landing in the branch below rather than this one.
            # Measured against the library, after the control caught the guess.
            "unprocessable": self._reason_for(
                {"SAMLResponse": base64.b64encode(b"hello world").decode()}, caplog
            ),
            "forged": self._reason_for(
                {"SAMLResponse": _forged_unsigned_response("attacker@victim.example.com", "_x")},
                caplog,
            ),
        }

    def test_each_refusal_logs_a_different_reason(self, caplog):
        """The whole issue, in one assertion.

        Distinctness is what turns the next `e2e-sso` red into a reading rather
        than an inference.
        """
        reasons = self._all_reasons(caplog)

        assert len(set(reasons.values())) == len(reasons), (
            "two different refusals log the same sentence, so the log still "
            f"cannot tell them apart: {reasons}"
        )

    def test_the_probe_reaches_five_distinct_raise_sites(self, caplog):
        """Negative control for the assertion above.

        This is not decoration — it earned its keep on the first run. The
        `unprocessable` case originally used base64 of `<not-a-saml-response/>`,
        which python3-saml parses successfully and rejects as "Unsupported SAML
        version": the *validation-failed* branch, not the *could-not-process*
        one. So the probe was hitting **four** raise sites while claiming five.

        🔑 And `test_each_refusal_logs_a_different_reason` PASSED throughout —
        the two validation-failed messages happened to carry different library
        text, so distinctness held for a reason that had nothing to do with the
        sites being distinct. A pairwise-difference assertion cannot tell you
        *which* things differed; only this one can.
        """
        reasons = self._all_reasons(caplog)

        assert "Missing SAMLResponse" in reasons["missing response"]
        assert "certificate" in reasons["no trust anchor"].lower()
        assert "replay" in reasons["replay"].lower()
        assert "could not be processed" in reasons["unprocessable"]
        assert "validation failed" in reasons["forged"]

    def test_the_missing_certificate_case_names_the_certificate(self, caplog):
        """core#768's diagnosis, and the one staging has been hitting blind."""
        reason = self._reason_for(
            {"SAMLResponse": base64.b64encode(b"<x/>").decode()}, caplog, cert=""
        )

        assert "certificate" in reason.lower(), reason

    def test_the_response_body_still_says_nothing(self, caplog):
        """The refusal must not become an oracle.

        Telling a caller *which* assertion property failed is how a validation
        endpoint turns into a probe for an org's SAML configuration. The reason
        belongs in our log and nowhere else — and putting it in the body is the
        obvious way to implement core#830.
        """
        app = Starlette(routes=sso_routes)
        with (
            patch("datanika.services.sso_routes._get_session") as mock_sess,
            patch("datanika.services.sso_routes._sso_service") as mock_svc,
            patch(
                "datanika.services.sso_routes._consume_saml_request_id",
                return_value=self._REQ_ID,
            ),
        ):
            mock_sess.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_sess.return_value.__exit__ = MagicMock(return_value=False)
            mock_svc.return_value.get_sso_config_by_org_slug.return_value = self._mock_saml_sso()
            client = TestClient(app, raise_server_exceptions=False)
            client.cookies.set("sso_state", self._signed_state_cookie())
            resp = client.post("/api/auth/sso/callback", data={}, follow_redirects=False)

        assert resp.status_code == 401
        assert resp.text == "Unauthorized: SAML assertion failed validation."
        # The two things that must never leak into the body.
        assert "SAMLResponse" not in resp.text
        assert "certificate" not in resp.text.lower()

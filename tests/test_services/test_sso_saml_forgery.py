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

python3-saml/xmlsec is a main dependency, so CI + Docker always run this; the
``importorskip`` only guards a stale local venv.
"""

import base64
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("onelogin.saml2")  # xmlsec/python3-saml needed to drive the real validator

from starlette.applications import Starlette  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from datanika.services.sso_routes import _sign_state, sso_routes  # noqa: E402


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

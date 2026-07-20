"""Security regression: SAML assertions get full validation before trust.

Guards the P0 auth bypass in ``sso_routes._saml_parse`` (the SAML Response was
base64-decoded and its ``NameID`` trusted with **no** signature verification, so
an attacker who started a login flow could forge an assertion for any victim and
receive a session). The fix validates every SAML Response against the org's IdP
certificate via python3-saml (xmlsec): signature + WantAssertionsSigned +
Audience + Destination + Conditions (expiry) + InResponseTo, with single-use
request-id consumption for replay.

Headline red-test (what the user asked for): a forged/unsigned SAMLResponse
POSTed to ``/api/auth/sso/callback`` -> **401**.
"""

import base64
import datetime as dt
import uuid
from unittest.mock import MagicMock, patch

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from onelogin.saml2.utils import OneLogin_Saml2_Utils
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.config import settings
from datanika.services.sso_routes import (
    _SSO_STATE_COOKIE,
    SamlValidationError,
    _saml_parse,
    _sign_state,
    sso_routes,
)

_IDP_ENTITY = "https://idp.example.com"
_SP_ENTITY = "datanika-test"
_REQUEST_ID = "_datanika_req_abc"
_ACS = f"{settings.oauth_redirect_base_url}/api/auth/sso/callback"


def _make_idp() -> tuple[str, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "idp.example.com")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(dt.datetime(2020, 1, 1))
        .not_valid_after(dt.datetime(2035, 1, 1))
        .sign(key, hashes.SHA256())
    )
    return key_pem, cert.public_bytes(serialization.Encoding.PEM).decode()


_KEY_PEM, _CERT_PEM = _make_idp()


def _iso(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _assertion_xml(
    *,
    email: str = "real@corp.com",
    audience: str = _SP_ENTITY,
    request_id: str = _REQUEST_ID,
    recipient: str = _ACS,
    minutes_valid: int = 5,
) -> str:
    now = dt.datetime.now(dt.UTC)
    nb = _iso(now - dt.timedelta(minutes=5))
    noa = _iso(now + dt.timedelta(minutes=minutes_valid))
    ii = _iso(now)
    return (
        f'<saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" ID="_a1"'
        f' Version="2.0" IssueInstant="{ii}"><saml:Issuer>{_IDP_ENTITY}</saml:Issuer>'
        "<saml:Subject><saml:NameID "
        'Format="urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress">'
        f"{email}</saml:NameID>"
        f'<saml:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">'
        f'<saml:SubjectConfirmationData NotOnOrAfter="{noa}" Recipient="{recipient}"'
        f' InResponseTo="{request_id}"/></saml:SubjectConfirmation></saml:Subject>'
        f'<saml:Conditions NotBefore="{nb}" NotOnOrAfter="{noa}"><saml:AudienceRestriction>'
        f"<saml:Audience>{audience}</saml:Audience></saml:AudienceRestriction></saml:Conditions>"
        f'<saml:AuthnStatement AuthnInstant="{ii}" SessionIndex="_s1"><saml:AuthnContext>'
        f"<saml:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password"
        f"</saml:AuthnContextClassRef></saml:AuthnContext></saml:AuthnStatement></saml:Assertion>"
    )


def _response_b64(
    assertion_xml: str,
    *,
    sign: bool = True,
    key_pem: str = _KEY_PEM,
    cert_pem: str = _CERT_PEM,
    destination: str = _ACS,
    request_id: str = _REQUEST_ID,
) -> str:
    a = OneLogin_Saml2_Utils.add_sign(assertion_xml, key_pem, cert_pem) if sign else assertion_xml
    if isinstance(a, bytes):
        a = a.decode()
    if a.startswith("<?xml"):
        a = a[a.index("?>") + 2 :]
    now = _iso(dt.datetime.now(dt.UTC))
    resp = (
        f'<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"'
        f' xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" ID="_r1" Version="2.0"'
        f' IssueInstant="{now}" Destination="{destination}" InResponseTo="{request_id}">'
        f"<saml:Issuer>{_IDP_ENTITY}</saml:Issuer><samlp:Status>"
        f'<samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/></samlp:Status>'
        f"{a}</samlp:Response>"
    )
    return base64.b64encode(resp.encode()).decode()


def _sso(cert: str = _CERT_PEM):
    sso = MagicMock()
    sso.saml_idp_cert = cert
    sso.saml_idp_entity_id = _IDP_ENTITY
    sso.saml_idp_sso_url = "https://idp.example.com/sso"
    sso.saml_sp_entity_id = _SP_ENTITY
    return sso


class _FakeRequest:
    def __init__(self, saml_response_b64: str):
        self._data = {"SAMLResponse": saml_response_b64}

    async def form(self):
        return self._data


async def _parse(response_b64: str, *, sso=None, request_id: str | None = _REQUEST_ID):
    """Call _saml_parse with the single-use request-id consume stubbed."""
    with patch("datanika.services.sso_routes._consume_saml_request_id", return_value=request_id):
        return await _saml_parse(_FakeRequest(response_b64), sso or _sso(), "state123")


class TestSamlParseFullValidation:
    async def test_valid_signed_assertion_accepted(self):
        email, _ = await _parse(_response_b64(_assertion_xml(email="real@corp.com")))
        assert email == "real@corp.com"

    async def test_unsigned_rejected(self):
        with pytest.raises(SamlValidationError):
            await _parse(_response_b64(_assertion_xml(email="victim@corp.com"), sign=False))

    async def test_signed_by_attacker_key_rejected(self):
        akey, acert = _make_idp()
        forged = _response_b64(
            _assertion_xml(email="victim@corp.com"), key_pem=akey, cert_pem=acert
        )
        with pytest.raises(SamlValidationError):
            await _parse(forged)  # verified against the configured IdP cert

    async def test_tampered_after_signing_rejected(self):
        b64 = _response_b64(_assertion_xml(email="real@corp.com"))
        tampered = base64.b64encode(
            base64.b64decode(b64).replace(b"real@corp.com", b"attacker@corp.com")
        ).decode()
        with pytest.raises(SamlValidationError):
            await _parse(tampered)

    async def test_wrong_audience_rejected(self):
        with pytest.raises(SamlValidationError):
            await _parse(_response_b64(_assertion_xml(audience="some-other-sp")))

    async def test_wrong_destination_rejected(self):
        forged = _response_b64(
            _assertion_xml(recipient="https://evil.example/acs"),
            destination="https://evil.example/acs",
        )
        with pytest.raises(SamlValidationError):
            await _parse(forged)

    async def test_bad_in_response_to_rejected(self):
        # Assertion built for a different request id than the one we consumed.
        forged = _response_b64(_assertion_xml(request_id="_forged"), request_id="_forged")
        with pytest.raises(SamlValidationError):
            await _parse(forged, request_id=_REQUEST_ID)

    async def test_expired_conditions_rejected(self):
        with pytest.raises(SamlValidationError):
            await _parse(_response_b64(_assertion_xml(minutes_valid=-10)))

    async def test_replayed_or_unknown_request_id_rejected(self):
        # Redis consume returns None -> already used / unknown -> replay.
        with pytest.raises(SamlValidationError):
            await _parse(_response_b64(_assertion_xml()), request_id=None)

    async def test_missing_idp_cert_rejected(self):
        with pytest.raises(SamlValidationError):
            await _parse(_response_b64(_assertion_xml()), sso=_sso(cert=""))


class TestSamlCallbackForgedReturns401:
    """The headline red-test: a forged/unsigned SAMLResponse to the ACS -> 401."""

    def _client(self, mock_svc, mock_session):
        mock_sso = _sso()
        mock_sso.is_active = True
        mock_sso.protocol.value = "saml"
        mock_svc.return_value.get_sso_config_by_org_slug.return_value = mock_sso
        mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session.return_value.__exit__ = MagicMock(return_value=False)
        return TestClient(Starlette(routes=sso_routes), raise_server_exceptions=False)

    @patch("datanika.services.sso_routes._get_session")
    @patch("datanika.services.sso_routes._sso_service")
    def test_forged_unsigned_response_returns_401(self, mock_svc, mock_session):
        org_slug = f"acme-{uuid.uuid4().hex[:6]}"
        state = "state123"
        cookie = f"{org_slug}:{state}:{_sign_state(state)}"
        forged = _response_b64(_assertion_xml(email="victim@corp.com"), sign=False)

        client = self._client(mock_svc, mock_session)
        with patch(
            "datanika.services.sso_routes._consume_saml_request_id", return_value=_REQUEST_ID
        ):
            resp = client.post(
                "/api/auth/sso/callback",
                data={"SAMLResponse": forged},
                cookies={_SSO_STATE_COOKIE: cookie},
                follow_redirects=False,
            )

        assert resp.status_code == 401

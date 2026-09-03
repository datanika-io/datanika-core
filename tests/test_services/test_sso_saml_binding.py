"""The SAML happy path, and the two fixture defects that have hidden it (core#830, core#768).

🚨 WHY THIS FILE EXISTS: **every other SAML test in this repo asserts a 401.**
``test_sso_saml_forgery.py`` mints forgeries and checks they are refused;
``test_sso_routes.py`` and ``test_sso_service.py`` never reach ``_saml_parse``.
So a ``_saml_parse`` that refused *everything* — including every real assertion
from every real IdP — would pass the entire suite. The only positive evidence
that SAML login works at all was the ``e2e-sso`` full-flow spec on staging, and

  * before 2026-07-20 it passed **because signatures went unverified** (core#768,
    the auth-bypass), so it was evidence of the bug rather than of the feature;
  * since ``e2e-sso`` was re-enabled on 2026-08-31 it has been **red on every
    single run**, so there has been no passing evidence anywhere.

That is this project's signature defect in its purest form: a suite that is green
and cannot distinguish a working implementation from a broken one. The first class
below is the missing positive — a **genuinely signed** assertion, minted here with
a throwaway keypair and validated by the real python3-saml/xmlsec, reaching the
real ``_saml_parse``. Its two negative controls are what stop it being vacuous.

WHY IT MINTS RATHER THAN MOCKS. ``add_sign`` and xmlsec do the signing, so the
signature is real and the verification is real. Mocking either would supply the
property under test — the trap that let ``test_dlt_runner.py`` assert MySQL
support for years against a ``MagicMock`` that manufactured it on demand.

────────────────────────────────────────────────────────────────────────────
THE STAGING FAILURE, DERIVED RATHER THAN GUESSED
────────────────────────────────────────────────────────────────────────────
``db83fc24`` shipped instrumentation — *"the next e2e-sso run reports a reason"* —
and shipped it deliberately alone, ahead of any fixture change, so a green would
say which change did it. **Nothing has read that reason in 37 dev pushes**: the
log line is written by the app container on staging, and ``e2e-sso`` collects the
Playwright report and never the app's log. So the instrumentation landed and its
output goes nowhere, which is the same defect one layer over.

The reason was recoverable without staging. The CI log records the browser
navigating to::

    https://staging-app.datanika.io/api/auth/sso/callback?SAMLResponse=vVXJbuM4EP...

Decoding that captured payload settles it, and the decode is reproduced by
``test_the_captured_staging_payload_is_redirect_bound`` below so the finding
cannot rot:

* it base64-decodes to **raw-DEFLATE** data, not to XML — the wire format of the
  **HTTP-Redirect** binding. HTTP-POST binding is plain base64 of the XML.
* inflated, its Issuer is
  ``http://localhost:9000/application/saml/datanika-saml-e2e/sso/binding/redirect/``
* and it carries **no ``ds:Signature`` element at all**.

So there are **several independent defects in series**, and none is in the
application. The first two are visible in the captured payload directly; the
third only appears once those are past, and the last two live in the seeding
step (see ``tests/test_scripts/test_sso_bootstrap_contract.py``):

1. **Binding.** Authentik is configured ``sp_binding: "redirect"``, so it returns
   the Response as a GET query parameter. ``_saml_parse`` reads
   ``await request.form()`` — correct for HTTP-POST, which is what our SP metadata
   advertises and what the SAML spec requires for a Response — and finds nothing.
   Refusal: ``Missing SAMLResponse``, the *first* of six raise sites.
2. **Signature.** The provider has no signing keypair, so the assertion is
   unsigned. ``wantAssertionsSigned`` is ``True`` and must stay true — an unsigned
   assertion is precisely the 2026-07-20 auth-bypass. Refusal: ``The Assertion of
   the Response is not signed and the SP require it``.

3. **Schema.** The Assertion carries ``<saml:AttributeStatement/>`` with no
   children, which ``saml-schema-assertion-2.0.xsd`` forbids. Authentik emits it
   when the SAML provider has no property mappings — OIDC's were configured and
   SAML's were not. Refusal: ``Not match the saml-schema-protocol-2.0.xsd``.

🚨 **Fixing any one of them leaves ``e2e-sso`` red**, because they fire in that
order and each masks the next. That is why two prior investigations each changed
one thing and observed no difference, and why "the fix did not work" was the
wrong reading both times. Two more sit behind these three — ``seed-sso-configs.py``
passes no ``idp_cert``, and the NameID is an opaque hash rather than an email —
and both are covered in ``tests/test_scripts/test_sso_bootstrap_contract.py``.

🔑 The application is **correct in both respects** and must not be "fixed" to
accept a redirect-bound Response. Widening the ACS to read the query string would
let an assertion arrive in a URL — logged by proxies, kept in browser history,
and replayable from either. Every test below that asserts a refusal is asserting
a security property, not a limitation.
"""

import asyncio
import base64
import re
import urllib.parse
import uuid
import zlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

# Deliberately a hard import, matching test_sso_saml_forgery.py's reasoning:
# xmlsec installs cleanly and then fails to import when its system libs are
# missing, and under `importorskip` that would silently un-guard this file.
from onelogin.saml2.utils import OneLogin_Saml2_Utils
from starlette.requests import Request

from datanika.services.sso_routes import SamlValidationError, _saml_parse, sso_metadata

_ACS = "http://localhost:8000/api/auth/sso/callback"  # settings.oauth_redirect_base_url default
_IDP_ENTITY = "https://idp.example.com"
_AUDIENCE = "datanika"
_FMT = "%Y-%m-%dT%H:%M:%SZ"

_POST_BINDING = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
_REDIRECT_BINDING = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"


def _throwaway_idp_keypair() -> tuple[str, str, str]:
    """A real RSA keypair + self-signed cert, generated once per test session.

    Returns ``(cert_body, cert_pem, key_pem)``. ``cert_body`` is the bare base64
    with no PEM armour, which is how ``SSOConfig.saml_idp_cert`` stores it.
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
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
    cert_pem = cert.public_bytes(serialization.Encoding.PEM).decode()
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    body = "".join(ln for ln in cert_pem.splitlines() if "CERTIFICATE" not in ln)
    return body, cert_pem, key_pem


_CERT_BODY, _CERT_PEM, _KEY_PEM = _throwaway_idp_keypair()


def _assertion_xml(email: str, in_response_to: str, now: datetime) -> str:
    """One Assertion, valid in every respect the SP checks except signing."""
    return (
        '<saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"'
        f' ID="_ass{uuid.uuid4().hex}" Version="2.0" IssueInstant="{now.strftime(_FMT)}">'
        f"<saml:Issuer>{_IDP_ENTITY}</saml:Issuer>"
        "<saml:Subject>"
        '<saml:NameID Format="urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress">'
        f"{email}</saml:NameID>"
        '<saml:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">'
        f'<saml:SubjectConfirmationData InResponseTo="{in_response_to}" Recipient="{_ACS}"'
        f' NotOnOrAfter="{(now + timedelta(minutes=5)).strftime(_FMT)}"/>'
        "</saml:SubjectConfirmation></saml:Subject>"
        f'<saml:Conditions NotBefore="{(now - timedelta(minutes=5)).strftime(_FMT)}"'
        f' NotOnOrAfter="{(now + timedelta(minutes=5)).strftime(_FMT)}">'
        f"<saml:AudienceRestriction><saml:Audience>{_AUDIENCE}</saml:Audience>"
        "</saml:AudienceRestriction></saml:Conditions>"
        f'<saml:AuthnStatement AuthnInstant="{now.strftime(_FMT)}"'
        f' SessionIndex="_sess{uuid.uuid4().hex}">'
        "<saml:AuthnContext><saml:AuthnContextClassRef>"
        "urn:oasis:names:tc:SAML:2.0:ac:classes:Password"
        "</saml:AuthnContextClassRef></saml:AuthnContext></saml:AuthnStatement>"
        "</saml:Assertion>"
    )


def _response_b64(email: str, in_response_to: str, *, sign: bool = True) -> str:
    """A SAML Response carrying one Assertion, base64 as the POST binding sends it.

    With ``sign=True`` the Assertion carries a real enveloped XML-DSig produced by
    xmlsec over the throwaway key — the same code path python3-saml verifies with.
    """
    now = datetime.now(UTC)
    assertion = _assertion_xml(email, in_response_to, now)
    if sign:
        signed = OneLogin_Saml2_Utils.add_sign(assertion, _KEY_PEM, _CERT_PEM)
        assertion = signed.decode() if isinstance(signed, bytes) else signed
        # add_sign returns a full document; drop the XML declaration so the
        # element can be embedded inside the Response.
        if assertion.lstrip().startswith("<?xml"):
            assertion = assertion[assertion.index("?>") + 2 :]
    xml = (
        '<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"'
        ' xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"'
        f' ID="_resp{uuid.uuid4().hex}" Version="2.0" IssueInstant="{now.strftime(_FMT)}"'
        f' Destination="{_ACS}" InResponseTo="{in_response_to}">'
        f"<saml:Issuer>{_IDP_ENTITY}</saml:Issuer>"
        '<samlp:Status><samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/>'
        "</samlp:Status>"
        f"{assertion}</samlp:Response>"
    )
    return base64.b64encode(xml.encode()).decode()


def _sso_config(cert: str = _CERT_BODY) -> MagicMock:
    sso = MagicMock()
    sso.is_active = True
    sso.protocol.value = "saml"
    sso.saml_idp_cert = cert
    sso.saml_idp_entity_id = _IDP_ENTITY
    sso.saml_idp_sso_url = f"{_IDP_ENTITY}/saml/sso"
    sso.saml_sp_entity_id = _AUDIENCE
    return sso


def _post_request(form: dict[str, str]) -> Request:
    """A REAL Starlette request carrying a form body — HTTP-POST binding."""
    body = urllib.parse.urlencode(form).encode()

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "path": "/api/auth/sso/callback",
            "raw_path": b"/api/auth/sso/callback",
            "query_string": b"",
            "root_path": "",
            "scheme": "http",
            "headers": [
                (b"content-type", b"application/x-www-form-urlencoded"),
                (b"content-length", str(len(body)).encode()),
            ],
        },
        receive=receive,
    )


def _redirect_request(params: dict[str, str]) -> Request:
    """A REAL Starlette request carrying query parameters and NO body.

    This is the shape staging actually produces. ``SAMLResponse`` is DEFLATE'd
    before base64 exactly as the HTTP-Redirect binding specifies, so the payload
    is byte-shaped like the one captured in the CI log.
    """
    compressor = zlib.compressobj(9, zlib.DEFLATED, -15)
    raw = base64.b64decode(params["SAMLResponse"])
    deflated = base64.b64encode(compressor.compress(raw) + compressor.flush()).decode()
    query = urllib.parse.urlencode({**params, "SAMLResponse": deflated}).encode()

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "path": "/api/auth/sso/callback",
            "raw_path": b"/api/auth/sso/callback",
            "query_string": query,
            "root_path": "",
            "scheme": "http",
            "headers": [],
        },
        receive=receive,
    )


def _parse(
    request: Request, *, cert: str = _CERT_BODY, request_id: str = "_rid"
) -> tuple[str, str]:
    with patch("datanika.services.sso_routes._consume_saml_request_id", return_value=request_id):
        return asyncio.run(_saml_parse(request, _sso_config(cert), "state"))


class TestAValidlySignedAssertionAuthenticates:
    """The positive the whole SAML surface has been missing.

    Every other SAML test asserts a refusal, so nothing anywhere distinguishes
    "validation works" from "validation refuses everything". This does.
    """

    _RID = "_datanika_" + uuid.uuid4().hex

    def test_a_post_bound_signed_assertion_yields_the_email(self):
        """A real IdP assertion, delivered the way the SP metadata says: accepted."""
        email, _ = _parse(
            _post_request({"SAMLResponse": _response_b64("sso-user@datanika.test", self._RID)}),
            request_id=self._RID,
        )

        assert email == "sso-user@datanika.test"

    def test_the_same_assertion_unsigned_is_refused(self):
        """Control — without this, the test above is satisfied by a parser that
        never checks anything.

        Byte-for-byte the same Response builder with ``sign=False``. This is also
        exactly what Authentik emits today (``ds:Signature`` absent from the
        captured staging payload), so the assertion doubles as core#768's proof.
        """
        with pytest_raises_saml() as refusal:
            _parse(
                _post_request(
                    {"SAMLResponse": _response_b64("sso-user@datanika.test", self._RID, sign=False)}
                ),
                request_id=self._RID,
            )

        assert "not signed" in str(refusal.value).lower(), refusal.value

    def test_tampering_with_the_signed_nameid_is_refused(self):
        """Control — proves the signature is *verified*, not merely *present*.

        A test that only compared signed-vs-unsigned would pass against an
        implementation that checked for the existence of a ``<ds:Signature>``
        element and never validated the digest. Swapping the NameID inside an
        otherwise-valid signed document is the discriminating case: the signature
        is there, and it no longer matches the bytes.
        """
        signed = base64.b64decode(_response_b64("sso-user@datanika.test", self._RID)).decode()
        tampered = signed.replace("sso-user@datanika.test", "attacker@evil.test")
        assert "attacker@evil.test" in tampered, "the harness failed to tamper with anything"

        with pytest_raises_saml() as refusal:
            _parse(
                _post_request({"SAMLResponse": base64.b64encode(tampered.encode()).decode()}),
                request_id=self._RID,
            )

        assert "validation failed" in str(refusal.value).lower(), refusal.value

    def test_a_signed_assertion_with_no_nameid_is_still_refused(self):
        """The sixth raise site, which nothing has ever been able to reach.

        ``test_sso_saml_forgery.py`` says so in ``_all_reasons``: *"the sixth
        (`did not contain a NameID`) needs a validly signed assertion carrying no
        subject, which this fixture's self-signed IdP cannot mint"*. This file
        holds the private key, so it can.

        ⚠️ **Recorded honestly: this is defence in depth, and mutation says so.**
        Flipping ``wantNameId`` to ``False`` in the real ``sso_routes.py`` leaves
        the whole file GREEN — because the library guard and our own
        ``if not email`` both refuse, and removing either leaves the other. So
        this test does not pin ``wantNameId``; it pins the *outcome*, which is
        the property that matters. Do not read its green as evidence that both
        guards are present.
        """
        now = datetime.now(UTC)
        subjectless = (
            '<saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"'
            f' ID="_ass{uuid.uuid4().hex}" Version="2.0" IssueInstant="{now.strftime(_FMT)}">'
            f"<saml:Issuer>{_IDP_ENTITY}</saml:Issuer>"
            f'<saml:Conditions NotBefore="{(now - timedelta(minutes=5)).strftime(_FMT)}"'
            f' NotOnOrAfter="{(now + timedelta(minutes=5)).strftime(_FMT)}">'
            f"<saml:AudienceRestriction><saml:Audience>{_AUDIENCE}</saml:Audience>"
            "</saml:AudienceRestriction></saml:Conditions>"
            f'<saml:AuthnStatement AuthnInstant="{now.strftime(_FMT)}"'
            f' SessionIndex="_sess{uuid.uuid4().hex}">'
            "<saml:AuthnContext><saml:AuthnContextClassRef>"
            "urn:oasis:names:tc:SAML:2.0:ac:classes:Password"
            "</saml:AuthnContextClassRef></saml:AuthnContext></saml:AuthnStatement>"
            "</saml:Assertion>"
        )
        signed = OneLogin_Saml2_Utils.add_sign(subjectless, _KEY_PEM, _CERT_PEM)
        signed = signed.decode() if isinstance(signed, bytes) else signed
        if signed.lstrip().startswith("<?xml"):
            signed = signed[signed.index("?>") + 2 :]
        xml = (
            '<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"'
            ' xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"'
            f' ID="_resp{uuid.uuid4().hex}" Version="2.0" IssueInstant="{now.strftime(_FMT)}"'
            f' Destination="{_ACS}" InResponseTo="{self._RID}">'
            f"<saml:Issuer>{_IDP_ENTITY}</saml:Issuer>"
            "<samlp:Status><samlp:StatusCode"
            ' Value="urn:oasis:names:tc:SAML:2.0:status:Success"/></samlp:Status>'
            f"{signed}</samlp:Response>"
        )

        with pytest_raises_saml():
            _parse(
                _post_request({"SAMLResponse": base64.b64encode(xml.encode()).decode()}),
                request_id=self._RID,
            )


class TestTheAssertionMustArriveByPostBinding:
    """core#830 — the staging failure, reproduced without staging.

    The discriminating property is that these tests use a Response which
    ``TestAValidlySignedAssertionAuthenticates`` has just shown is **accepted**.
    So the only variable is the binding, and the refusal can be attributed to it
    rather than to anything about the assertion's contents.
    """

    _RID = "_datanika_" + uuid.uuid4().hex

    def test_a_redirect_bound_response_is_refused_as_missing(self):
        """The exact refusal staging has been hitting since 2026-07-17.

        A GET has no body, so ``request.form()`` is empty and the *first* of six
        raise sites fires. Note what this means for the diagnosis: the missing
        IdP certificate and the unsigned assertion are both real and neither has
        ever been reached, because this check comes first.
        """
        valid = _response_b64("sso-user@datanika.test", self._RID)

        with pytest_raises_saml() as refusal:
            _parse(
                _redirect_request({"SAMLResponse": valid, "RelayState": "e2e-fixture-saml:x"}),
                request_id=self._RID,
            )

        assert str(refusal.value) == "Missing SAMLResponse", refusal.value

    def test_the_identical_response_by_post_is_accepted(self):
        """The other half of the attribution. Same bytes, different binding."""
        valid = _response_b64("sso-user@datanika.test", self._RID)

        email, _ = _parse(_post_request({"SAMLResponse": valid}), request_id=self._RID)

        assert email == "sso-user@datanika.test"

    def test_the_sp_metadata_advertises_post_and_only_post(self):
        """What we hand an IdP admin has to match what we parse.

        An IdP configured from this document cannot produce the staging failure;
        the fixture was configured by hand instead of from the metadata.
        """
        metadata = asyncio.run(
            sso_metadata(MagicMock(path_params={"org_slug": "any"}))
        ).body.decode()

        acs = re.search(r"<md:AssertionConsumerService\b[^>]*>", metadata, re.S)
        assert acs, metadata
        assert _POST_BINDING in acs.group(0)
        assert _REDIRECT_BINDING not in acs.group(0)


class TestTheCapturedStagingPayload:
    """Pins the decode that produced the diagnosis, so the finding cannot rot.

    The payload below is copied verbatim from the ``e2e-sso`` job log of run
    33692757285 (``dev 89e7e2b``, 2026-09-02T23:20:38Z) — the browser's own
    navigation target. Keeping it here means the two claims this whole file rests
    on stay checkable by anyone, without a staging box and without a CI log that
    GitHub will eventually expire.
    """

    # Truncated to the DEFLATE header plus enough body to inflate a prefix is NOT
    # possible — a raw-DEFLATE stream must be whole — so this is the full value.
    _CAPTURED = (
        "vVXJbuM4EP0VQ3O2xUUrERvIJBggwPSCJOhDXxpcijGnZdEQKTifPyXZEtpuJ9Pow5wE1vL46lWVeBPk"
        "rtmLRwh73wZYvO6aNojRuE76rhVeBhdEK3cQRNTi6fbD34KtiNh3Pnrtm%2BSHlPczZAjQRefbKcWEdb"
        "KNcS%2FS9HA4rA585buXlBFCUlKnGGOCe%2Fljit6Z9%2BF3EKWRUU7xr9DqN%2FBpSrIBHyMQ%2Fgt0"
        "AVmtE0RJFg8h9PDQhijbiCbCiiWpl4Q9My4YEYx%2FTRb3EKJrZRyzhhsCXoEZL659Wcr9fjXwaN13uX"
        "I%2BlXuXyj5u0xB8qmXTKKm%2F4z336%2BQbzWkNRAIFy7KcUqUz0IxWFiRHniWGtVNrnj0mTMDfTEEK"
        "WzBtSs645owRpQqe0TrLcisJTzY3Q0fEWE63OcnQeCSw9SGKelAZqTZOj3WkQ3Q6wS%2BH0xIYjKSVaw"
        "1WlnZgXAc6pjfpj9g3xxF6ijL24fx05w0svsimh%2Fd7F8Zo8dRrDSEk6eZ4wzmouJ0G6LJlg5QVVRmr"
        "baF5UWUF2FoWFmqmqtpCibr8Umf%2FZ9GwYPUP%2Bk6nj6jLw%2F3iL9%2FtZHxbMLqio8WZpR1DRd%2"
        "BGPWhnHZhkw3AojFGVsgBVUSrUIau1YnmZaVJpXknDC1JqXZWyyFVVKlvbMs%2BVLcscQJ94Hsmc87zz"
        "rXXDjUMPPkDc%2Bv%2FYSb0TCmQHXfI20D3K9%2Ftjvvjo46f2U3drI3Q%2FdTU%2F7usjirN3MLT997"
        "Z1Gshr%2FC9cp0IxwrjBHQaGfwJ2Ci7p0RO9Xyhh2oDeYBkaUKzYOX28%2FsyzmYo5sZrtF%2BczhPSC"
        "8owZt%2B2wgbBD7Rbj8Y39mUp5wvVFhIfWwOs6qSA3kLOc1ZxokzGaEW4Zp5wraqBUaKsI0IpxQ2qeVd"
        "jqkhhem0xnGC1nvCsKUbok9OfVHUliJRFe4xXTXYOv0CPYzbsPlRZ6iEPzZ%2FwcfGc%2B42uHzQXz3E"
        "lcNt%2FFWc8r4Fd8Z7ZZ04lhxF6oPsLsmAdu%2FufNv8RpUTb%2FAg%3D%3D"
    )

    def _xml(self) -> str:
        raw = base64.b64decode(urllib.parse.unquote(self._CAPTURED))
        # -15 = raw DEFLATE, no zlib header: the HTTP-Redirect binding's encoding.
        return zlib.decompress(raw, -15).decode()

    def test_the_captured_staging_payload_is_redirect_bound(self):
        """It does not base64-decode to XML, which is what POST binding produces.

        This is the whole binding diagnosis in one assertion, and it needs no
        staging access to re-run.
        """
        raw = base64.b64decode(urllib.parse.unquote(self._CAPTURED))

        assert not raw.lstrip().startswith(b"<"), (
            "the captured payload decodes straight to XML, i.e. POST binding — "
            "if that is now true on staging, this file's diagnosis is stale"
        )
        assert "urn:oasis:names:tc:SAML:2.0:protocol" in self._xml()
        assert "/sso/binding/redirect/" in self._xml(), "the IdP names the binding itself"

    def test_the_captured_staging_assertion_carries_no_signature(self):
        """core#768, read off the wire rather than inferred from a fixture diff."""
        xml = self._xml()

        assert "Success" in xml, "the IdP thought the login succeeded"
        assert "ds:Signature" not in xml and "SignatureValue" not in xml, (
            "the captured assertion IS signed, so core#768's premise no longer holds"
        )

    def test_the_captured_assertion_carries_an_empty_attribute_statement(self):
        """A **third** defect, and it is only visible once the first two are past.

        ``<saml:AttributeStatement/>`` with no children is not schema-valid:
        ``AttributeStatementType`` requires at least one ``Attribute`` or
        ``EncryptedAttribute``. Authentik emits it when the SAML provider has no
        property mappings — which the fixture never configured, because OIDC's
        mappings were set up and SAML's were not.

        🔑 This is why iterating against staging one push at a time is the wrong
        method here. Defects 1 and 2 mask it completely: the binding check fires
        first, the signature check second, and nothing reaches the schema until
        both are fixed. Someone shipping the binding fix alone would have watched
        ``e2e-sso`` stay red and reasonably concluded the fix did not work.
        """
        assert "<saml:AttributeStatement/>" in self._xml()

    def test_stripping_only_that_element_moves_the_refusal_past_the_schema(self):
        """The discriminating control for the test above.

        Asserting "the captured payload fails schema validation" on its own does
        not identify *which* part of it is invalid — a dozen things could produce
        that message, and the obvious suspects (the ``unspecified`` NameID format,
        the missing signature) are all present too. Removing exactly one element
        and watching the refusal move is what attributes it.

        Measured: as captured → ``Not match the saml-schema-protocol-2.0.xsd``;
        with the empty element removed → ``InResponseTo ... does not match``,
        i.e. schema validation now passes and the next check speaks instead.
        """
        raw = base64.b64decode(urllib.parse.unquote(self._CAPTURED))
        as_post = zlib.decompress(raw, -15).decode()
        stripped = as_post.replace("<saml:AttributeStatement/>", "")
        assert stripped != as_post, "the control stripped nothing — it proves nothing"

        with pytest_raises_saml() as before:
            _parse(_post_request({"SAMLResponse": base64.b64encode(as_post.encode()).decode()}))
        with pytest_raises_saml() as after:
            _parse(_post_request({"SAMLResponse": base64.b64encode(stripped.encode()).decode()}))

        assert "saml-schema-protocol" in str(before.value), before.value
        assert "saml-schema-protocol" not in str(after.value), (
            "removing the empty AttributeStatement did not clear the schema error, "
            f"so something else is also schema-invalid: {after.value}"
        )


def pytest_raises_saml():
    """``pytest.raises(SamlValidationError)`` — named so the intent reads at the call site."""
    import pytest

    return pytest.raises(SamlValidationError)


def test_this_file_is_the_only_positive_saml_evidence_in_the_suite():
    """A meta-guard, and it is not decoration.

    The reason SAML could be broken for six weeks is that *every* SAML test
    asserted a refusal, so the suite could not tell a working validator from one
    that rejected everything. If someone deletes the positive above, that state
    returns silently — the suite goes green with one fewer test.

    This fails if no test in ``test_sso_saml_binding.py`` asserts a successful
    parse, which is the property that must not be lost.
    """
    source = Path(__file__).read_text(encoding="utf-8")

    accepts = source.count('assert email == "sso-user@datanika.test"')

    assert accepts >= 2, (
        "no test here asserts that a valid assertion is ACCEPTED. Every other "
        "SAML test in this repo asserts a 401, so without a positive the suite "
        "cannot distinguish working validation from blanket refusal (core#768)."
    )

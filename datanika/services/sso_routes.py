"""SSO routes — Starlette routes for OIDC and SAML enterprise login."""

import hashlib
import hmac
import logging
import secrets
from urllib.parse import urlencode, urlparse

from starlette.requests import Request
from starlette.responses import RedirectResponse, Response
from starlette.routing import Route

from datanika.config import settings
from datanika.services.auth import AuthService
from datanika.services.encryption import EncryptionService
from datanika.services.oidc_token import IdTokenError, verify_id_token
from datanika.services.sso_service import SSOService
from datanika.services.user_service import UserService

logger = logging.getLogger(__name__)

_SSO_STATE_COOKIE = "sso_state"

# SAML AuthnRequest ids are stored single-use (InResponseTo binding + replay
# defence): written at login keyed by the state token, consumed
# (get-and-delete) at the callback. A replayed or forged Response finds no id
# and is rejected.
_SAML_REQUEST_TTL = 600  # seconds


class SamlValidationError(Exception):
    """A SAML Response failed validation. The callback maps this to a 401."""


def _redis():
    import redis

    return redis.from_url(settings.redis_url, decode_responses=True)


def _saml_request_key(state: str) -> str:
    return f"sso:saml:req:{state}"


def _store_saml_request_id(state: str, request_id: str) -> None:
    try:
        _redis().setex(_saml_request_key(state), _SAML_REQUEST_TTL, request_id)
    except Exception:
        logger.exception("Failed to store SAML request id (SAML login will fail closed)")


def _consume_saml_request_id(state: str) -> str | None:
    """Return the stored AuthnRequest id for ``state`` and delete it (single use)."""
    try:
        r = _redis()
        key = _saml_request_key(state)
        pipe = r.pipeline()
        pipe.get(key)
        pipe.delete(key)
        value, _ = pipe.execute()
        return value
    except Exception:
        logger.exception("Failed to consume SAML request id")
        return None


def _get_session():
    from datanika.db import get_sync_session

    return get_sync_session()


def _frontend(path: str) -> str:
    return f"{settings.frontend_url}{path}"


def _sign_state(state: str) -> str:
    return hmac.new(settings.secret_key.encode(), state.encode(), hashlib.sha256).hexdigest()


def _verify_state(state: str, signature: str) -> bool:
    expected = _sign_state(state)
    return hmac.compare_digest(expected, signature)


def _sso_service() -> SSOService:
    encryption = EncryptionService(settings.credential_encryption_key)
    return SSOService(encryption)


async def sso_login(request: Request) -> RedirectResponse:
    """GET /api/auth/sso/login/{org_slug} — redirect to IDP."""
    org_slug = request.path_params["org_slug"]

    with _get_session() as session:
        svc = _sso_service()
        sso = svc.get_sso_config_by_org_slug(session, org_slug)

    if sso is None or not sso.is_active:
        return RedirectResponse(
            url=_frontend("/login?error=SSO+not+configured+for+this+organization"),
            status_code=302,
        )

    if sso.protocol.value == "oidc":
        return await _oidc_login(request, sso, org_slug)
    elif sso.protocol.value == "saml":
        return await _saml_login(request, sso, org_slug)

    return RedirectResponse(url=_frontend("/login?error=Unsupported+SSO+protocol"), status_code=302)


async def _oidc_login(request: Request, sso, org_slug: str) -> RedirectResponse:
    """Build OIDC authorize URL and redirect."""
    try:
        import httpx

        async with httpx.AsyncClient() as client:
            discovery_url = sso.oidc_issuer_url.rstrip("/") + "/.well-known/openid-configuration"
            resp = await client.get(discovery_url)
            resp.raise_for_status()
            config = resp.json()
    except Exception:
        logger.exception("Failed to fetch OIDC discovery for %s", org_slug)
        return RedirectResponse(
            url=_frontend("/login?error=SSO+provider+unreachable"), status_code=302
        )

    authorize_url = config.get("authorization_endpoint", "")
    if not authorize_url:
        return RedirectResponse(
            url=_frontend("/login?error=Invalid+OIDC+configuration"), status_code=302
        )

    state = secrets.token_urlsafe(32)
    redirect_uri = f"{settings.oauth_redirect_base_url}/api/auth/sso/callback"
    params = urlencode(
        {
            "response_type": "code",
            "client_id": sso.oidc_client_id,
            "redirect_uri": redirect_uri,
            "scope": "openid email profile",
            "state": f"{org_slug}:{state}",
        }
    )

    response = RedirectResponse(url=f"{authorize_url}?{params}", status_code=302)
    signed = f"{org_slug}:{state}:{_sign_state(state)}"
    response.set_cookie(_SSO_STATE_COOKIE, signed, max_age=600, httponly=True, samesite="lax")
    return response


async def _saml_login(request: Request, sso, org_slug: str) -> RedirectResponse:
    """Build SAML AuthnRequest and redirect to IDP SSO URL."""
    if not sso.saml_idp_sso_url:
        return RedirectResponse(
            url=_frontend("/login?error=SAML+IDP+not+configured"), status_code=302
        )

    try:
        import base64
        import zlib
        from xml.etree.ElementTree import Element, SubElement, tostring

        sp_entity_id = sso.saml_sp_entity_id or settings.sso_sp_entity_id
        acs_url = f"{settings.oauth_redirect_base_url}/api/auth/sso/callback"
        request_id = f"_datanika_{secrets.token_hex(16)}"

        # Build AuthnRequest XML
        ns = "urn:oasis:names:tc:SAML:2.0:protocol"
        root = Element(f"{{{ns}}}AuthnRequest")
        root.set("xmlns:saml", "urn:oasis:names:tc:SAML:2.0:assertion")
        root.set("ID", request_id)
        root.set("Version", "2.0")
        root.set("AssertionConsumerServiceURL", acs_url)
        root.set("Destination", sso.saml_idp_sso_url)
        root.set("ProtocolBinding", "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST")

        issuer = SubElement(root, "{urn:oasis:names:tc:SAML:2.0:assertion}Issuer")
        issuer.text = sp_entity_id

        xml_bytes = tostring(root, encoding="unicode").encode("utf-8")
        deflated = zlib.compress(xml_bytes)[2:-4]  # raw deflate (no header/checksum)
        saml_request = base64.b64encode(deflated).decode()

        state = secrets.token_urlsafe(32)
        # Bind this AuthnRequest id to the state so the callback can enforce
        # InResponseTo + single-use (replay defence).
        _store_saml_request_id(state, request_id)
        params = urlencode(
            {
                "SAMLRequest": saml_request,
                "RelayState": f"{org_slug}:{state}",
            }
        )

        response = RedirectResponse(url=f"{sso.saml_idp_sso_url}?{params}", status_code=302)
        signed = f"{org_slug}:{state}:{_sign_state(state)}"
        response.set_cookie(_SSO_STATE_COOKIE, signed, max_age=600, httponly=True, samesite="lax")
        return response
    except Exception:
        logger.exception("Failed to build SAML AuthnRequest for %s", org_slug)
        return RedirectResponse(url=_frontend("/login?error=SAML+request+failed"), status_code=302)


async def sso_callback(request: Request) -> RedirectResponse:
    """GET/POST /api/auth/sso/callback — handle IDP response."""
    # Determine protocol from state/cookie
    cookie_value = request.cookies.get(_SSO_STATE_COOKIE, "")
    parts = cookie_value.split(":")
    if len(parts) < 3:
        return RedirectResponse(url=_frontend("/login?error=Invalid+SSO+state"), status_code=302)

    org_slug = parts[0]
    stored_state = parts[1]
    signature = parts[2]

    if not _verify_state(stored_state, signature):
        return RedirectResponse(url=_frontend("/login?error=Invalid+SSO+state"), status_code=302)

    with _get_session() as session:
        svc = _sso_service()
        sso = svc.get_sso_config_by_org_slug(session, org_slug)

        if sso is None or not sso.is_active:
            return RedirectResponse(
                url=_frontend("/login?error=SSO+not+configured"), status_code=302
            )

        try:
            if sso.protocol.value == "oidc":
                email, full_name, email_verified = await _oidc_exchange(request, sso, svc)
            elif sso.protocol.value == "saml":
                email, full_name = await _saml_parse(request, sso, stored_state)
                # The assertion is only reached here after full validation
                # against the org's configured IdP certificate, so the address
                # is asserted by the IdP the org's own admin designated as
                # authoritative for its users.
                email_verified = True
            else:
                return RedirectResponse(
                    url=_frontend("/login?error=Unsupported+protocol"), status_code=302
                )

            if not email:
                return RedirectResponse(
                    url=_frontend("/login?error=SSO+did+not+return+email"), status_code=302
                )

            # Reuse OAuth user creation flow
            auth = AuthService(settings.secret_key)
            user_svc = UserService(auth)
            provider_name = f"{sso.protocol.value}:{org_slug}"

            user, is_new = user_svc.find_or_create_oauth_user(
                session,
                email,
                full_name or email.split("@")[0],
                oauth_provider=provider_name,
                oauth_provider_id=email,
                email_verified=email_verified,
            )

            # Ensure user is a member of the SSO org
            org = session.execute(
                __import__("sqlalchemy")
                .select(__import__("datanika.models.user", fromlist=["Organization"]).Organization)
                .where(
                    __import__("datanika.models.user", fromlist=["Organization"]).Organization.slug
                    == org_slug
                )
            ).scalar_one_or_none()
            # Gross imports above — let me fix inline
        except SamlValidationError:
            # A forged / unsigned / tampered / expired / replayed assertion is
            # an attack, not a user error — reject with 401, issue no session.
            logger.warning("SAML validation rejected the callback for org %s", org_slug)
            return Response("Unauthorized: SAML assertion failed validation.", status_code=401)
        except Exception:
            logger.exception("SSO callback failed for %s", org_slug)
            return RedirectResponse(
                url=_frontend("/login?error=SSO+authentication+failed"), status_code=302
            )

        # Issue JWT tokens
        from sqlalchemy import select

        from datanika.models.user import Membership, Organization

        org = session.execute(
            select(Organization).where(Organization.slug == org_slug)
        ).scalar_one_or_none()

        if org:
            # Check membership, create if needed
            membership = session.execute(
                select(Membership).where(
                    Membership.user_id == user.id,
                    Membership.org_id == org.id,
                    Membership.deleted_at.is_(None),
                )
            ).scalar_one_or_none()

            if membership is None:
                from datanika.models.user import MemberRole

                membership = Membership(user_id=user.id, org_id=org.id, role=MemberRole.VIEWER)
                session.add(membership)
                session.flush()

            org_id = org.id
        else:
            # Fallback to user's first org
            membership = session.execute(
                select(Membership)
                .where(
                    Membership.user_id == user.id,
                    Membership.deleted_at.is_(None),
                )
                .order_by(Membership.id)
                .limit(1)
            ).scalar_one_or_none()
            org_id = membership.org_id if membership else 0

        access_token = auth.create_access_token(user.id, org_id)
        refresh_token = auth.create_refresh_token(user.id)
        session.commit()

    params = urlencode(
        {
            "token": access_token,
            "refresh": refresh_token,
            "is_new": "1" if is_new else "0",
        }
    )
    response = RedirectResponse(url=_frontend(f"/auth/complete?{params}"), status_code=302)
    response.delete_cookie(_SSO_STATE_COOKIE)
    return response


async def _oidc_exchange(request: Request, sso, svc: SSOService) -> tuple[str, str, bool]:
    """Exchange an OIDC authorization code for user info.

    Returns ``(email, name, email_verified)``. The third element states whether
    this address was actually asserted *by the issuer*, and it is what stops the
    caller from linking a session to an account on an unauthenticated claim.

    Note the trust model differs from public social login (``oauth_service``):
    there we require the provider's own ``email_verified`` claim, because anyone
    may open a Google or GitHub account. Here the org's admin configured this
    issuer as authoritative for their users, so establishing that the response
    genuinely came from *that issuer* is the check that matters. Many enterprise
    IdPs never emit ``email_verified`` at all.
    """
    import httpx

    code = request.query_params.get("code", "")
    if not code:
        raise ValueError("Missing authorization code")

    # Fetch discovery config
    async with httpx.AsyncClient() as client:
        discovery_url = sso.oidc_issuer_url.rstrip("/") + "/.well-known/openid-configuration"
        disco = (await client.get(discovery_url)).json()

        token_endpoint = disco["token_endpoint"]
        userinfo_endpoint = disco.get("userinfo_endpoint", "")

        # Exchange code for tokens
        client_secret = svc.get_oidc_client_secret(sso)
        redirect_uri = f"{settings.oauth_redirect_base_url}/api/auth/sso/callback"

        token_resp = await client.post(
            token_endpoint,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": sso.oidc_client_id,
                "client_secret": client_secret,
            },
        )
        token_resp.raise_for_status()
        tokens = token_resp.json()

        # Get user info
        access_token = tokens.get("access_token", "")
        if userinfo_endpoint and access_token:
            user_resp = await client.get(
                userinfo_endpoint,
                headers={"Authorization": f"Bearer {access_token}"},
            )
            user_resp.raise_for_status()
            user_info = user_resp.json()
            # Reached over TLS at an endpoint named by the issuer's own
            # discovery document, authenticated with the access token this
            # exchange just minted: the issuer is the one saying this.
            return user_info.get("email", ""), user_info.get("name", ""), True

        # Fallback for issuers that publish no `userinfo_endpoint`. The claims
        # are only attributable to the issuer once the signature over them
        # chains to a key the issuer publishes, so a failure here yields
        # nothing at all rather than an unverified address.
        id_token = tokens.get("id_token", "")
        if id_token:
            try:
                claims = await verify_id_token(
                    client, id_token, disco, client_id=sso.oidc_client_id
                )
            except IdTokenError:
                logger.warning(
                    "OIDC id_token failed verification for issuer %s", disco.get("issuer")
                )
                return "", "", False
            return claims.get("email", ""), claims.get("name", ""), True

    return "", "", False


async def _saml_parse(request: Request, sso, state: str) -> tuple[str, str]:
    """Validate a SAML Response and extract the user's email.

    SECURITY (auth boundary): the assertion is trusted ONLY after full SAML
    validation against the org's configured IdP certificate — XML-DSig
    signature (WantAssertionsSigned), Audience, Destination, Conditions
    (NotBefore / NotOnOrAfter), and InResponseTo. Any failure raises
    ``SamlValidationError``, which the callback turns into a 401. Parsing and
    signature verification are done by python3-saml (xmlsec): there is no
    hand-rolled XML, so no XXE and no signature-wrapping. Replay is prevented
    by consuming the single-use AuthnRequest id bound at login.
    """
    from onelogin.saml2.auth import OneLogin_Saml2_Auth
    from onelogin.saml2.utils import OneLogin_Saml2_Utils

    form = await request.form()
    saml_response = form.get("SAMLResponse", "")
    if not saml_response:
        raise SamlValidationError("Missing SAMLResponse")

    if not (sso.saml_idp_cert or "").strip():
        # No trust anchor configured -> nothing to verify against -> refuse.
        raise SamlValidationError("SAML IdP certificate not configured")

    # Consume the single-use AuthnRequest id (InResponseTo + replay defence).
    # A replayed or unsolicited Response finds no id here and is rejected.
    request_id = _consume_saml_request_id(state)
    if not request_id:
        raise SamlValidationError("Unknown or already-used SAML request (possible replay)")

    acs_url = f"{settings.oauth_redirect_base_url}/api/auth/sso/callback"
    parsed = urlparse(acs_url)
    req_data = {
        "https": "on" if parsed.scheme == "https" else "off",
        "http_host": parsed.netloc,  # includes the port when the ACS URL has one
        "script_name": parsed.path,
        "get_data": {},
        "post_data": {"SAMLResponse": saml_response},
    }
    saml_settings = {
        "strict": True,
        "sp": {
            "entityId": sso.saml_sp_entity_id or settings.sso_sp_entity_id,
            "assertionConsumerService": {
                "url": acs_url,
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
            },
            "NameIDFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress",
        },
        "idp": {
            "entityId": sso.saml_idp_entity_id or "",
            "singleSignOnService": {
                "url": sso.saml_idp_sso_url or acs_url,
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
            },
            "x509cert": OneLogin_Saml2_Utils.format_cert(sso.saml_idp_cert),
        },
        "security": {
            "wantAssertionsSigned": True,
            "wantMessagesSigned": False,
            "wantNameId": True,
            "wantAttributeStatement": False,
            "requestedAuthnContext": False,
            "rejectUnsolicitedResponsesWithInResponseTo": True,
        },
    }

    try:
        auth = OneLogin_Saml2_Auth(req_data, old_settings=saml_settings)
        auth.process_response(request_id=request_id)
    except Exception as exc:
        logger.warning("SAML response could not be processed: %s", type(exc).__name__)
        raise SamlValidationError("SAML response could not be processed") from exc

    if not auth.is_authenticated():
        reason = auth.get_last_error_reason() or "; ".join(auth.get_errors())
        logger.warning("SAML validation failed: %s", reason)
        raise SamlValidationError(f"SAML validation failed: {reason}")

    email = (auth.get_nameid() or "").strip()
    if not email:
        raise SamlValidationError("SAML assertion did not contain a NameID")

    full_name = ""
    attributes = auth.get_attributes()
    for key in (
        "displayName",
        "http://schemas.microsoft.com/identity/claims/displayname",
        "name",
        "cn",
    ):
        values = attributes.get(key)
        if values:
            full_name = values[0]
            break

    return email, full_name


async def sso_metadata(request: Request) -> Response:
    """GET /api/auth/sso/metadata/{org_slug} — SP metadata XML for SAML IDP setup."""
    _ = request.path_params["org_slug"]  # validate path param exists
    sp_entity_id = settings.sso_sp_entity_id
    acs_url = f"{settings.oauth_redirect_base_url}/api/auth/sso/callback"

    metadata = f"""<?xml version="1.0" encoding="UTF-8"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
    entityID="{sp_entity_id}">
  <md:SPSSODescriptor
      AuthnRequestsSigned="false"
      WantAssertionsSigned="true"
      protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
    <md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress</md:NameIDFormat>
    <md:AssertionConsumerService
        Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
        Location="{acs_url}"
        index="1" />
  </md:SPSSODescriptor>
</md:EntityDescriptor>"""

    return Response(content=metadata, media_type="application/xml")


sso_routes = [
    Route("/api/auth/sso/login/{org_slug}", sso_login, methods=["GET"]),
    Route("/api/auth/sso/callback", sso_callback, methods=["GET", "POST"]),
    Route("/api/auth/sso/metadata/{org_slug}", sso_metadata, methods=["GET"]),
]

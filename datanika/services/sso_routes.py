"""SSO routes — Starlette routes for OIDC and SAML enterprise login."""

import hashlib
import hmac
import logging
import secrets
from urllib.parse import urlencode

from starlette.requests import Request
from starlette.responses import RedirectResponse, Response
from starlette.routing import Route

from datanika.config import settings
from datanika.services.auth import AuthService
from datanika.services.encryption import EncryptionService
from datanika.services.sso_service import SSOService
from datanika.services.user_service import UserService

logger = logging.getLogger(__name__)

_SSO_STATE_COOKIE = "sso_state"


def _get_session():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    engine = create_engine(settings.database_url_sync)
    return SASession(engine)


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

    return RedirectResponse(
        url=_frontend("/login?error=Unsupported+SSO+protocol"), status_code=302
    )


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
    params = urlencode({
        "response_type": "code",
        "client_id": sso.oidc_client_id,
        "redirect_uri": redirect_uri,
        "scope": "openid email profile",
        "state": f"{org_slug}:{state}",
    })

    response = RedirectResponse(url=f"{authorize_url}?{params}", status_code=302)
    signed = f"{org_slug}:{state}:{_sign_state(state)}"
    response.set_cookie(
        _SSO_STATE_COOKIE, signed, max_age=600, httponly=True, samesite="lax"
    )
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
        params = urlencode({
            "SAMLRequest": saml_request,
            "RelayState": f"{org_slug}:{state}",
        })

        response = RedirectResponse(
            url=f"{sso.saml_idp_sso_url}?{params}", status_code=302
        )
        signed = f"{org_slug}:{state}:{_sign_state(state)}"
        response.set_cookie(
            _SSO_STATE_COOKIE, signed, max_age=600, httponly=True, samesite="lax"
        )
        return response
    except Exception:
        logger.exception("Failed to build SAML AuthnRequest for %s", org_slug)
        return RedirectResponse(
            url=_frontend("/login?error=SAML+request+failed"), status_code=302
        )


async def sso_callback(request: Request) -> RedirectResponse:
    """GET/POST /api/auth/sso/callback — handle IDP response."""
    # Determine protocol from state/cookie
    cookie_value = request.cookies.get(_SSO_STATE_COOKIE, "")
    parts = cookie_value.split(":")
    if len(parts) < 3:
        return RedirectResponse(
            url=_frontend("/login?error=Invalid+SSO+state"), status_code=302
        )

    org_slug = parts[0]
    stored_state = parts[1]
    signature = parts[2]

    if not _verify_state(stored_state, signature):
        return RedirectResponse(
            url=_frontend("/login?error=Invalid+SSO+state"), status_code=302
        )

    with _get_session() as session:
        svc = _sso_service()
        sso = svc.get_sso_config_by_org_slug(session, org_slug)

        if sso is None or not sso.is_active:
            return RedirectResponse(
                url=_frontend("/login?error=SSO+not+configured"), status_code=302
            )

        try:
            if sso.protocol.value == "oidc":
                email, full_name = await _oidc_exchange(request, sso, svc)
            elif sso.protocol.value == "saml":
                email, full_name = await _saml_parse(request, sso)
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
                session, email, full_name or email.split("@")[0],
                oauth_provider=provider_name,
                oauth_provider_id=email,
            )

            # Ensure user is a member of the SSO org
            org = session.execute(
                __import__("sqlalchemy").select(
                    __import__("datanika.models.user", fromlist=["Organization"]).Organization
                ).where(
                    __import__("datanika.models.user", fromlist=["Organization"]).Organization.slug
                    == org_slug
                )
            ).scalar_one_or_none()
            # Gross imports above — let me fix inline
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

                membership = Membership(
                    user_id=user.id, org_id=org.id, role=MemberRole.VIEWER
                )
                session.add(membership)
                session.flush()

            org_id = org.id
        else:
            # Fallback to user's first org
            membership = session.execute(
                select(Membership).where(
                    Membership.user_id == user.id,
                    Membership.deleted_at.is_(None),
                ).order_by(Membership.id).limit(1)
            ).scalar_one_or_none()
            org_id = membership.org_id if membership else 0

        access_token = auth.create_access_token(user.id, org_id)
        refresh_token = auth.create_refresh_token(user.id)
        session.commit()

    params = urlencode({
        "token": access_token,
        "refresh": refresh_token,
        "is_new": "1" if is_new else "0",
    })
    response = RedirectResponse(url=_frontend(f"/auth/complete?{params}"), status_code=302)
    response.delete_cookie(_SSO_STATE_COOKIE)
    return response


async def _oidc_exchange(request: Request, sso, svc: SSOService) -> tuple[str, str]:
    """Exchange OIDC authorization code for user info."""
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

        token_resp = await client.post(token_endpoint, data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": sso.oidc_client_id,
            "client_secret": client_secret,
        })
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
            email = user_info.get("email", "")
            name = user_info.get("name", "")
            return email, name

        # Try extracting from ID token
        id_token = tokens.get("id_token", "")
        if id_token:
            import base64
            import json

            payload = id_token.split(".")[1]
            payload += "=" * (4 - len(payload) % 4)
            claims = json.loads(base64.urlsafe_b64decode(payload))
            return claims.get("email", ""), claims.get("name", "")

    return "", ""


async def _saml_parse(request: Request, sso) -> tuple[str, str]:
    """Parse SAML Response and extract user email."""
    import base64
    from xml.etree.ElementTree import fromstring

    form = await request.form()
    saml_response = form.get("SAMLResponse", "")
    if not saml_response:
        raise ValueError("Missing SAMLResponse")

    xml_bytes = base64.b64decode(saml_response)
    root = fromstring(xml_bytes)

    # Extract NameID (email)
    ns = {
        "saml": "urn:oasis:names:tc:SAML:2.0:assertion",
        "samlp": "urn:oasis:names:tc:SAML:2.0:protocol",
    }
    name_id = root.find(".//saml:NameID", ns)
    email = name_id.text if name_id is not None else ""

    # Extract attributes for name
    full_name = ""
    for attr in root.findall(".//saml:Attribute", ns):
        attr_name = attr.get("Name", "")
        val_elem = attr.find("saml:AttributeValue", ns)
        if (
            val_elem is not None
            and val_elem.text
            and ("displayName" in attr_name or "name" in attr_name.lower())
        ):
                full_name = val_elem.text
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

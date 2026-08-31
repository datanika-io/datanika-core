"""OAuth 2.1 Authorization Server for the hosted MCP endpoint (core#393, P2).

Implements the MCP remote-auth flow (SPEC_REMOTE_MCP §5.3/§8/§10) so an MCP
client can add Datanika with one click instead of a pasted API key:

    POST /mcp (no token)            -> 401 + WWW-Authenticate: resource_metadata=...
    GET  /.well-known/oauth-protected-resource
    GET  /.well-known/oauth-authorization-server
    POST /oauth/register            -> RFC 7591 dynamic client registration
    GET  /oauth/authorize           -> validate, then hand off to the consent screen
    POST /api/oauth/consent         -> (app JWT) mint the key + authorization code
    POST /oauth/token               -> code+PKCE -> access/refresh; rotating refresh

**Why consent redirects to the frontend.** Datanika's browser session is a JWT
the Reflex frontend holds (social login lands on ``/auth/complete?token=…``),
so a backend route cannot read it from a cookie. ``/oauth/authorize`` therefore
validates the OAuth request and redirects to the frontend consent page, which
carries the user's JWT to ``POST /api/oauth/consent`` in an Authorization
header. No token is ever put in a query string.

**Scope of the grant.** Read-only by default — silence is never consent to
write. Since P3 (core#442) a client may *ask* for write, and the human decides
at the consent screen; the minted key carries exactly what was granted, so
``api_middleware`` enforces it and this layer adds no authz of its own (§5.4).

**Why consent-time and not per-call.** SPEC §10 asked for a per-call
confirmation before a remote agent spends compute or money. That assumed an
interactive session; the hosted transport is stateless, and a two-phase token
the agent mints and echoes to itself is friction, not a gate — the agent is the
thing being gated. The decision that matters is a *human* one, so it belongs at
consent, made once and knowingly. Spend is bounded by the per-key rate limit
and byte quota, which §10 named alongside the phasing.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode, urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.config import settings
from datanika.models.api_key import ApiKey
from datanika.models.mcp_oauth import OAuthClient, OAuthGrant, OAuthToken
from datanika.services.api_key_service import ApiKeyService
from datanika.services.encryption import EncryptionService

MCP_RESOURCE_PATH = "/mcp"

#: Where the frontend renders the consent screen (Product owns the page itself).
CONSENT_PATH = "/oauth/consent"

_CODE_TTL = timedelta(minutes=5)
_ACCESS_TTL = timedelta(hours=1)
_REFRESH_TTL = timedelta(days=30)

_CODE_PREFIX = "dtk_ac_"
_ACCESS_PREFIX = "dtk_at_"
_REFRESH_PREFIX = "dtk_rt_"

#: Every resource family the REST API exposes, read half only. A grant carries
#: exactly these, so `api_middleware` rejects writes without the MCP layer
#: implementing any authz of its own (SPEC §5.4).
_READ_SCOPES = (
    "catalog:read",
    "connections:read",
    "notifications:read",
    "pipelines:read",
    "runs:read",
    "schedules:read",
    "transformations:read",
    "uploads:read",
)


#: The write half. P3 (core#442) lets a consent grant these — the human decides
#: once, knowingly, at the consent screen. There is deliberately **no** finer
#: "may build but may not trigger" tier: the REST API gives ``create_upload``
#: and ``trigger_upload`` the same ``uploads:write`` scope, so a split could
#: only be enforced in the MCP layer while the credential still permitted both.
#: A control the token doesn't back is theatre. Spend is bounded by the per-key
#: rate limit and byte quota instead (SPEC §10).
_WRITE_SCOPES = (
    "connections:write",
    "notifications:write",
    "pipelines:write",
    "runs:write",
    "schedules:write",
    "transformations:write",
    "uploads:write",
)


def read_only_scopes() -> list[str]:
    """The scope set a consent grants by default."""
    return list(_READ_SCOPES)


def write_scopes() -> list[str]:
    """Read **plus** write — a write-only grant would be useless."""
    return list(_READ_SCOPES) + list(_WRITE_SCOPES)


def narrow_scope(requested: str) -> list[str]:
    """The scopes a consent will actually grant for ``requested``.

    Module-level and public because the consent **screen** has to describe this
    before the user approves it (core#450). Two implementations of this rule —
    one minting the grant, one describing it — is exactly how the screen came
    to promise read-only while handing over write.

    An empty or unrecognised request collapses to **read-only**: MCP clients
    routinely omit ``scope``, and silence must never be read as consent to
    write (core#442). Write is granted only when a write scope was explicitly
    asked for — and then read comes with it, since a write-only credential is
    useless.
    """
    asked = [s for s in (requested or "").split() if s]
    if not asked:
        return read_only_scopes()

    if any(s in _WRITE_SCOPES for s in asked):
        granted = [s for s in write_scopes() if s in asked or s in _READ_SCOPES]
        return granted or write_scopes()

    narrowed = [s for s in asked if s in _READ_SCOPES]
    return narrowed or read_only_scopes()


def grants_write(requested: str) -> bool:
    """Whether ``requested`` will mint a grant that can change anything."""
    return any(s in _WRITE_SCOPES for s in narrow_scope(requested))


class McpOAuthError(Exception):
    """An OAuth error, carrying the RFC 6749 error code as its message.

    The code is what goes on the wire (``invalid_grant``, ``invalid_request``,
    …); ``description`` is the human half, safe to show a developer.
    """

    def __init__(self, code: str, description: str = "") -> None:
        super().__init__(code)
        self.code = code
        self.description = description


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _now() -> datetime:
    return datetime.now(UTC)


def _aware(value: datetime | None) -> datetime | None:
    """SQLite hands back naive datetimes; compare in UTC regardless."""
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _is_loopback(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host in {"127.0.0.1", "::1", "localhost"}


@dataclass(frozen=True)
class ResolvedGrant:
    """What a bearer token resolves to: a credential *and* its authority.

    ``allow_write`` is a property of the **grant**, not of the transport — the
    human decided it at the consent screen — so it travels with the key rather
    than being inferred anywhere downstream (core#442).
    """

    api_key: str
    allow_write: bool
    scope: str


class McpOAuthService:
    """Authorization-server logic. Transport-free so it is testable directly."""

    def __init__(
        self,
        public_base_url: str | None = None,
        encryption_key_provider=None,
        api_key_service: ApiKeyService | None = None,
    ) -> None:
        self._base = (public_base_url or settings.oauth_redirect_base_url).rstrip("/")
        self._encryption_key = encryption_key_provider or (
            lambda: settings.credential_encryption_key
        )
        self._keys = api_key_service or ApiKeyService()

    # ------------------------------------------------------------------
    # Identity of this AS / RS
    # ------------------------------------------------------------------

    @property
    def issuer(self) -> str:
        return self._base

    @property
    def resource(self) -> str:
        """The RFC 8707 resource identifier tokens are bound to."""
        return f"{self._base}{MCP_RESOURCE_PATH}"

    def protected_resource_metadata(self) -> dict:
        return {
            "resource": self.resource,
            "authorization_servers": [self.issuer],
            # Advertise write too — a client cannot ask for what the AS never
            # lists, and the grant is what actually decides (core#442).
            "scopes_supported": write_scopes(),
            "bearer_methods_supported": ["header"],
        }

    def authorization_server_metadata(self) -> dict:
        return {
            "issuer": self.issuer,
            "authorization_endpoint": f"{self._base}/oauth/authorize",
            "token_endpoint": f"{self._base}/oauth/token",
            "registration_endpoint": f"{self._base}/oauth/register",
            # Advertise write too — a client cannot ask for what the AS never
            # lists, and the grant is what actually decides (core#442).
            "scopes_supported": write_scopes(),
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            # OAuth 2.1: PKCE is mandatory and `plain` is gone.
            "code_challenge_methods_supported": ["S256"],
            # Public clients only — there are no secrets to authenticate with.
            "token_endpoint_auth_methods_supported": ["none"],
        }

    # ------------------------------------------------------------------
    # RFC 7591 — dynamic client registration
    # ------------------------------------------------------------------

    def register_client(self, session: Session, client_name: str, redirect_uris: list[str]) -> dict:
        if not redirect_uris:
            raise McpOAuthError("invalid_redirect_uri", "At least one redirect_uri is required.")
        for uri in redirect_uris:
            parsed = urlparse(uri)
            # https everywhere, except the loopback interface native clients
            # must use (RFC 8252 §7.3).
            if parsed.scheme == "https":
                continue
            if parsed.scheme == "http" and _is_loopback(uri):
                continue
            raise McpOAuthError(
                "invalid_redirect_uri",
                f"redirect_uri must be https (or http on loopback): {uri}",
            )

        client = OAuthClient(
            client_id="mcp_" + secrets.token_urlsafe(24),
            client_name=(client_name or "MCP client")[:255],
            redirect_uris=list(redirect_uris),
        )
        session.add(client)
        session.flush()

        return {
            "client_id": client.client_id,
            "client_name": client.client_name,
            "redirect_uris": client.redirect_uris,
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
            "client_id_issued_at": int(_now().timestamp()),
        }

    def get_client(self, session: Session, client_id: str) -> OAuthClient | None:
        return session.execute(
            select(OAuthClient).where(
                OAuthClient.client_id == client_id, OAuthClient.deleted_at.is_(None)
            )
        ).scalar_one_or_none()

    # ------------------------------------------------------------------
    # Authorization request validation (pre-consent)
    # ------------------------------------------------------------------

    def validate_authorization_request(
        self,
        session: Session,
        *,
        client_id: str,
        redirect_uri: str,
        response_type: str,
        code_challenge: str,
        code_challenge_method: str,
        resource: str | None,
    ) -> OAuthClient:
        """Check everything that must hold before a human is shown a consent screen.

        Errors here cannot be redirected back to the client (an unvalidated
        ``redirect_uri`` is exactly what an attacker would supply), so callers
        render them directly.
        """
        client = self.get_client(session, client_id)
        if client is None:
            raise McpOAuthError("invalid_client", "Unknown client_id.")
        if redirect_uri not in (client.redirect_uris or []):
            raise McpOAuthError(
                "invalid_request", "redirect_uri is not registered for this client."
            )
        if response_type != "code":
            raise McpOAuthError(
                "unsupported_response_type", "Only response_type=code is supported."
            )
        self._check_pkce(code_challenge, code_challenge_method)
        self._check_resource(resource)
        return client

    def _check_pkce(self, code_challenge: str, code_challenge_method: str) -> None:
        if code_challenge_method != "S256":
            raise McpOAuthError(
                "invalid_request", "code_challenge_method must be S256 (OAuth 2.1)."
            )
        if not code_challenge:
            raise McpOAuthError("invalid_request", "code_challenge is required.")

    def _check_resource(self, resource: str | None) -> None:
        """RFC 8707 — refuse to mint a token for anyone else's resource.

        This is the confused-deputy guard (SPEC §10): a token we issue is only
        ever valid for our own ``/mcp``, so it cannot be replayed elsewhere,
        and we never accept one minted elsewhere.
        """
        if resource is None:
            return  # optional; the token is bound to our resource regardless
        if resource.rstrip("/") != self.resource:
            raise McpOAuthError("invalid_target", f"Unknown resource: {resource}")

    # ------------------------------------------------------------------
    # Consent — mint the org-scoped key and the one-time code
    # ------------------------------------------------------------------

    def grant_consent(
        self,
        session: Session,
        *,
        client_id: str,
        org_id: int,
        user_id: int,
        redirect_uri: str,
        code_challenge: str,
        code_challenge_method: str,
        scope: str,
        resource: str | None,
    ) -> tuple[OAuthGrant, str]:
        """Record consent. Returns ``(grant, authorization_code)``.

        The raw code is returned once and never stored — only its hash is, so a
        database leak cannot be replayed into a token.
        """
        client = self.get_client(session, client_id)
        if client is None:
            raise McpOAuthError("invalid_client", "Unknown client_id.")
        if redirect_uri not in (client.redirect_uris or []):
            raise McpOAuthError(
                "invalid_request", "redirect_uri is not registered for this client."
            )
        self._check_pkce(code_challenge, code_challenge_method)
        self._check_resource(resource)

        granted = self._narrow_scope(scope)

        api_key, raw_key = self._keys.create_api_key(
            session,
            org_id=org_id,
            user_id=user_id,
            name=f"MCP: {client.client_name}"[:255],
            scopes=granted,
        )

        code = _CODE_PREFIX + secrets.token_urlsafe(32)
        grant = OAuthGrant(
            client_id=client.client_id,
            org_id=org_id,
            user_id=user_id,
            api_key_id=api_key.id,
            encrypted_api_key=self._encrypt(raw_key),
            code_hash=_sha256(code),
            code_challenge=code_challenge,
            redirect_uri=redirect_uri,
            scope=" ".join(granted),
            resource=self.resource,
            code_expires_at=_now() + _CODE_TTL,
        )
        session.add(grant)
        session.flush()
        return grant, code

    def _narrow_scope(self, requested: str) -> list[str]:
        """Grant no more than was asked for, and never write by accident.

        Delegates to the module-level :func:`narrow_scope` so the consent
        screen can describe the same grant this mints, from the same code.
        """
        return narrow_scope(requested)

    # ------------------------------------------------------------------
    # Token endpoint
    # ------------------------------------------------------------------

    def exchange_code(
        self, session: Session, *, code: str, code_verifier: str, redirect_uri: str
    ) -> dict:
        grant = session.execute(
            select(OAuthGrant).where(
                OAuthGrant.code_hash == _sha256(code),
                OAuthGrant.revoked_at.is_(None),
                OAuthGrant.deleted_at.is_(None),
            )
        ).scalar_one_or_none()
        # A redeemed code has had its hash cleared, so a replay lands here too.
        if grant is None:
            raise McpOAuthError("invalid_grant", "Authorization code is invalid or already used.")

        expires = _aware(grant.code_expires_at)
        if expires is not None and expires < _now():
            raise McpOAuthError("invalid_grant", "Authorization code has expired.")
        if redirect_uri != grant.redirect_uri:
            raise McpOAuthError("invalid_grant", "redirect_uri does not match the grant.")
        if not self._verify_pkce(code_verifier, grant.code_challenge):
            raise McpOAuthError("invalid_grant", "PKCE verification failed.")

        # Burn the code before issuing anything — a concurrent replay finds
        # nothing to redeem.
        grant.code_hash = None
        session.flush()

        return self._issue(session, grant)

    def _org_grant(self, session: Session, org_id: int, grant_id: int) -> OAuthGrant | None:
        """Resolve a grant within an org — the single definition of ownership.

        #732. A bare ``session.get(OAuthGrant, id)`` returns whichever
        tenant's row carries that primary key. The caller always knows the
        org by this point (it came from the token that was just verified),
        so a mismatch is a corrupt reference and must resolve to nothing.
        """
        return session.execute(
            select(OAuthGrant).where(
                OAuthGrant.id == grant_id,
                OAuthGrant.org_id == org_id,
            )
        ).scalar_one_or_none()

    def _verify_pkce(self, verifier: str, challenge: str) -> bool:
        if not verifier:
            return False
        digest = hashlib.sha256(verifier.encode()).digest()
        computed = base64.urlsafe_b64encode(digest).decode().rstrip("=")
        return secrets.compare_digest(computed, challenge)

    def refresh(self, session: Session, *, refresh_token: str) -> dict:
        token = session.execute(
            select(OAuthToken).where(
                OAuthToken.refresh_token_hash == _sha256(refresh_token),
                OAuthToken.revoked_at.is_(None),
                OAuthToken.deleted_at.is_(None),
            )
        ).scalar_one_or_none()
        if token is None:
            raise McpOAuthError("invalid_grant", "Refresh token is invalid or already rotated.")

        refresh_expires = _aware(token.refresh_expires_at)
        if refresh_expires is not None and refresh_expires < _now():
            raise McpOAuthError("invalid_grant", "Refresh token has expired.")

        grant = self._org_grant(session, token.org_id, token.grant_id)
        if grant is None or grant.revoked_at is not None:
            raise McpOAuthError("invalid_grant", "The underlying grant was revoked.")

        # Rotation: the presented pair dies as the new one is born, so a stolen
        # refresh token is single-use and its reuse is detectable.
        token.revoked_at = _now()
        session.flush()
        return self._issue(session, grant)

    def _issue(self, session: Session, grant: OAuthGrant) -> dict:
        access = _ACCESS_PREFIX + secrets.token_urlsafe(32)
        refresh = _REFRESH_PREFIX + secrets.token_urlsafe(32)
        now = _now()

        session.add(
            OAuthToken(
                grant_id=grant.id,
                org_id=grant.org_id,
                access_token_hash=_sha256(access),
                refresh_token_hash=_sha256(refresh),
                expires_at=now + _ACCESS_TTL,
                refresh_expires_at=now + _REFRESH_TTL,
            )
        )
        session.flush()

        return {
            "access_token": access,
            "refresh_token": refresh,
            "token_type": "Bearer",
            "expires_in": int(_ACCESS_TTL.total_seconds()),
            "scope": grant.scope,
        }

    # ------------------------------------------------------------------
    # Resource-server side — bearer -> the credential the tools replay
    # ------------------------------------------------------------------

    def resolve_access_token(self, session: Session, access_token: str) -> ResolvedGrant | None:
        """Return the credential **and the authority** behind a bearer token.

        Returns a :class:`ResolvedGrant` rather than a bare key string because
        the tool surface has to know how far the *human* opened it (core#442):
        write is a property of the grant, not of the transport, so it has to
        travel with the credential.

        ``None`` covers every rejection — unknown, expired, revoked grant, and
        (deliberately) a revoked *API key*: revocation in the existing keys UI
        has to kill the MCP session too, which is the whole point of minting a
        real key rather than inventing a parallel credential (SPEC §5.3).
        """
        if not access_token or not access_token.startswith(_ACCESS_PREFIX):
            return None

        token = session.execute(
            select(OAuthToken).where(
                OAuthToken.access_token_hash == _sha256(access_token),
                OAuthToken.revoked_at.is_(None),
                OAuthToken.deleted_at.is_(None),
            )
        ).scalar_one_or_none()
        if token is None:
            return None

        expires = _aware(token.expires_at)
        if expires is not None and expires < _now():
            return None

        # #732: the token is resolved by hash (cross-org by necessity — it is
        # what establishes the org), but everything after that point has an
        # org in hand. Following these foreign keys by bare primary key means
        # a stored cross-org reference hands out another tenant's API key.
        grant = self._org_grant(session, token.org_id, token.grant_id)
        if grant is None or grant.revoked_at is not None or grant.deleted_at is not None:
            return None

        api_key = session.execute(
            select(ApiKey).where(
                ApiKey.id == grant.api_key_id,
                ApiKey.org_id == grant.org_id,
            )
        ).scalar_one_or_none()
        if api_key is None or api_key.deleted_at is not None:
            return None
        key_expiry = _aware(api_key.expires_at)
        if key_expiry is not None and key_expiry < _now():
            return None

        granted = (grant.scope or "").split()
        return ResolvedGrant(
            api_key=self.decrypt_api_key(grant),
            allow_write=any(s in _WRITE_SCOPES for s in granted),
            scope=grant.scope or "",
        )

    # ------------------------------------------------------------------
    # Credential storage
    # ------------------------------------------------------------------

    def _encrypt(self, raw_key: str) -> str:
        return EncryptionService(self._encryption_key()).encrypt({"key": raw_key})

    def decrypt_api_key(self, grant: OAuthGrant) -> str:
        return EncryptionService(self._encryption_key()).decrypt(grant.encrypted_api_key)["key"]

    # ------------------------------------------------------------------
    # Consent hand-off URL
    # ------------------------------------------------------------------

    def consent_redirect_url(self, params: dict, frontend_url: str | None = None) -> str:
        """Where ``/oauth/authorize`` sends the browser to collect consent."""
        base = (frontend_url or settings.frontend_url).rstrip("/")
        return f"{base}{CONSENT_PATH}?{urlencode(params)}"

    @staticmethod
    def client_redirect_url(redirect_uri: str, code: str, state: str | None) -> str:
        params = {"code": code}
        if state:
            params["state"] = state
        joiner = "&" if "?" in redirect_uri else "?"
        return f"{redirect_uri}{joiner}{urlencode(params)}"

    @staticmethod
    def error_json(exc: McpOAuthError) -> bytes:
        return json.dumps({"error": exc.code, "error_description": exc.description}).encode()

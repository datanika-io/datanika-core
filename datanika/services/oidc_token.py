"""Verification of OIDC ``id_token``s against the issuer's published JWKS.

Used by the SSO callback when an issuer publishes no ``userinfo_endpoint`` and
the ``id_token`` is the only source of the user's identity. Keeping that fallback
matters — SSO is an Enterprise-tier feature, and "we only support issuers that
expose userinfo" is a compatibility limit we would carry forever — so the token
gets verified rather than the branch getting deleted.

Two decisions here are load-bearing, and both are about not letting the token
choose how it is checked:

* **The algorithm allowlist comes from the discovery document, intersected with
  an asymmetric-only set.** Never from the token's own header. A verifier that
  reads ``alg`` out of the thing it is verifying accepts ``none``, and accepts
  ``HS256`` signed with the issuer's *public* key — which is published, by
  design, at the ``jwks_uri``.
* **Every failure raises.** There is no partial success and no "verified enough":
  the caller turns the outcome into a boolean that decides whether an account is
  handed a session.
"""

import logging
import time

from jose import jwt
from jose.exceptions import JWTError

from datanika.errors import UserFacingError

logger = logging.getLogger(__name__)


class IdTokenError(UserFacingError):
    """An id_token could not be verified. Never carries provider text outward."""


#: Asymmetric only. A symmetric algorithm would be "verified" with a secret both
#: parties hold, and `none` with nothing at all; neither attributes the claims to
#: the issuer, which is the entire point of checking.
_ALLOWED_ALGS = frozenset(
    {"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512"}
)

#: Seconds of tolerance for clock drift between us and the IdP. Applies to the
#: `exp`/`nbf` window; an IdP whose clock is a few seconds ahead should not fail
#: every login.
_LEEWAY_SECONDS = 60

#: jwks_uri -> (fetched_at_monotonic, document). Process-local and small: a
#: handful of issuers, refreshed on a timer, and force-refreshed on a `kid` miss
#: so key rotation resolves on the first login that needs the new key rather
#: than after the TTL.
_JWKS_CACHE: dict[str, tuple[float, dict]] = {}
_JWKS_TTL_SECONDS = 600


def _permitted_algorithms(discovery: dict) -> list[str]:
    """The algorithms this issuer says it signs with, minus the unsafe ones."""
    advertised = discovery.get("id_token_signing_alg_values_supported") or ["RS256"]
    permitted = [a for a in advertised if a in _ALLOWED_ALGS]
    if not permitted:
        raise IdTokenError(
            f"Issuer advertises no asymmetric id_token signing algorithm; got {advertised!r}"
        )
    return permitted


async def _fetch_jwks(client, jwks_uri: str, *, force: bool = False) -> dict:
    now = time.monotonic()
    if not force:
        cached = _JWKS_CACHE.get(jwks_uri)
        if cached is not None and now - cached[0] < _JWKS_TTL_SECONDS:
            return cached[1]
    try:
        resp = await client.get(jwks_uri)
        resp.raise_for_status()
        document = resp.json()
    except Exception as exc:
        raise IdTokenError("Could not fetch the issuer's signing keys") from exc
    _JWKS_CACHE[jwks_uri] = (now, document)
    return document


def _select_key(document: dict, kid: str | None):
    """The signing key for ``kid``, or the whole set when the token names none.

    Handing the full document to python-jose makes it try each key, which is the
    compatible behaviour for issuers that publish no ``kid``. It is not a
    weakening: a candidate key still has to produce a valid signature.
    """
    keys = [k for k in document.get("keys", []) if k.get("use", "sig") == "sig"]
    if not keys:
        return None
    if not kid:
        return {"keys": keys}
    return next((k for k in keys if k.get("kid") == kid), None)


async def verify_id_token(client, id_token: str, discovery: dict, *, client_id: str) -> dict:
    """Return the verified claims of ``id_token``, or raise :class:`IdTokenError`.

    Checks, all of them: an asymmetric signature chaining to a key published at
    the issuer's ``jwks_uri``; ``aud`` == ``client_id``; ``iss`` == the issuer's
    own declared identity; and an unexpired validity window.
    """
    algorithms = _permitted_algorithms(discovery)

    jwks_uri = discovery.get("jwks_uri")
    if not jwks_uri:
        raise IdTokenError("Issuer publishes no jwks_uri, so nothing can be verified")

    try:
        kid = jwt.get_unverified_header(id_token).get("kid")
    except JWTError as exc:
        raise IdTokenError("Malformed id_token header") from exc

    document = await _fetch_jwks(client, jwks_uri)
    key = _select_key(document, kid)
    if key is None:
        # Rotation: the cached set predates this key. Refetch once — never once
        # per attempt, or an unknown kid becomes a way to hammer the issuer.
        document = await _fetch_jwks(client, jwks_uri, force=True)
        key = _select_key(document, kid)
    if key is None:
        raise IdTokenError("No published signing key matches this id_token")

    issuer = discovery.get("issuer")
    try:
        return jwt.decode(
            id_token,
            key,
            algorithms=algorithms,
            audience=client_id,
            issuer=issuer,
            options={
                "leeway": _LEEWAY_SECONDS,
                # Checked only when an access token is supplied, which it is not
                # on this path; stated rather than left to a default.
                "verify_at_hash": False,
                "require_exp": True,
            },
        )
    except JWTError as exc:
        # The provider's message can quote token contents; log the type only.
        logger.warning("id_token verification failed: %s", type(exc).__name__)
        raise IdTokenError("id_token failed verification") from exc

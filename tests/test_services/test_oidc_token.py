"""An OIDC ``id_token`` is a bearer of claims until its signature is checked.

``_oidc_exchange`` falls back to the ``id_token`` when an issuer publishes no
``userinfo_endpoint``. That fallback exists for a real reason — SSO is an
Enterprise-tier feature and dropping it would narrow provider compatibility
permanently — so the answer is to verify the token, not to delete the branch.

Verification here means all of: an asymmetric signature that chains to a key the
issuer publishes at its own ``jwks_uri``; an ``aud`` equal to our client id; an
``iss`` equal to the issuer's own declared identity; and an unexpired window.
Two of these tests exist specifically because getting the *shape* right is not
enough — ``test_alg_none_is_rejected`` and
``test_public_key_used_as_an_hmac_secret_is_rejected`` are the two classic ways a
verifier that reads its algorithm out of the token it is verifying accepts
anything at all.
"""

import base64
import hashlib
import hmac
import json
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jose import jwk, jwt

from datanika.services import oidc_token
from datanika.services.oidc_token import IdTokenError, verify_id_token

ISSUER = "https://idp.example.com"
CLIENT_ID = "datanika-client"
JWKS_URI = "https://idp.example.com/jwks"


def _keypair():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    public_pem = (
        key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return private_pem, public_pem


@pytest.fixture(scope="module")
def key_a():
    return _keypair()


@pytest.fixture(scope="module")
def key_b():
    return _keypair()


def _public_jwk(public_pem: str, kid: str) -> dict:
    d = jwk.construct(public_pem, algorithm="RS256").to_dict()
    out = {}
    for k, v in d.items():
        out[k] = v.decode() if isinstance(v, bytes) else v
    out["kid"] = kid
    out["use"] = "sig"
    out["alg"] = "RS256"
    return out


def _epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def _claims(**overrides) -> dict:
    now = datetime.now(UTC)
    base = {
        "iss": ISSUER,
        "aud": CLIENT_ID,
        "sub": "user-1",
        "email": "person@example.com",
        "name": "A Person",
        "iat": _epoch(now),
        "exp": _epoch(now + timedelta(minutes=5)),
    }
    base.update(overrides)
    return base


def _sign(private_pem: str, kid: str, **overrides) -> str:
    return jwt.encode(_claims(**overrides), private_pem, algorithm="RS256", headers={"kid": kid})


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _forge(header: dict, claims: dict, signature: bytes = b"") -> str:
    """Hand-assemble a JWT.

    python-jose refuses to *produce* an `alg: none` token, and refuses to use an
    asymmetric PEM as an HMAC secret — so a forgery cannot be built with the same
    library that verifies it. An attacker has no such constraint, and neither
    does this helper. Building these by hand is the point: it is the only way the
    two classic bypasses actually reach the verifier under test.
    """
    h = _b64(json.dumps(header, separators=(",", ":")).encode())
    p = _b64(json.dumps(claims, separators=(",", ":")).encode())
    return f"{h}.{p}.{_b64(signature)}"


def _disco(**overrides) -> dict:
    d = {
        "issuer": ISSUER,
        "jwks_uri": JWKS_URI,
        "token_endpoint": f"{ISSUER}/token",
        "id_token_signing_alg_values_supported": ["RS256"],
    }
    d.update(overrides)
    return d


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class _CountingClient:
    """Serves a JWKS document and counts how many times it was asked for one."""

    def __init__(self, jwks_sequence: list[dict]):
        self._sequence = jwks_sequence
        self.fetches = 0

    async def get(self, url, **_kw):
        assert url == JWKS_URI, f"unexpected fetch of {url}"
        self.fetches += 1
        index = min(self.fetches - 1, len(self._sequence) - 1)
        return _FakeResponse(self._sequence[index])


@pytest.fixture(autouse=True)
def _clear_cache():
    """The JWKS cache is module-level; a leaked entry makes the next test lie."""
    oidc_token._JWKS_CACHE.clear()
    yield
    oidc_token._JWKS_CACHE.clear()


# ---------------------------------------------------------------------------
# The happy path, and the things that must not pass as one
# ---------------------------------------------------------------------------
class TestSignatureVerification:
    @pytest.mark.asyncio
    async def test_valid_token_is_accepted(self, key_a):
        private, public = key_a
        client = _CountingClient([{"keys": [_public_jwk(public, "a")]}])
        claims = await verify_id_token(client, _sign(private, "a"), _disco(), client_id=CLIENT_ID)
        assert claims["email"] == "person@example.com"
        assert claims["sub"] == "user-1"

    @pytest.mark.asyncio
    async def test_foreign_key_signature_is_rejected(self, key_a, key_b):
        """Signed by someone who is not the issuer."""
        _, public_a = key_a
        private_b, _ = key_b
        client = _CountingClient([{"keys": [_public_jwk(public_a, "a")]}])
        with pytest.raises(IdTokenError):
            await verify_id_token(client, _sign(private_b, "a"), _disco(), client_id=CLIENT_ID)

    @pytest.mark.asyncio
    async def test_alg_none_is_rejected(self, key_a):
        """An unsigned token must not verify, whatever its header claims."""
        _, public = key_a
        unsigned = _forge({"alg": "none", "typ": "JWT", "kid": "a"}, _claims())
        client = _CountingClient([{"keys": [_public_jwk(public, "a")]}])
        with pytest.raises(IdTokenError):
            await verify_id_token(client, unsigned, _disco(), client_id=CLIENT_ID)

    @pytest.mark.asyncio
    async def test_public_key_used_as_an_hmac_secret_is_rejected(self, key_a):
        """Algorithm confusion: the issuer's *public* key is published, so if the
        verifier honours a header that says HS256 the attacker can sign with it.

        The allowlist comes from the discovery document, never from the token.
        """
        _, public = key_a
        header = {"alg": "HS256", "typ": "JWT", "kid": "a"}
        claims = _claims()
        signing_input = (
            f"{_b64(json.dumps(header, separators=(',', ':')).encode())}."
            f"{_b64(json.dumps(claims, separators=(',', ':')).encode())}"
        ).encode()
        mac = hmac.new(public.encode(), signing_input, hashlib.sha256).digest()
        forged = _forge(header, claims, mac)

        client = _CountingClient([{"keys": [_public_jwk(public, "a")]}])
        with pytest.raises(IdTokenError):
            await verify_id_token(client, forged, _disco(), client_id=CLIENT_ID)

    @pytest.mark.asyncio
    async def test_issuer_advertising_only_symmetric_algs_is_refused(self, key_a):
        _, public = key_a
        client = _CountingClient([{"keys": [_public_jwk(public, "a")]}])
        with pytest.raises(IdTokenError):
            await verify_id_token(
                client,
                _sign(key_a[0], "a"),
                _disco(id_token_signing_alg_values_supported=["HS256", "none"]),
                client_id=CLIENT_ID,
            )


class TestClaimVerification:
    @pytest.mark.asyncio
    async def test_wrong_audience_is_rejected(self, key_a):
        """A token minted for a different client is not a token for us."""
        private, public = key_a
        client = _CountingClient([{"keys": [_public_jwk(public, "a")]}])
        with pytest.raises(IdTokenError):
            await verify_id_token(
                client,
                _sign(private, "a", aud="some-other-client"),
                _disco(),
                client_id=CLIENT_ID,
            )

    @pytest.mark.asyncio
    async def test_wrong_issuer_is_rejected(self, key_a):
        private, public = key_a
        client = _CountingClient([{"keys": [_public_jwk(public, "a")]}])
        with pytest.raises(IdTokenError):
            await verify_id_token(
                client,
                _sign(private, "a", iss="https://evil.example.net"),
                _disco(),
                client_id=CLIENT_ID,
            )

    @pytest.mark.asyncio
    async def test_expired_token_is_rejected(self, key_a):
        private, public = key_a
        past = datetime.now(UTC) - timedelta(hours=2)
        client = _CountingClient([{"keys": [_public_jwk(public, "a")]}])
        with pytest.raises(IdTokenError):
            await verify_id_token(
                client,
                _sign(private, "a", iat=past, exp=past + timedelta(minutes=5)),
                _disco(),
                client_id=CLIENT_ID,
            )

    @pytest.mark.asyncio
    async def test_small_clock_skew_is_tolerated(self, key_a):
        """An IdP clock a few seconds ahead of ours must not fail every login."""
        private, public = key_a
        soon = datetime.now(UTC) + timedelta(seconds=20)
        client = _CountingClient([{"keys": [_public_jwk(public, "a")]}])
        claims = await verify_id_token(
            client,
            _sign(private, "a", nbf=soon, iat=soon),
            _disco(),
            client_id=CLIENT_ID,
        )
        assert claims["sub"] == "user-1"


class TestJWKSHandling:
    @pytest.mark.asyncio
    async def test_jwks_is_cached_between_verifications(self, key_a):
        private, public = key_a
        client = _CountingClient([{"keys": [_public_jwk(public, "a")]}])
        await verify_id_token(client, _sign(private, "a"), _disco(), client_id=CLIENT_ID)
        await verify_id_token(client, _sign(private, "a"), _disco(), client_id=CLIENT_ID)
        assert client.fetches == 1, "the issuer's JWKS was refetched for every login"

    @pytest.mark.asyncio
    async def test_unknown_kid_forces_one_refetch(self, key_a, key_b):
        """Key rotation: the cached set predates the key that signed this token."""
        private_b, public_b = key_b
        _, public_a = key_a
        client = _CountingClient(
            [
                {"keys": [_public_jwk(public_a, "a")]},
                {"keys": [_public_jwk(public_a, "a"), _public_jwk(public_b, "b")]},
            ]
        )
        # Warm the cache on the old key set.
        await verify_id_token(client, _sign(key_a[0], "a"), _disco(), client_id=CLIENT_ID)
        assert client.fetches == 1

        claims = await verify_id_token(client, _sign(private_b, "b"), _disco(), client_id=CLIENT_ID)
        assert claims["sub"] == "user-1"
        assert client.fetches == 2, "a rotated key must trigger exactly one refetch"

    @pytest.mark.asyncio
    async def test_kid_absent_from_a_refetched_set_is_rejected(self, key_a, key_b):
        """One refetch, then give up — never a fetch per attempt."""
        private_b, _ = key_b
        _, public_a = key_a
        client = _CountingClient([{"keys": [_public_jwk(public_a, "a")]}])
        with pytest.raises(IdTokenError):
            await verify_id_token(client, _sign(private_b, "zzz"), _disco(), client_id=CLIENT_ID)
        assert client.fetches <= 2

    @pytest.mark.asyncio
    async def test_missing_jwks_uri_is_rejected(self, key_a):
        private, _ = key_a
        client = _CountingClient([{"keys": []}])
        disco = _disco()
        del disco["jwks_uri"]
        with pytest.raises(IdTokenError):
            await verify_id_token(client, _sign(private, "a"), disco, client_id=CLIENT_ID)

    @pytest.mark.asyncio
    async def test_cache_expires(self, key_a, monkeypatch):
        private, public = key_a
        client = _CountingClient([{"keys": [_public_jwk(public, "a")]}])
        await verify_id_token(client, _sign(private, "a"), _disco(), client_id=CLIENT_ID)

        real = time.monotonic
        monkeypatch.setattr(
            oidc_token.time, "monotonic", lambda: real() + oidc_token._JWKS_TTL_SECONDS + 1
        )
        await verify_id_token(client, _sign(private, "a"), _disco(), client_id=CLIENT_ID)
        assert client.fetches == 2


# ---------------------------------------------------------------------------
# The branch this exists to make safe
# ---------------------------------------------------------------------------
class TestOidcExchangeFallback:
    """``_oidc_exchange`` reports the id_token path as verified only now that it is."""

    @staticmethod
    def _patch(monkeypatch, disco: dict, tokens: dict, jwks: dict):
        import datanika.services.sso_routes as sso_routes

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                return False

            async def get(self, url, **_kw):
                if "openid-configuration" in url:
                    return _FakeResponse(disco)
                if url == JWKS_URI:
                    return _FakeResponse(jwks)
                raise AssertionError(f"unrouted GET {url}")

            async def post(self, _url, **_kw):
                return _FakeResponse(tokens)

        import httpx

        monkeypatch.setattr(httpx, "AsyncClient", lambda *_a, **_k: _Client())
        return sso_routes

    @pytest.mark.asyncio
    async def test_valid_id_token_reports_verified(self, key_a, monkeypatch):
        private, public = key_a
        # No userinfo_endpoint -> the fallback is the only path available.
        sso_routes = self._patch(
            monkeypatch,
            _disco(),
            {"id_token": _sign(private, "a")},
            {"keys": [_public_jwk(public, "a")]},
        )
        request = MagicMock()
        request.query_params = {"code": "abc"}
        sso = MagicMock(oidc_issuer_url=ISSUER, oidc_client_id=CLIENT_ID)
        svc = MagicMock()
        svc.get_oidc_client_secret.return_value = "secret"

        email, name, verified = await sso_routes._oidc_exchange(request, sso, svc)
        assert (email, name) == ("person@example.com", "A Person")
        assert verified is True

    @pytest.mark.asyncio
    async def test_forged_id_token_never_reports_verified(self, key_a, key_b, monkeypatch):
        _, public_a = key_a
        private_b, _ = key_b
        sso_routes = self._patch(
            monkeypatch,
            _disco(),
            {"id_token": _sign(private_b, "a")},
            {"keys": [_public_jwk(public_a, "a")]},
        )
        request = MagicMock()
        request.query_params = {"code": "abc"}
        sso = MagicMock(oidc_issuer_url=ISSUER, oidc_client_id=CLIENT_ID)
        svc = MagicMock()
        svc.get_oidc_client_secret.return_value = "secret"

        email, _name, verified = await sso_routes._oidc_exchange(request, sso, svc)
        assert verified is False, (
            "a token whose signature does not check out must never be reported "
            "as verified — find_or_create_oauth_user decides on this boolean"
        )
        assert not email, "no email should be carried out of a failed verification"

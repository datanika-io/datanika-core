"""Fetch an OpenAPI spec from a user-supplied URL (core#410, OpenAPI P2).

Kept out of ``openapi_import.py`` on purpose: that module is deterministic and
touches no network, which is what makes it cheap to fuzz and reason about. This
module is the *only* place the spec path talks to the outside world, so all of
SPEC_OPENAPI_CONNECTOR §7's guards live here and nowhere else:

  * **https only** — stricter than ``validate_egress_host`` (which allows http
    for connector base URLs), because a spec URL has no reason to be plaintext;
  * **host must resolve public** — the same ``validate_egress_host`` the
    connector family uses, not a second implementation (§7: "reuse/centralize
    with any existing outbound-fetch guard");
  * **every redirect hop re-checked** — via the guarded session from core#403,
    so a public host cannot bounce us into ``10.0.0.0/8``;
  * **10 s timeout** and a **5 MB cap enforced while streaming** — applied to
    bytes actually read, never to a header or a finished body. The case that
    needs it is a response with **no ``Content-Length``**, delimited by
    connection close: urllib3 then reads until EOF and nothing but this cap
    bounds it. (An *understated* ``Content-Length`` is not the danger — urllib3
    stops at the declared length and truncates. Measured, not assumed.)

**Inherited residual:** core#405 (DNS-rebinding TOCTOU) applies here too, and
bites slightly harder than it does in the worker — this runs in the web tier,
where loopback reaches the app's own API and, compose-internally, Postgres and
Redis. §7 does not require closing it and §13.1 signed off "URL in P2 behind §7
guards", so it ships knowingly, not unnoticed.
"""

from __future__ import annotations

from urllib.parse import urlparse

from datanika.services.egress_guard import (
    EgressValidationError,
    build_guarded_session,
    validate_egress_host,
)
from datanika.services.openapi_import import MAX_SPEC_BYTES, OpenApiImportError

FETCH_TIMEOUT_SECONDS = 10
_CHUNK_BYTES = 64_000

#: §7: spec URLs are https-only. A module constant rather than a literal so the
#: streaming/size tests can point at a plaintext local server without weakening
#: the rule itself (which has its own tests against the real code path).
REQUIRED_SCHEME = "https"


def fetch_openapi_spec(url: str) -> str:
    """Return the spec document at *url* as text.

    Raises :class:`OpenApiImportError` with ``code`` ∈ {``invalid_spec_url``,
    ``spec_too_large``, ``spec_fetch_failed``} so the endpoint can branch
    without matching on messages — the same contract the parser already uses.
    """
    parsed = urlparse(url or "")
    if parsed.scheme != REQUIRED_SCHEME:
        raise OpenApiImportError(
            "Spec URL must use https (paste the document instead if the host is http-only).",
            "invalid_spec_url",
        )

    session = build_guarded_session()
    try:
        # The guarded session validates this URL and every redirect hop anyway;
        # calling it explicitly first means an internal host is refused before a
        # socket is opened, and surfaces as a URL problem rather than a generic
        # fetch failure.
        validate_egress_host(url)
        with session.get(url, timeout=FETCH_TIMEOUT_SECONDS, stream=True) as response:
            response.raise_for_status()
            return _read_capped(response)
    except EgressValidationError as exc:
        raise OpenApiImportError(str(exc), "invalid_spec_url") from exc
    except OpenApiImportError:
        raise
    except Exception as exc:  # network/TLS/HTTP status — one typed failure
        raise OpenApiImportError(f"Could not fetch the spec: {exc}", "spec_fetch_failed") from exc
    finally:
        session.close()


def resolve_spec(spec_inline, spec_url) -> str:
    """Return the spec text from whichever source the caller supplied.

    Lives here rather than in the route so the "exactly one of" decision is
    unit-testable: the endpoint is decorated for auth and cannot be called
    directly, which is how this branch would otherwise ship untested.
    """
    if bool(spec_inline) == bool(spec_url):
        # Both or neither — ambiguous rather than merely incomplete, so the
        # message names both options instead of demanding one.
        raise OpenApiImportError(
            "Provide exactly one of spec_inline (the document) or spec_url (where to fetch it).",
            "invalid_request",
        )
    if spec_url is not None and spec_url != "":
        if not isinstance(spec_url, str):
            raise OpenApiImportError("spec_url must be a string", "invalid_request")
        return fetch_openapi_spec(spec_url)
    if not isinstance(spec_inline, str):
        raise OpenApiImportError(
            "spec_inline (the OpenAPI 3.x document as a string) is required", "invalid_request"
        )
    return spec_inline


def _read_capped(response) -> str:
    """Read at most ``MAX_SPEC_BYTES``, aborting as soon as the cap is passed."""
    chunks: list[bytes] = []
    total = 0
    for chunk in response.iter_content(chunk_size=_CHUNK_BYTES):
        if not chunk:
            continue
        total += len(chunk)
        if total > MAX_SPEC_BYTES:
            # Stop pulling immediately — do not buffer the rest to measure it.
            raise OpenApiImportError("Spec exceeds the size limit", "spec_too_large")
        chunks.append(chunk)

    body = b"".join(chunks)
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise OpenApiImportError("Spec is not valid UTF-8 text", "invalid_spec") from exc

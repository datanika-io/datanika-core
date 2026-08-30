"""Establishing the real client IP, or admitting we cannot (core#623, D5).

Production is Cloudflare → Apache → ``127.0.0.1:8000``. So the socket peer is
always loopback, and any per-IP limiter that reads it collapses the entire
internet into one bucket: the tenth password-reset request *from anyone* locks
out *everyone*. That failure cannot reproduce in dev, where there is no proxy
and the limiter looks like it works.

Reflex's own ``router.session.client_ip`` is not usable either. It takes the
**leftmost** ``X-Forwarded-For`` entry, which is whatever the client chose to
send — so an attacker rotates it per request and is never limited, while an
innocent user who happens to collide with a forged value is.

This module answers one question: **can we name this client with confidence?**
When the answer is no it returns ``""`` and the caller skips the IP bucket
entirely, because a limiter keyed on the wrong thing is worse than no limiter.

The awkward case is a multi-hop chain with no ``CF-Connecting-IP``. Neither end
of the chain is safe to take: the leftmost is client-supplied, and the last hop
is whatever machine connected to our Apache — which, in our own production
shape, is the Cloudflare *edge*, a handful of addresses shared by every visitor.
Bucketing on that is the global lockout by another route. Deciding correctly
would need a list of trusted proxies we do not maintain, so this refuses.
"""

import ipaddress
from collections.abc import Mapping


def _usable(candidate: str) -> str:
    """Return ``candidate`` if it is an address that identifies a client.

    Private ranges are kept: a self-hosted instance on a LAN sees only
    ``192.168.x.x``, and those are genuinely distinct clients. Loopback and
    ``0.0.0.0`` are the ones that mean "I am looking at the proxy, not the
    caller".
    """
    candidate = (candidate or "").strip()
    if not candidate:
        return ""
    try:
        parsed = ipaddress.ip_address(candidate)
    except ValueError:
        return ""
    if parsed.is_loopback or parsed.is_unspecified:
        return ""
    return candidate


def resolve_client_ip(headers: Mapping[str, str]) -> str:
    """Best trustworthy client address, or ``""`` when there is not one."""
    lowered = {str(k).lower(): v for k, v in (headers or {}).items()}

    # 1. Cloudflare's own header. A client cannot set it: the edge overwrites
    #    it on every proxied request, so it is the authoritative value in prod.
    cf = _usable(lowered.get("cf-connecting-ip", ""))
    if cf:
        return cf

    # 2. Exactly one X-Forwarded-For entry means one trusted proxy in front of
    #    the app, and that entry is its own observation of the peer.
    forwarded = [part.strip() for part in lowered.get("x-forwarded-for", "").split(",")]
    forwarded = [part for part in forwarded if part]
    if len(forwarded) == 1:
        return _usable(forwarded[0])
    if len(forwarded) > 1:
        # Ambiguous — see the module docstring. Refusing is the safe answer.
        return ""

    # 3. No proxy at all: the socket peer is the client.
    return _usable(lowered.get("asgi-scope-client", ""))

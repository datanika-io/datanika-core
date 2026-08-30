"""Resolving the real client IP behind Cloudflare → Apache (SPEC_PASSWORD_RESET D5).

``request.client.host`` is ``127.0.0.1`` for every request in production —
traffic arrives Cloudflare → Apache → ``127.0.0.1:8000``. A per-IP limiter that
reads it collapses the entire internet into one bucket, so the tenth
password-reset request *from anyone* locks out *everyone*: a global outage that
cannot reproduce in dev.

Reflex's own ``router.session.client_ip`` is not usable either — it takes the
**leftmost** ``X-Forwarded-For`` entry, which is whatever the client sent.

So this module answers one question: *can we name this client with confidence?*
When the answer is no it returns ``""`` and the caller skips the IP bucket. A
limiter keyed on the wrong thing is worse than no limiter.
"""

from datanika.services.client_ip import resolve_client_ip


class TestCloudflare:
    def test_cf_connecting_ip_wins(self):
        assert (
            resolve_client_ip(
                {
                    "cf-connecting-ip": "203.0.113.7",
                    "x-forwarded-for": "203.0.113.7, 172.71.0.1",
                    "asgi-scope-client": "127.0.0.1",
                }
            )
            == "203.0.113.7"
        )

    def test_cf_connecting_ip_is_preferred_over_a_spoofed_xff(self):
        """A client can write anything into XFF; it cannot write CF-Connecting-IP."""
        assert (
            resolve_client_ip(
                {
                    "cf-connecting-ip": "203.0.113.7",
                    "x-forwarded-for": "1.2.3.4, 203.0.113.7, 172.71.0.1",
                }
            )
            == "203.0.113.7"
        )

    def test_header_lookup_is_case_insensitive(self):
        assert resolve_client_ip({"CF-Connecting-IP": "203.0.113.7"}) == "203.0.113.7"

    def test_ipv6_is_accepted(self):
        assert resolve_client_ip({"cf-connecting-ip": "2001:db8::1"}) == "2001:db8::1"

    def test_a_malformed_cf_header_falls_through(self):
        assert resolve_client_ip({"cf-connecting-ip": "not-an-ip"}) == ""


class TestSingleReverseProxy:
    """One trusted proxy in front of the app: the single XFF entry is its own
    observation of the peer, so it is the client."""

    def test_one_entry_is_used(self):
        assert resolve_client_ip({"x-forwarded-for": "198.51.100.9"}) == "198.51.100.9"

    def test_whitespace_is_stripped(self):
        assert resolve_client_ip({"x-forwarded-for": "  198.51.100.9  "}) == "198.51.100.9"

    def test_a_lan_client_is_a_real_client(self):
        """A self-hosted instance on a LAN sees only private addresses, and they
        are still distinct clients. Only loopback means 'I am seeing the proxy'."""
        assert resolve_client_ip({"x-forwarded-for": "192.168.1.50"}) == "192.168.1.50"


class TestAmbiguousChains:
    def test_a_multi_hop_chain_without_cf_is_refused(self):
        """Two or more hops and no CF header: we cannot say which entry is the
        client without a trusted-proxy list we do not have.

        Taking the leftmost trusts the client. Taking the last would, in our own
        production shape, yield the Cloudflare *edge* address — a handful of IPs
        shared by everyone, which is the global-lockout bucket this whole module
        exists to avoid. Refusing is the only safe answer.
        """
        assert resolve_client_ip({"x-forwarded-for": "1.2.3.4, 172.71.0.1"}) == ""

    def test_three_hops_are_refused_too(self):
        assert resolve_client_ip({"x-forwarded-for": "1.2.3.4, 5.6.7.8, 172.71.0.1"}) == ""

    def test_a_single_malformed_entry_is_refused(self):
        assert resolve_client_ip({"x-forwarded-for": "garbage"}) == ""


class TestDirectPeer:
    def test_a_public_peer_is_used_when_there_is_no_proxy(self):
        assert resolve_client_ip({"asgi-scope-client": "198.51.100.9"}) == "198.51.100.9"

    def test_loopback_is_refused(self):
        """This is the production value, and the reason the whole guard exists."""
        assert resolve_client_ip({"asgi-scope-client": "127.0.0.1"}) == ""

    def test_ipv6_loopback_is_refused(self):
        assert resolve_client_ip({"asgi-scope-client": "::1"}) == ""

    def test_unspecified_is_refused(self):
        assert resolve_client_ip({"asgi-scope-client": "0.0.0.0"}) == ""

    def test_no_headers_at_all_is_refused(self):
        assert resolve_client_ip({}) == ""


class TestProductionShape:
    def test_the_real_prod_request_resolves_to_the_visitor(self):
        """Cloudflare → Apache → 127.0.0.1:8000, as the box actually serves it."""
        headers = {
            "host": "app.datanika.io",
            "cf-connecting-ip": "203.0.113.7",
            "cf-ipcountry": "GR",
            "x-forwarded-for": "203.0.113.7, 172.71.18.4",
            "x-forwarded-host": "app.datanika.io",
            "asgi-scope-client": "127.0.0.1",
        }
        assert resolve_client_ip(headers) == "203.0.113.7"

    def test_the_dev_shape_resolves_to_nothing(self):
        """Local dev has no proxy at all, so the IP bucket simply does not engage
        — which is why an IP limiter that looks fine locally can still be a
        global lockout in prod."""
        assert resolve_client_ip({"host": "localhost:3000", "asgi-scope-client": "127.0.0.1"}) == ""

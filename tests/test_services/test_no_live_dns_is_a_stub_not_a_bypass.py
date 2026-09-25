"""Controls for the resolver this directory owns (core#1597, core#1280 before it).

``conftest.py`` makes resolution in ``tests/test_services`` either **stubbed** (a test
requested ``no_live_dns``) or **refused**. Never the network. These are the controls for
that, and they live beside the fixture rather than beside one of its callers — core#1280
put them beside one caller, and when the same defect turned up in three more modules the
controls were nowhere near the place a reader would look.

**Two failure modes, pointing opposite ways, and both have to be held:**

1. **The stub becomes a guard bypass.** The tempting simplification is to patch
   ``validate_egress_host`` itself. Every test stays green while the SSRF control is off
   for the whole directory, and nothing says so — a guard is inert if anything upstream
   already answers (``ENGINEERING_RULES`` §57, core#896). Two modules here *do* relax the
   guard deliberately, for local-vendor servers the guard must refuse by design, and they
   say so in their own fixtures; what must not happen is the **shared** fixture doing it.
2. **The refusal becomes inert.** A denier that never fires, or a fixture that fails to
   patch, hands back exactly today's behaviour — which is the defect being fixed, one
   level up.

⚠️ **This module deliberately does NOT request ``no_live_dns``** except where a test says
so, because half of what is asserted here is the *refusing* mode.
"""

from __future__ import annotations

import socket

import pytest

from datanika.services import egress_guard
from datanika.services.dlt_runner import DltRunnerService
from datanika.services.egress_guard import EgressValidationError

# Plain helper functions, not fixtures — `tests/test_fixture_sharing.py` bans importing a
# FIXTURE across test modules (pytest registers a second FixtureDef) and explicitly allows
# this. Absolute form, matching `tests/test_migrations/`'s existing conftest imports.
from tests.test_services.conftest import _gai, _needs_no_lookup


class TestTheStubReplacedTheNetworkNotTheGuard:
    """The invariant from core#1597 AC1, stated as behaviour rather than as a docstring."""

    def test_a_private_address_is_still_refused_under_the_stub(self, no_live_dns, monkeypatch):
        """Patching the RESOLVER keeps the guard's classification running.

        Hand the stubbed resolver a private address and ``validate_egress_host`` must
        still refuse it. If this ever passes by *not* raising, the fixture has stopped
        being a network stub and become an egress bypass.
        """
        monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: _gai("10.0.0.5"))
        with pytest.raises(EgressValidationError):
            egress_guard.validate_egress_host("https://internal.example.test/x")

    def test_the_guard_itself_is_never_patched_by_the_shared_fixture(self, no_live_dns):
        """``validate_egress_host`` must be the real function while the stub is active.

        The positive form (§4: assert the presence of the right thing). A bypass would
        leave a lambda or a Mock here, and asserting "no lambda" would be satisfied by
        deleting the assertion.
        """
        assert egress_guard.validate_egress_host.__module__ == "datanika.services.egress_guard"
        assert egress_guard.validate_egress_host.__name__ == "validate_egress_host"

    def test_a_public_address_is_allowed_so_the_classification_really_ran(self, no_live_dns):
        """Anti-vacuity for the test above: the guard must be capable of *passing* too.

        A guard that refused everything would satisfy the private-address test while
        making every build fail, so both directions are needed.
        """
        egress_guard.validate_egress_host("https://anything.example.test/x")
        assert no_live_dns == ["anything.example.test"], no_live_dns


class TestTheRefusalFiresAndNamesDns:
    """The denier's own controls. No ``no_live_dns`` here — refusing is the subject."""

    def test_an_unstubbed_lookup_is_refused(self):
        with pytest.raises(socket.gaierror):
            socket.getaddrinfo("graph.facebook.com", None)

    def test_the_refusal_names_dns_and_the_issue_rather_than_a_connector(self):
        """core#1597's defect 3: the old failure named the wrong layer.

        It surfaced as a connector/endpoint assertion, so the reader started by looking
        for a connector defect that was not there — twice, a year apart. The message has
        to say what it is and what to do.
        """
        with pytest.raises(socket.gaierror) as exc:
            socket.getaddrinfo("graph.facebook.com", None)
        text = str(exc.value)
        assert "live DNS is disabled" in text, text
        assert "core#1597" in text, text
        assert "no_live_dns" in text, text
        assert "NOT a connector defect" in text, text

    def test_the_refusal_survives_the_egress_guard_wrapping_it(self):
        """``validate_egress_host`` turns ``gaierror`` into ``EgressValidationError``.

        That wrapping is where the explanation could be lost, so assert it arrives at the
        layer a test author actually reads.
        """
        with pytest.raises(EgressValidationError) as exc:
            egress_guard.validate_egress_host("https://graph.facebook.com/v1/x")
        assert "core#1597" in str(exc.value), str(exc.value)


class TestTheCarveOutIsNarrow:
    """Loopback and IP literals pass through. Nothing else does."""

    @pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1", "10.0.0.5", "8.8.8.8"])
    def test_no_lookup_needed(self, host):
        assert _needs_no_lookup(host) is True

    @pytest.mark.parametrize(
        "host",
        [
            "graph.facebook.com",
            "app.asana.com",
            # 🚨 The shapes a substring or prefix test would wave through. `localhost.`
            # and the embedded-literal forms are the classic SSRF decoys.
            "localhost.attacker.example",
            "127.0.0.1.attacker.example",
            "not-localhost",
        ],
    )
    def test_a_lookup_is_needed_and_therefore_refused(self, host):
        assert _needs_no_lookup(host) is False

    def test_loopback_really_reaches_the_real_resolver(self):
        """Not just that the predicate says so — that the resolver honours it.

        The local-vendor tests in this directory bind real HTTP servers to loopback, so a
        denier that refused `localhost` would break them in a way that looks like a
        product defect.
        """
        assert socket.getaddrinfo("127.0.0.1", 0)


class TestTheStubIsOnTheHotPath:
    """Anti-vacuity: the fixture must be protecting something that actually resolves."""

    def test_building_a_source_goes_through_the_stubbed_resolver(self, no_live_dns, tmp_path):
        """If ``build_source`` stopped resolving hosts, the fixture would still pass its
        own in-effect probe (which calls the guard directly) while protecting nothing.
        """
        DltRunnerService(pipelines_dir=str(tmp_path)).build_source(
            "freshdesk", {"api_key": "t", "domain": "d"}, {}
        )
        assert no_live_dns, (
            "building a source resolved no host, so `no_live_dns` is protecting nothing. "
            "Either the egress guard left the build path — in which case the four modules "
            "that request this fixture no longer need it — or the stub is not reaching it."
        )

    def test_an_unstubbed_build_is_refused_rather_than_reaching_the_network(self, tmp_path):
        """The other half, and the one that makes this a guard rather than a convenience.

        A module added to this directory that builds a source without requesting the
        fixture fails immediately, with the message above, instead of silently doing live
        lookups and going green in CI while red on somebody's machine. **This is the
        assertion that covers module five**, which is how core#1280's per-module fix came
        to leave three modules unfixed (``WORKFLOW_RULES`` §5a: the guard asserts the
        invariant, not today's three instances).
        """
        with pytest.raises(EgressValidationError) as exc:
            DltRunnerService(pipelines_dir=str(tmp_path)).build_source(
                "freshdesk", {"api_key": "t", "domain": "d"}, {}
            )
        assert "core#1597" in str(exc.value), str(exc.value)

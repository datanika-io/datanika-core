"""A reserved host port is moved WITHIN the band, so the stack actually starts (core#1481).

## Why refusing cleanly was not enough

The pre-flight in this repo turns a half-started stack into an honest refusal. That is right and
it is not the same as working: Product's band frontend ports (53000, 53001, 53010) sit inside a
Windows reserved range, so every `master`-built walk stays blocked —
`SPEC_CONNECTOR_GUIDE_VERIFICATION` §2.4 needs such a stack, and three capture tasks are behind it.

`--port-offset` cannot help: it takes a multiple of 10000, i.e. it selects **another department's
whole band**, which is the cross-agent collision the isolation exists to prevent.

## The rule, and the two halves that are not symmetric

A port is relocated **only when the OS refuses it outright** (`EACCES` — a reserved range).

⚠️ **A port that is merely IN USE is deliberately left where it is.** It is very often *this stack,
already up*, and moving it would give a running stack a new set of ports on every `up` — breaking
the re-`up` the pre-flight was careful to preserve. The asymmetry is the design, not an oversight.

## Robust to the range MOVING, not to today's values

WinNAT's reserved ranges move between reboots — and measured 2026-09-21, they moved **inside a
single session**: 53064 read bindable when the ranges were first listed and RESERVED a few hours
later. Nothing here encodes a range; the allocator asks the host every run through an injected
probe. `test_a_different_reserved_set_gives_a_different_but_valid_map` is the arm that pins that
property — if the ranges were baked in it would still pass against today's values and fail there.

## Why the probe is injected

A test binding real sockets would never execute the reserved-port arm on a runner where every port
is free: green everywhere, unable to fail on the one host it exists for. The real binder is
exercised separately in `test_worktree_stack_port_preflight.py`.
"""

from __future__ import annotations

import errno
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "scripts" / "worktree_stack.py"

_spec = importlib.util.spec_from_file_location("worktree_stack", MODULE)
assert _spec and _spec.loader, f"cannot load {MODULE}"
ws = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ws)

AGENT = "product"
OFFSET = ws.OFFSETS[AGENT]  # 50_000


def _port(published: int, target: int) -> dict:
    return {
        "mode": "ingress",
        "host_ip": "127.0.0.1",
        "target": target,
        "published": str(published),
        "protocol": "tcp",
    }


#: The real shape, trimmed: a Reflex server, its twin, and two datastores.
BASE = {
    "name": "datanika-core-product",
    "services": {
        "app": {
            "container_name": "datanika-app",
            "image": f"{ws.IMAGE_REPO}:worktree-{AGENT}",
            "ports": [_port(3000, 3000), _port(8000, 8000)],
        },
        "app_b": {
            "container_name": "datanika-app-b",
            "image": f"{ws.IMAGE_REPO}:worktree-{AGENT}",
            "ports": [_port(3010, 3000), _port(8010, 8000)],
        },
        "postgres": {
            "container_name": "datanika-postgres",
            "image": "postgres:16-alpine",
            "ports": [_port(5432, 5432)],
        },
        "proxy": {
            "container_name": "datanika-local-proxy",
            "image": "nginx:1.27-alpine",
            "ports": [_port(3100, 80)],
        },
    },
}

#: What Windows reported on 2026-09-21. Used as a REALISTIC case, never as a constant the
#: allocator knows about.
MEASURED_RANGES = [(52963, 53062), (53152, 53251), (53252, 53351), (53488, 53587)]


def probe_for(ranges, in_use=()):
    """A stand-in binder: EACCES inside any range, EADDRINUSE for `in_use`, else bindable."""

    def probe(host_ip: str, port: int):
        if port in in_use:
            return errno.EADDRINUSE
        for low, high in ranges:
            if low <= port <= high:
                return errno.EACCES
        return None

    return probe


def natural_ports(base=BASE, offset=OFFSET) -> set[int]:
    return {
        int(p["published"]) + offset
        for service in base["services"].values()
        for p in ws._published(service)
    }


class TestNothingMovesWhenNothingNeedsTo:
    def test_an_unreserved_host_maps_every_port_to_itself(self):
        mapping = ws.allocate_ports(BASE, OFFSET, probe=probe_for([]))
        assert mapping == {p: p for p in natural_ports()}

    def test_a_port_merely_in_use_is_left_alone(self):
        """The control that decides the design.

        53000 in use is very often THIS stack, already up. Moving it would hand a running
        stack a new port on every `up`.
        """
        mapping = ws.allocate_ports(BASE, OFFSET, probe=probe_for([], in_use={53000}))
        assert mapping[53000] == 53000


class TestAReservedPortMovesWithinTheBand:
    def test_the_reserved_port_moves_and_the_others_do_not(self):
        mapping = ws.allocate_ports(BASE, OFFSET, probe=probe_for(MEASURED_RANGES))
        # 53000, 53001 (not in this base), 53010 are inside 52963-53062.
        assert mapping[53000] != 53000
        assert mapping[53010] != 53010
        # Everything outside a reserved range is untouched.
        assert mapping[58000] == 58000
        assert mapping[55432] == 55432
        assert mapping[53100] == 53100

    def test_every_resolved_port_is_bindable(self):
        probe = probe_for(MEASURED_RANGES)
        mapping = ws.allocate_ports(BASE, OFFSET, probe=probe)
        for natural, resolved in mapping.items():
            assert probe("127.0.0.1", resolved) is None, (
                f"{natural} was moved to {resolved}, which the host also refuses"
            )

    def test_every_resolved_port_stays_inside_the_band(self):
        """Leaving the band is the cross-department collision this whole module prevents."""
        mapping = ws.allocate_ports(BASE, OFFSET, probe=probe_for(MEASURED_RANGES))
        for resolved in mapping.values():
            assert OFFSET <= resolved < OFFSET + ws.BAND, resolved

    def test_no_two_ports_resolve_to_the_same_host_port(self):
        mapping = ws.allocate_ports(BASE, OFFSET, probe=probe_for(MEASURED_RANGES))
        assert len(set(mapping.values())) == len(mapping)

    def test_a_relocation_never_lands_on_another_service_s_natural_port(self):
        """A candidate that is free *now* because its owner has not started yet is not free.

        Reserving 53100's neighbourhood forces a search that would otherwise be tempted by
        53100 itself, which `proxy` is about to take.
        """
        probe = probe_for([(53000, 53099)])
        mapping = ws.allocate_ports(BASE, OFFSET, probe=probe)
        assert mapping[53100] == 53100, "proxy's own port should not have moved"
        others = {n: r for n, r in mapping.items() if n != 53100}
        assert 53100 not in others.values()

    def test_it_is_deterministic(self):
        first = ws.allocate_ports(BASE, OFFSET, probe=probe_for(MEASURED_RANGES))
        second = ws.allocate_ports(BASE, OFFSET, probe=probe_for(MEASURED_RANGES))
        assert first == second


class TestRobustToTheRangeMoving:
    def test_a_different_reserved_set_gives_a_different_but_valid_map(self):
        """The property the coordinator asked for: nothing encodes today's ranges.

        If the allocator hardcoded 52963-53062 it would still "work" on the measured set
        and fail here.
        """
        moved = [(58000, 58099), (55400, 55499)]
        probe = probe_for(moved)
        mapping = ws.allocate_ports(BASE, OFFSET, probe=probe)
        assert mapping[58000] != 58000, "a range that moved onto the BACKEND port was ignored"
        assert mapping[55432] != 55432
        assert mapping[53000] == 53000, "the old range is no longer reserved, so nothing moves"
        for resolved in mapping.values():
            assert probe("127.0.0.1", resolved) is None

    def test_the_band_being_almost_entirely_reserved_still_resolves(self):
        """A pathological host: only a narrow window is bindable."""
        probe = probe_for([(OFFSET, OFFSET + ws.BAND - 21)])
        mapping = ws.allocate_ports(BASE, OFFSET, probe=probe)
        assert len(set(mapping.values())) == len(mapping)
        for resolved in mapping.values():
            assert probe("127.0.0.1", resolved) is None


class TestItFailsClosedRatherThanGuessing:
    def test_a_fully_reserved_band_refuses_and_names_the_port(self):
        probe = probe_for([(OFFSET, OFFSET + ws.BAND - 1)])
        with pytest.raises(ws.RefusalError) as exc:
            ws.allocate_ports(BASE, OFFSET, probe=probe)
        message = str(exc.value)
        assert "53000" in message or "no bindable host port" in message
        assert str(AGENT) in message or "band" in message


class TestTheOverlayCarriesTheResolvedPorts:
    """The load-bearing integration property.

    A relocation that did not reach `REFLEX_API_URL` would recreate core#1197's original
    defect from the other direction: the browser sent to a port nothing is listening on.
    """

    def test_reflex_urls_use_the_resolved_ports_not_the_natural_ones(self):
        probe = probe_for(MEASURED_RANGES)
        mapping = ws.allocate_ports(BASE, OFFSET, probe=probe)
        text = ws.overlay(BASE, AGENT, OFFSET, probe=probe)

        assert f'REFLEX_API_URL: "http://localhost:{mapping[58000]}"' in text
        assert f'REFLEX_DEPLOY_URL: "http://localhost:{mapping[53000]}"' in text
        assert f'"127.0.0.1:{mapping[53000]}:3000"' in text
        assert "53000:3000" not in text, "the reserved port is still published"

    def test_without_a_probe_the_overlay_is_unchanged(self):
        """Back-compat: the pure form still produces exactly the old mapping, so every
        existing caller and test keeps its meaning."""
        text = ws.overlay(BASE, AGENT, OFFSET)
        assert '"127.0.0.1:53000:3000"' in text
        assert 'REFLEX_API_URL: "http://localhost:58000"' in text

"""core#719 item 1 — the cross-tenant boundary table must not silently fall behind the routes.

``tests/test_security/test_tenant_jwt_boundary.py`` parameterises over ``MUTATION_ROUTES``,
a hand-written list. ``datanika/services/api_v1_routes.py`` declares the real surface.
Nothing compared the two, so a new mutating endpoint could ship with no cross-tenant
coverage while the suite stayed green and said nothing — *"a green that would look
identical had the thing failed"*.

## What the census found (2026-09-02, QA; re-derived 2026-09-03 before shipping)

**Zero drift.** 35 real mutating routes; 25 carry a path id and all 25 are pinned; no stale
rows. The table was exact. This guard is therefore green on its first day, which is the
right time to add one — it locks in a correct state rather than papering over a broken one.

⚠️ **A guard that is green on arrival is exactly the one to distrust**, so it was not
trusted on that reading. Neither ``api_v1_routes.py`` nor the boundary table changed between
the census and this commit (checked, not assumed), and the guard was additionally **forced
red against the real file** — a throwaway ``DELETE /api/v1/widgets/{id:int}`` was added to
``api_v1_routes.py`` itself, the guard named that exact route, and the file was restored
byte-identically. ``test_an_injected_unpinned_route_is_detected`` keeps an in-process form of
that proof in the suite permanently, so it survives the one-off manual step nobody repeats.

## 🚨 The matcher, which is the whole difficulty

The table writes ``/api/v1/connections/{id}``. Starlette declares
``/api/v1/connections/{id:int}``. **Without normalising the path converter the census
reports all 25 routes as unguarded** — a maximally alarming and completely false result on
the security boundary. That was measured, not imagined:

    without {id:int} normalisation -> 25 "missing"
    with it                        -> 0

``test_the_normaliser_is_load_bearing`` pins it, so a future edit that drops the
normalisation fails *here* rather than emitting 25 phantom findings.

## Scope

This guard closes the **arithmetic** only — every path-id mutating route is pinned. It does
not close the *shape*: 10 mutating routes take no path id (creates, bulk import) and are
outside the table's threat model by construction. That class is covered by
``tests/test_security/test_tenant_fk_boundary.py``; the bulk-import pair was separately
verified to be safe by construction (the payload has no ``*_connection_id`` field at all —
references are by name, resolved from an org-scoped list). See core#719.
"""

from __future__ import annotations

import re

import pytest

from datanika.services import api_v1_routes as routes_module
from tests.test_security.test_tenant_jwt_boundary import MUTATION_ROUTES

MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

# Routes that mutate but legitimately take no org-owned resource id in the path.
# Cross-tenant reachability for these is covered by test_tenant_fk_boundary.py.
# Entries are asserted to still match something, so one cannot outlive its reason.
NO_PATH_ID_ALLOWLIST = {
    ("POST", "/api/v1/connections"),
    ("POST", "/api/v1/connections/openapi/parse"),
    ("POST", "/api/v1/import"),
    ("POST", "/api/v1/notifications/channels"),
    ("POST", "/api/v1/notifications/read-all"),
    ("POST", "/api/v1/pipelines"),
    ("POST", "/api/v1/pipelines/yaml"),
    ("POST", "/api/v1/schedules"),
    ("POST", "/api/v1/transformations"),
    ("POST", "/api/v1/uploads"),
}

_CONVERTER = re.compile(r"\{(\w+):[^}]+\}")


def _norm(path: str) -> str:
    """``/x/{id:int}`` -> ``/x/{id}``. See the module docstring — load-bearing."""
    return _CONVERTER.sub(r"{\1}", path)


def _real_mutating_routes() -> set[tuple[str, str]]:
    """Enumerate the routes the app actually mounts, from the module it mounts them from."""
    found: set[tuple[str, str]] = set()
    for value in vars(routes_module).values():
        if not isinstance(value, list) or not value:
            continue
        if value[0].__class__.__name__ != "Route":
            continue
        for route in value:
            for method in route.methods or ():
                if method in MUTATING_METHODS:
                    found.add((method, _norm(route.path)))
    return found


def _table_routes() -> set[tuple[str, str]]:
    return {(method, _norm(path)) for method, path, _resource, _body in MUTATION_ROUTES}


class TestBoundaryTableCoversEveryPathIdMutation:
    def test_the_route_scan_is_armed(self) -> None:
        """Without this, every assertion below is about an empty set.

        A guard that cannot distinguish 'nothing is missing' from 'I found no routes'
        is the exact failure mode this file exists to prevent.
        """
        real = _real_mutating_routes()
        assert len(real) >= 30, (
            f"only {len(real)} mutating routes found in api_v1_routes — the scan is broken "
            "or the module was restructured. Do NOT read this as 'nothing to cover'."
        )
        assert ("DELETE", "/api/v1/connections/{id}") in real, (
            "a known route is missing from the scan; the normaliser or the enumeration is wrong"
        )

    def test_every_path_id_mutation_is_pinned_by_the_boundary_table(self) -> None:
        real = _real_mutating_routes()
        with_path_id = {r for r in real if "{" in r[1]}
        missing = sorted(with_path_id - _table_routes())
        assert not missing, (
            f"{len(missing)} mutating route(s) take a resource id in the path but are NOT in "
            f"MUTATION_ROUTES, so nothing checks that they refuse a cross-tenant request:\n"
            + "\n".join(f"  {m} {p}" for m, p in missing)
            + "\n\nAdd them to MUTATION_ROUTES in tests/test_security/test_tenant_jwt_boundary.py "
            "(core#719)."
        )

    def test_the_table_has_no_rows_for_routes_that_no_longer_exist(self) -> None:
        """Drift runs both ways; a stale row is a test asserting on a dead endpoint."""
        stale = sorted(_table_routes() - _real_mutating_routes())
        assert not stale, f"{len(stale)} MUTATION_ROUTES row(s) match no real route:\n" + "\n".join(
            f"  {m} {p}" for m, p in stale
        )

    def test_the_no_path_id_allowlist_still_matches_something(self) -> None:
        """An allowlist entry must not outlive the route it excuses."""
        real = _real_mutating_routes()
        dead = sorted(NO_PATH_ID_ALLOWLIST - real)
        assert not dead, (
            f"{len(dead)} allowlist entr(ies) match no real route and should be deleted:\n"
            + "\n".join(f"  {m} {p}" for m, p in dead)
        )

    def test_every_body_only_mutation_is_accounted_for(self) -> None:
        """A NEW create-shaped route must not slip in unnoticed either.

        These are outside the boundary table by construction, but 'outside the table'
        must be a decision someone made, not a gap nobody saw.
        """
        real = _real_mutating_routes()
        without_path_id = {r for r in real if "{" not in r[1]}
        unaccounted = sorted(without_path_id - NO_PATH_ID_ALLOWLIST)
        assert not unaccounted, (
            f"{len(unaccounted)} mutating route(s) take no path id and are not in the "
            f"allowlist:\n" + "\n".join(f"  {m} {p}" for m, p in unaccounted) + "\n\n"
            "Such a route cannot be covered by MUTATION_ROUTES (no id in the path). Confirm it "
            "resolves any body-supplied resource reference through an org-scoped accessor — see "
            "tests/test_security/test_tenant_fk_boundary.py — then add it to NO_PATH_ID_ALLOWLIST."
        )


class TestTheGuardCanFail:
    """Negative controls. A check that has never failed has never been shown able to."""

    def test_the_normaliser_is_load_bearing(self) -> None:
        """Measured: dropping normalisation turns 0 findings into 25 false ones."""
        assert _norm("/api/v1/connections/{id:int}") == "/api/v1/connections/{id}"
        assert _norm("/api/v1/connections/{id}") == "/api/v1/connections/{id}"

        raw = {
            (m, p)
            for value in vars(routes_module).values()
            if isinstance(value, list) and value and value[0].__class__.__name__ == "Route"
            for route in value
            for m in (route.methods or ())
            if m in MUTATING_METHODS
            for p in (route.path,)
        }
        raw_missing = {r for r in raw if "{" in r[1]} - _table_routes()
        assert raw_missing, (
            "Un-normalised paths now match the table directly, so the normaliser is doing "
            "nothing and this control no longer controls anything. Either the table adopted "
            "`{id:int}` or the routes dropped it — re-derive before deleting the normaliser."
        )

    def test_an_injected_unpinned_route_is_detected(self) -> None:
        """Force the red this guard exists to produce, without editing the app.

        core#719's acceptance criterion is 'add a throwaway mutating route, watch it fail
        naming that route'. Doing it in-process keeps the proof in the suite permanently
        rather than in a one-off manual step nobody repeats.
        """
        from starlette.routing import Route

        def _throwaway(request):  # pragma: no cover - never called
            raise AssertionError("not callable")

        injected = Route("/api/v1/widgets/{id:int}", _throwaway, methods=["DELETE"])
        original = routes_module.api_v1_routes
        routes_module.api_v1_routes = [*original, injected]
        try:
            real = _real_mutating_routes()
            assert ("DELETE", "/api/v1/widgets/{id}") in real, "the scan did not see the route"
            missing = {r for r in real if "{" in r[1]} - _table_routes()
            assert ("DELETE", "/api/v1/widgets/{id}") in missing, (
                "the guard did not flag an unpinned mutating route — it cannot fail, so its "
                "green means nothing"
            )
            with pytest.raises(AssertionError, match=r"/api/v1/widgets/\{id\}"):
                self_test = TestBoundaryTableCoversEveryPathIdMutation()
                self_test.test_every_path_id_mutation_is_pinned_by_the_boundary_table()
        finally:
            routes_module.api_v1_routes = original

        # And the tree is restored — otherwise every later test in this session is wrong.
        assert ("DELETE", "/api/v1/widgets/{id}") not in _real_mutating_routes()

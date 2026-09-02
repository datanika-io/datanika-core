"""core#719 — the cross-tenant boundary table must not silently fall behind the real routes.

``tests/test_security/test_tenant_jwt_boundary.py`` parameterises over ``MUTATION_ROUTES``, a
hand-written list. ``datanika/services/api_v1_routes.py`` declares the real surface. Nothing
compared the two, so a new mutating endpoint could ship with no cross-tenant coverage while the
suite stayed green and said nothing — *"a green that would look identical had the thing failed"*.

## What the census found when this was written (2026-09-02, QA)

**Zero drift.** 35 real mutating routes; 25 carry a path id and all 25 are pinned; no stale rows.
The table was exact. This guard is therefore green on its first day, which is the right time to
add one — it locks in a correct state rather than papering over a broken one.

## 🚨 The matcher, which is the whole difficulty

The table writes ``/api/v1/connections/{id}``. Starlette declares ``/api/v1/connections/{id:int}``.
**Without normalising the path converter the census reports all 25 routes as unguarded** — a
maximally alarming and completely false result, on the security boundary that matters most.
Measured, not imagined::

    without {id:int} normalisation -> 25 "missing"
    with it                        -> 0

``test_the_normaliser_is_load_bearing`` pins it, so a future edit that drops the normalisation
fails *here* rather than emitting 25 phantom findings at 3am.

## Scope — this closes the arithmetic, not the shape

Every path-id mutating route is pinned. Ten mutating routes take **no** path id (creates, bulk
import) and are outside the table's threat model *by construction*. That class is covered by
``tests/test_security/test_tenant_fk_boundary.py``; the two bulk-import routes were separately
verified safe by construction — their payload has no ``*_connection_id`` field at all, only names
resolved from an org-scoped list, so a foreign id has nowhere to go in the request.
"""

from __future__ import annotations

import re

import pytest
from starlette.routing import Route

from datanika.services import api_v1_routes as routes_module
from tests.test_security.test_tenant_jwt_boundary import MUTATION_ROUTES

MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

# Mutating routes that legitimately take no org-owned resource id in the path.
# Cross-tenant reachability for these is covered by test_tenant_fk_boundary.py.
# Entries are asserted to still match a real route, so one cannot outlive its reason.
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


def _declared_routes() -> list[Route]:
    """Every ``Route`` the module declares, found by shape rather than by name.

    Looking the list up by its variable name would silently return nothing if it were
    renamed; the arming test below turns "found nothing" into a failure either way.
    """
    out: list[Route] = []
    for value in vars(routes_module).values():
        if isinstance(value, list) and value and isinstance(value[0], Route):
            out.extend(value)
    return out


def _mutating(routes: list[Route], *, normalise: bool = True) -> set[tuple[str, str]]:
    return {
        (method, _norm(r.path) if normalise else r.path)
        for r in routes
        for method in (r.methods or ())
        if method in MUTATING_METHODS
    }


def _table_routes() -> set[tuple[str, str]]:
    return {(method, _norm(path)) for method, path, _resource, _body in MUTATION_ROUTES}


@pytest.fixture(scope="module")
def real_routes() -> list[Route]:
    return _declared_routes()


class TestBoundaryTableCoversEveryPathIdMutation:
    def test_the_route_scan_is_armed(self, real_routes: list[Route]) -> None:
        """Without this, every assertion below is an assertion about an empty set.

        A guard that cannot distinguish "nothing is missing" from "I found no routes" is
        the exact failure mode this file exists to prevent.
        """
        assert len(real_routes) >= 40, (
            f"only {len(real_routes)} routes found in api_v1_routes — the scan is broken or the "
            "module was restructured. Do NOT read this as 'nothing to cover'."
        )
        mutating = _mutating(real_routes)
        assert len(mutating) >= 30, f"only {len(mutating)} mutating routes found"
        assert ("DELETE", "/api/v1/connections/{id}") in mutating, (
            "a known route is missing from the scan; the enumeration or the normaliser is wrong"
        )

    def test_every_path_id_mutation_is_pinned_by_the_boundary_table(
        self, real_routes: list[Route]
    ) -> None:
        with_path_id = {r for r in _mutating(real_routes) if "{" in r[1]}
        missing = sorted(with_path_id - _table_routes())
        assert not missing, (
            f"{len(missing)} mutating route(s) take a resource id in the path but are NOT in "
            "MUTATION_ROUTES, so nothing checks that they refuse a cross-tenant request:\n"
            + "\n".join(f"  {m} {p}" for m, p in missing)
            + "\n\nAdd them to MUTATION_ROUTES in "
            "tests/test_security/test_tenant_jwt_boundary.py (core#719)."
        )

    def test_the_table_has_no_rows_for_routes_that_no_longer_exist(
        self, real_routes: list[Route]
    ) -> None:
        """Drift runs both ways; a stale row is a test asserting on a dead endpoint."""
        stale = sorted(_table_routes() - _mutating(real_routes))
        assert not stale, f"{len(stale)} MUTATION_ROUTES row(s) match no real route:\n" + "\n".join(
            f"  {m} {p}" for m, p in stale
        )

    def test_the_no_path_id_allowlist_still_matches_something(
        self, real_routes: list[Route]
    ) -> None:
        """An allowlist entry must not outlive the route it excuses."""
        dead = sorted(NO_PATH_ID_ALLOWLIST - _mutating(real_routes))
        assert not dead, (
            f"{len(dead)} allowlist entr(ies) match no real route and should be deleted:\n"
            + "\n".join(f"  {m} {p}" for m, p in dead)
        )

    def test_every_body_only_mutation_is_accounted_for(self, real_routes: list[Route]) -> None:
        """A new create-shaped route must not slip in unnoticed either.

        These cannot be covered by MUTATION_ROUTES (no id in the path), but "outside the
        table" must be a decision someone made, not a gap nobody saw.
        """
        without_path_id = {r for r in _mutating(real_routes) if "{" not in r[1]}
        unaccounted = sorted(without_path_id - NO_PATH_ID_ALLOWLIST)
        assert not unaccounted, (
            f"{len(unaccounted)} mutating route(s) take no path id and are not in the "
            "allowlist:\n" + "\n".join(f"  {m} {p}" for m, p in unaccounted) + "\n\n"
            "Confirm it resolves any body-supplied resource reference through an org-scoped "
            "accessor (see tests/test_security/test_tenant_fk_boundary.py), then add it to "
            "NO_PATH_ID_ALLOWLIST."
        )


class TestTheGuardCanFail:
    """Negative controls. A check that has never failed has never been shown able to."""

    def test_the_normaliser_is_load_bearing(self, real_routes: list[Route]) -> None:
        """Measured: dropping normalisation turns 0 findings into 25 false ones."""
        assert _norm("/api/v1/connections/{id:int}") == "/api/v1/connections/{id}"
        assert _norm("/api/v1/connections/{id}") == "/api/v1/connections/{id}"

        raw = _mutating(real_routes, normalise=False)
        raw_missing = {r for r in raw if "{" in r[1]} - _table_routes()
        assert raw_missing, (
            "Un-normalised paths now match the table directly, so the normaliser is doing "
            "nothing and this control no longer controls anything. Either the table adopted "
            "`{id:int}` or the routes dropped it — re-derive before deleting the normaliser."
        )

    def test_an_unpinned_route_is_detected(self, real_routes: list[Route]) -> None:
        """Force the red this guard exists to produce.

        core#719's acceptance criterion is "add a throwaway mutating route, watch it fail
        naming that route". Done against a synthetic list rather than by monkeypatching the
        module: this suite shares a process with ~5000 other tests, and a global mutation
        that escapes its ``finally`` would corrupt every one of them.
        """

        async def _throwaway(request):  # pragma: no cover - never called
            raise AssertionError("not callable")

        injected = [*real_routes, Route("/api/v1/widgets/{id:int}", _throwaway, methods=["DELETE"])]

        mutating = _mutating(injected)
        assert ("DELETE", "/api/v1/widgets/{id}") in mutating, "the scan did not see the route"

        missing = {r for r in mutating if "{" in r[1]} - _table_routes()
        assert missing == {("DELETE", "/api/v1/widgets/{id}")}, (
            "the guard did not flag exactly the unpinned route — it either cannot fail (its "
            f"green means nothing) or it over-reports. Got: {sorted(missing)}"
        )

        # And the real module is untouched, because nothing was monkeypatched.
        assert ("DELETE", "/api/v1/widgets/{id}") not in _mutating(_declared_routes())

    def test_a_stale_table_row_is_detected(self, real_routes: list[Route]) -> None:
        """The other direction of drift must also be detectable."""
        fake_table = _table_routes() | {("DELETE", "/api/v1/gone/{id}")}
        stale = fake_table - _mutating(real_routes)
        assert stale == {("DELETE", "/api/v1/gone/{id}")}, (
            f"the stale-row check does not discriminate; got {sorted(stale)}"
        )

"""core#1097 / SPEC_PAGE_ENTRY §5 — a protected route's loaders must not work for a stranger.

`/` registers four `on_load` handlers. `check_auth` has already decided to send a
signed-out visitor to `/login`, and **that decision buys the other three nothing**:
Reflex dispatches the whole list, and a later handler never learns that an earlier
one returned a redirect. So each loader has to guard itself.

Two of the three did. `DashboardState.load_dashboard` did not, and opened a
session, ran five service calls against `org_id=0` and emitted `usage.get_summary`
— which the cloud plugin answers by opening a **second** session of its own.

## Why this file is a sweep and not a test of one handler

SPEC_PAGE_ENTRY AC5.5 asked for the other thirteen routes to be swept and the
number reported. The number is **eleven loaders across ten routes**, not one:

    /                            DashboardState.load_dashboard
    /connections                 ConnectionState.load_connections
    /dag                         DagState.load_dependencies
    /models                      ModelState.load_models
    /models/[id]                 ModelDetailState.load_model_detail
    /pipelines                   PipelineState.load_pipelines
    /runs                        RunState.load_runs
    /schedules                   ScheduleState.load_schedules
    /transformations             TransformationState.load_transformations
    /transformations/sql-editor  TransformationState.load_transformations
    /uploads                     UploadState.load_uploads

⚠️ **`/settings` — the route the spec told me to start with, on the grounds that
its six `on_load` handlers were the largest unexamined surface — is CLEAN.** All
four of its loaders already carry the guard. The prioritisation hint pointed at
the one large route that did not need it, and the defect was spread across ten
smaller ones. Handler *count* was the wrong ordering key.

## The set is derived, not listed

`registered_loaders()` reads `datanika/datanika.py`'s AST for every `add_page`
carrying `AuthState.check_auth` and returns its other `on_load` handlers. So the
twelfth loader is covered on the day it is registered, rather than on the day
someone runs another sweep. A hardcoded list would be satisfied by exactly the
routes that already exist — which is what made this a sweep and not a bug report.

## The harness has to be able to see the work, or its green means nothing

Every assertion here is *"nothing happened"*, and nothing-happened is also what a
harness that never reached the handler produces. Three things guard against that:

1. **The unauthenticated driver requires the handler to return normally.** An
   exception is a red, not a pass — a loader that blows up has demonstrated that
   it ran far enough to break, not that it guarded.
2. **`test_the_harness_can_see_a_session_and_an_emit`** drives the same handler
   with an *authenticated* stand-in and requires **>= 1 session and >= 1 emit**.
   If that goes green while the sweep is green, the sweep is measuring something.
   If it goes red, every "0 sessions" above it is unattributable.
3. **`patch` on a missing attribute raises**, so a module that stops importing
   `get_sync_session` fails loudly rather than quietly dropping out of the sweep.

🚨 **Both anti-vacuity measures earned their keep on the first run, in opposite
directions.** Without `_caller`'s real `_get_org_id`, all eleven loaders reded on
`TypeError: object MagicMock can't be used in 'await' expression` — a table that
matched the predicted eleven exactly while measuring nothing about sessions.
And without patching `EncryptionService` on the *unauthenticated* path, six of
them died on the Fernet placeholder **before** opening a session, so the session
counter read **0** for loaders that do plenty: a "did no work" green.
"""

import ast
import pathlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datanika.ui.state.auth_state import AuthState, OrgInfo, UserInfo
from datanika.ui.state.base_state import BaseState

_APP = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "datanika.py"
_STATE_PKG = "datanika.ui.state"

#: Loaders that legitimately touch no database and emit nothing. Named rather
#: than detected, so adding one is a decision somebody makes on purpose.
_NO_IO_LOADERS = {
    "ConnectionState.load_template_from_query",
    "SettingsState.redirect_legacy_billing_tab",
}


def registered_loaders() -> list[tuple[str, str]]:
    """(route, "Class.method") for every non-``check_auth`` loader on a protected page."""
    tree = ast.parse(_APP.read_bytes().decode("utf-8"), filename=str(_APP))
    out: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or getattr(node.func, "attr", None) != "add_page":
            continue
        route, handlers = None, []
        for kw in node.keywords:
            if kw.arg == "route" and isinstance(kw.value, ast.Constant):
                route = kw.value.value
            if kw.arg == "on_load":
                items = kw.value.elts if isinstance(kw.value, ast.List) else [kw.value]
                for it in items:
                    target = it.func if isinstance(it, ast.Call) else it
                    if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name):
                        handlers.append(f"{target.value.id}.{target.attr}")
        if route is None or "AuthState.check_auth" not in handlers:
            continue
        for h in handlers:
            if h != "AuthState.check_auth" and h not in _NO_IO_LOADERS:
                out.append((route, h))
    return sorted(set(out))


def _resolve(qualname: str):
    """ "Class.method" -> (module object, state class, the underlying async function)."""
    import importlib
    import re

    cls_name, meth = qualname.split(".")
    # StateClass -> module: CamelCase to snake_case, which is this package's convention.
    mod_name = re.sub(r"(?<!^)(?=[A-Z])", "_", cls_name).lower()
    module = importlib.import_module(f"{_STATE_PKG}.{mod_name}")
    cls = getattr(module, cls_name)
    handler = getattr(cls, meth)
    return module, cls, getattr(handler, "fn", handler)


def _auth_stand_in(*, signed_in: bool):
    """An ``AuthState`` stand-in carrying its REAL field defaults.

    A bare ``MagicMock`` answers every attribute truthily, so ``if org_id == 0``
    would never be taken and this file would measure nothing — the trap
    ``test_handler_session_revalidation.py`` documents in its own docstring.
    """
    st = MagicMock()
    for name, field in AuthState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    if signed_in:
        st.current_org = OrgInfo(id=7, name="Acme", slug="acme")
        st.current_user = UserInfo(id=3, email="a@b.c", full_name="A B")
    return st


def _caller(auth_stand_in):
    """A stand-in loader state whose ``get_state`` yields ``auth_stand_in``.

    🚨 ``_get_org_id`` is delegated to the **real** ``BaseState`` implementation,
    and it is load-bearing. Every one of these loaders opens with
    ``await self._get_org_id()``; on a bare ``MagicMock`` that returns a
    ``MagicMock``, and awaiting one raises ``TypeError``.

    That mattered more than a broken fixture usually does. Before this line
    existed the sweep reported **exactly the eleven failures it had predicted** —
    and every one of them was the ``TypeError``, not a session being opened. A
    harness measuring the wrong thing had produced a table that matched the
    hypothesis perfectly.
    """
    st = MagicMock()
    st.get_state = AsyncMock(return_value=auth_stand_in)
    st._get_org_id = lambda: BaseState._get_org_id(st)
    # load_model_detail reads a route param before anything else; give it a real
    # one so the positive control is not satisfied by its own `except TypeError`.
    st.router.page.params = {"id": "1"}
    return st


class _Counter:
    """A ``get_sync_session`` stand-in that counts entries and yields a session."""

    def __init__(self, session=None):
        self.entered = 0
        self._session = session

    def __call__(self):
        self.entered += 1
        return self

    def __enter__(self):
        return self._session if self._session is not None else MagicMock()

    def __exit__(self, *exc):
        return False


async def _drive(qualname: str, *, signed_in: bool, session=None) -> tuple[int, int, str]:
    """Run the loader; return (sessions opened, hooks emitted, exception repr or "").

    The exception is **returned rather than raised** because the two callers want
    opposite things from it. The sweep requires it to be empty — a loader that
    blows up has not proved it guarded. The positive control ignores it, since
    its only question is whether the counters *can* move.
    """
    module, _cls, fn = _resolve(qualname)
    counter = _Counter(session)
    emits: list[str] = []
    raised = ""

    stack = [patch.object(module, "get_sync_session", counter)]
    if hasattr(module, "emit"):
        stack.append(patch.object(module, "emit", lambda name, **kw: emits.append(name)))
    if hasattr(module, "EncryptionService"):
        # The test config carries the insecure placeholder rather than a Fernet
        # key, and nothing here decrypts anything — same patch and same reason as
        # test_models_empty_state_after_a_load.py.
        #
        # 🚨 It is patched on BOTH paths, and that is what makes the pre-fix red
        # attributable. Six of these loaders build an EncryptionService *before*
        # they open a session, so unpatched they die on the Fernet key with the
        # session counter still reading **0** — a "did no work" green for a
        # loader that does plenty. The `raised` assertion caught it, but the
        # failure would have named the wrong defect.
        stack.append(patch.object(module, "EncryptionService", MagicMock()))

    for ctx in stack:
        ctx.start()
    try:
        await fn(_caller(_auth_stand_in(signed_in=signed_in)))
    except Exception as exc:  # noqa: BLE001 - reported, never swallowed
        raised = f"{type(exc).__name__}: {exc}"
    finally:
        for ctx in reversed(stack):
            ctx.stop()
    return counter.entered, len(emits), raised


LOADERS = registered_loaders()


class TestTheSweepIsNotVacuous:
    def test_the_derivation_found_the_protected_routes(self):
        routes = {route for route, _ in LOADERS}
        assert len(routes) >= 10, (
            f"the AST derivation found loaders on only {len(routes)} protected routes; "
            "it is reporting on almost nothing"
        )

    def test_every_loader_resolves_to_a_real_handler(self):
        """A qualname this cannot resolve would silently drop out of the sweep."""
        unresolved = []
        for _route, qualname in LOADERS:
            try:
                _resolve(qualname)
            except (ImportError, AttributeError) as exc:
                unresolved.append(f"{qualname}: {exc}")
        assert unresolved == [], unresolved

    @pytest.mark.asyncio
    async def test_the_harness_can_see_a_session_and_an_emit(self, db_session):
        """The positive control. Without this, every zero below is unattributable.

        ``load_dashboard`` is the one loader that does both, so it is the only
        handler that can arm both counters at once.

        ⚠️ It is handed the **real** ``db_session``, not a ``MagicMock``. Against
        a mock the first ``len(...)`` of a mocked query result raises before the
        emit is ever reached, so the emit counter would read 0 — and a positive
        control that cannot reach the thing it is arming is not one.
        """
        sessions, emits, raised = await _drive(
            "DashboardState.load_dashboard", signed_in=True, session=db_session
        )
        assert sessions >= 1, (
            "the harness saw no session for an AUTHENTICATED load_dashboard, so it "
            f"cannot distinguish 'the guard worked' from 'the handler never ran'. {raised}"
        )
        assert emits >= 1, (
            "the harness saw no hook emit for an authenticated load_dashboard; the "
            f"emit half of AC5.2 is untested and would pass by not looking. {raised}"
        )


class TestNoLoaderWorksWithoutASession:
    @pytest.mark.parametrize(
        ("route", "qualname"),
        LOADERS,
        ids=[f"{r}::{q}" for r, q in LOADERS],
    )
    @pytest.mark.asyncio
    async def test_it_opens_no_session_and_emits_nothing(self, route, qualname):
        """core#1097 AC5.1 + AC5.2, for every protected route rather than for one.

        Proven red first against unguarded code: eleven of these failed, and
        ``/settings``'s four passed — which is what attributes the red to the
        missing guard rather than to the harness.
        """
        sessions, emits, raised = await _drive(qualname, signed_in=False)
        assert (sessions, emits) == (0, 0), (
            f"{route} -> {qualname} did work for a visitor with no resolved org or "
            f"user: {sessions} database session(s), {emits} hook emit(s). "
            "Add the guard used by onboarding_state.py and notification_center_state.py."
        )
        assert raised == "", (
            f"{route} -> {qualname} raised instead of returning: {raised}. A loader "
            "that blows up has not demonstrated that it guarded — it demonstrated "
            "that it ran far enough to break."
        )

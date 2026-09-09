"""Every hook handler must accept what its event actually sends (core#456).

The P0: `run.*_completed` was emitted with kwargs the registered handlers could
not bind, so `emit()` raised `TypeError` **after** the run had completed, and
the caller's `except` overwrote the finished run with `fail_run(...)`. Every
successful run in prod was recorded as FAILED, run notifications never
dispatched, and cloud's byte metering — registered behind the raising handler —
never executed.

2493 tests were green throughout. Emitters and handlers were each tested in
isolation; **nothing asserted that a handler could accept what its emitter
sends**, because that contract only exists once both sides are in the same
process, which is the Celery worker and not the unit suite.

So this file tests the seam rather than either side:

1. scan the source for every ``emit``/``announce`` call and collect the kwargs
   each event is actually sent — the *emitter* half of the contract, taken from
   the code rather than from a list someone maintains by hand;
2. register all core hooks exactly as the worker does, and assert every
   registered handler can bind those kwargs.

Fixing only the three broken events would have left the class open, which is
why this is generic: a new event, a new subscriber, or a renamed kwarg all fail
here rather than in production.
"""

import ast
import inspect
import pathlib

import pytest

_SOURCE_ROOT = pathlib.Path(__file__).resolve().parents[1] / "datanika"
_DISPATCH_FUNCS = {"emit", "announce", "collect_events"}

#: Choke points that FORWARD to a dispatcher: name -> index of the event-name argument.
#:
#: 🚨 core#657 AC4 moved the three ``run.*_completed`` announces behind
#: ``ExecutionService.announce_completion``, because the call sites had been passing
#: ``status="success"`` as a hardcoded literal and billing users for runs they cancelled.
#: A scanner that knows only the direct form finds **nothing** for those three events —
#: and ``test_handlers_can_bind_what_their_emitters_send`` *skips* events it cannot find
#: (``if event not in emitted: continue``), so the contract stops being checked without
#: anything going red. ``test_source_scan_finds_the_run_events`` is the floor that caught
#: exactly this, in CI, after a local run scoped to two directories missed it.
_FORWARDERS = {"announce_completion": 3}


def _forwarder_injected_kwargs() -> dict[str, set[str]]:
    """What each forwarder adds to the payload itself, read from ITS OWN source.

    ``announce_completion`` supplies ``session``, ``org_id``, ``run_id`` and — the whole
    point of core#657 AC4 — ``status``, read from the run rather than hardcoded. Those are
    genuinely sent, so a handler must be able to bind them.

    ⚠️ Reporting only the call-site kwargs would not break this file, it would **weaken**
    it: a smaller expected set makes the binding assertion easier to satisfy. A guard that
    quietly asks less is the failure mode this module exists to prevent.

    Derived rather than listed, because a hand-maintained list is precisely what this
    module's docstring says it refuses to depend on.
    """
    injections: dict[str, set[str]] = {}
    for path in _SOURCE_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for fn in ast.walk(tree):
            if not isinstance(fn, ast.FunctionDef) or fn.name not in _FORWARDERS:
                continue
            for node in ast.walk(fn):
                if not isinstance(node, ast.Call):
                    continue
                name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
                if name in _DISPATCH_FUNCS:
                    injections.setdefault(fn.name, set()).update(
                        kw.arg for kw in node.keywords if kw.arg is not None
                    )
    return injections


def _emitted_kwargs_by_event() -> dict[str, set[str]]:
    """Map event name -> kwargs it is dispatched with, by reading the source.

    Static because the alternative is a hand-maintained list, and a
    hand-maintained list is exactly what drifts from the code it describes.
    Call sites that compute the event name dynamically are skipped — there are
    none today, and a literal is what makes this checkable.
    """
    injected = _forwarder_injected_kwargs()
    found: dict[str, set[str]] = {}
    for path in _SOURCE_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = getattr(func, "id", None) or getattr(func, "attr", None)
            if name in _DISPATCH_FUNCS:
                index, extra = 0, set()
            elif name in _FORWARDERS:
                index, extra = _FORWARDERS[name], injected.get(name, set())
            else:
                continue
            if len(node.args) <= index:
                continue
            event = node.args[index]
            if not isinstance(event, ast.Constant) or not isinstance(event.value, str):
                continue
            kwargs = {kw.arg for kw in node.keywords if kw.arg is not None} | extra
            found.setdefault(event.value, set()).update(kwargs)
    return found


@pytest.fixture
def registered_handlers():
    """Register core hooks the way ``tasks/celery_app.py`` does, then restore."""
    from datanika import hooks
    from datanika.services._register_hooks import register_all_core_hooks

    saved = {event: list(handlers) for event, handlers in hooks._handlers.items()}
    hooks.clear()
    try:
        register_all_core_hooks()
        yield dict(hooks._handlers)
    finally:
        hooks.clear()
        hooks._handlers.update(saved)


class TestEveryHandlerBindsItsEvent:
    def test_source_scan_finds_the_run_events(self):
        """Guard the guard: if the scan finds nothing, it proves nothing."""
        events = _emitted_kwargs_by_event()
        assert "run.upload_completed" in events
        assert "run.models_completed" in events
        assert "run.transformation_completed" in events

    def test_the_forwarder_scan_finds_its_injected_kwargs(self):
        """Floor for the forwarder machinery itself.

        If this returned an empty set the three events would still be *found*, but with a
        smaller kwarg set — and a smaller set makes the binding assertion below easier to
        satisfy. A guard that quietly asks less does not go red.
        """
        injected = _forwarder_injected_kwargs()
        assert "announce_completion" in injected, (
            "the forwarder scan found no dispatch call inside announce_completion — "
            "every kwarg it injects would be missing from the contract below"
        )
        assert {"session", "org_id", "run_id", "status"} <= injected["announce_completion"]

    def test_the_run_events_still_carry_status(self):
        """`status` is what cloud's `_is_billable` reads (core#657 AC4).

        It moved from the call sites into the forwarder, so it is exactly the kwarg a
        naive rescan would drop.
        """
        events = _emitted_kwargs_by_event()
        for event in (
            "run.upload_completed",
            "run.models_completed",
            "run.transformation_completed",
        ):
            assert "status" in events[event], f"{event} is no longer scanned as sending status"

    def test_handlers_can_bind_what_their_emitters_send(self, registered_handlers):
        emitted = _emitted_kwargs_by_event()
        failures = []

        for event, handlers in registered_handlers.items():
            if event not in emitted:
                continue
            sample = dict.fromkeys(emitted[event], object())
            for handler in handlers:
                try:
                    inspect.signature(handler).bind(**sample)
                except TypeError as exc:
                    failures.append(
                        f"{event}: {getattr(handler, '__qualname__', handler)} "
                        f"cannot accept {sorted(sample)} — {exc}"
                    )

        assert not failures, "Hook handlers cannot bind their event's kwargs:\n" + "\n".join(
            failures
        )


class TestAnnouncedEventsCannotBeVetoed:
    """A subscriber may not veto — or starve — an event that already happened."""

    def test_a_raising_handler_does_not_propagate(self):
        from datanika import hooks

        hooks.clear()
        try:
            hooks.on("test.done", lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
            hooks.announce("test.done", org_id=1)  # must not raise
        finally:
            hooks.clear()

    def test_a_raising_handler_does_not_starve_the_next_one(self):
        """The metering half of core#456: order stopped being load-bearing."""
        from datanika import hooks

        reached = []
        hooks.clear()
        try:
            hooks.on("test.done", lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
            hooks.on("test.done", lambda **kw: reached.append(kw))
            hooks.announce("test.done", org_id=7)
        finally:
            hooks.clear()

        assert reached == [{"org_id": 7}], "a failing subscriber starved the one behind it"

    def test_emit_still_propagates_for_gates(self):
        """Quota hooks veto by raising — isolation there would disable them."""
        from datanika import hooks

        hooks.clear()
        try:
            hooks.on(
                "test.before_create", lambda **kw: (_ for _ in ()).throw(RuntimeError("quota"))
            )
            with pytest.raises(RuntimeError, match="quota"):
                hooks.emit("test.before_create", org_id=1)
        finally:
            hooks.clear()

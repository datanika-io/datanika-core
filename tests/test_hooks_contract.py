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


def _emitted_kwargs_by_event() -> dict[str, set[str]]:
    """Map event name -> kwargs it is dispatched with, by reading the source.

    Static because the alternative is a hand-maintained list, and a
    hand-maintained list is exactly what drifts from the code it describes.
    Call sites that compute the event name dynamically are skipped — there are
    none today, and a literal is what makes this checkable.
    """
    found: dict[str, set[str]] = {}
    for path in _SOURCE_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = getattr(func, "id", None) or getattr(func, "attr", None)
            if name not in _DISPATCH_FUNCS or not node.args:
                continue
            event = node.args[0]
            if not isinstance(event, ast.Constant) or not isinstance(event.value, str):
                continue
            kwargs = {kw.arg for kw in node.keywords if kw.arg is not None}
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

"""Unit tests for the generic hook / event system."""

import pytest

from datanika import hooks


@pytest.fixture(autouse=True)
def _clean_handlers():
    """Isolate each test by clearing handlers before and after."""
    hooks.clear()
    yield
    hooks.clear()


class TestOnAndEmit:
    def test_on_and_emit(self):
        calls = []
        hooks.on("test.event", lambda **kw: calls.append(kw))
        hooks.emit("test.event", org_id=1, count=5)
        assert calls == [{"org_id": 1, "count": 5}]

    def test_emit_no_handlers(self):
        hooks.emit("nonexistent.event", x=1)  # should not raise

    def test_multiple_handlers(self):
        order = []
        hooks.on("evt", lambda **kw: order.append("a"))
        hooks.on("evt", lambda **kw: order.append("b"))
        hooks.emit("evt")
        assert order == ["a", "b"]


class TestOff:
    def test_off(self):
        calls = []

        def handler(**kw):
            calls.append(kw)

        hooks.on("evt", handler)
        hooks.emit("evt", x=1)
        assert len(calls) == 1

        hooks.off("evt", handler)
        hooks.emit("evt", x=2)
        assert len(calls) == 1  # not called again

    def test_off_nonexistent_handler(self):
        hooks.off("evt", lambda **kw: None)  # should not raise

    def test_off_nonexistent_event(self):
        hooks.off("no.such.event", lambda **kw: None)  # should not raise


class TestClear:
    def test_clear(self):
        calls = []
        hooks.on("a", lambda **kw: calls.append("a"))
        hooks.on("b", lambda **kw: calls.append("b"))
        hooks.clear()
        hooks.emit("a")
        hooks.emit("b")
        assert calls == []


class TestHandlerExceptionPropagates:
    def test_handler_exception_propagates(self):
        def bad_handler(**kw):
            raise ValueError("quota exceeded")

        hooks.on("check", bad_handler)
        with pytest.raises(ValueError, match="quota exceeded"):
            hooks.emit("check", org_id=1)


class TestEventIsolation:
    def test_different_events_isolated(self):
        calls_a = []
        calls_b = []
        hooks.on("event.a", lambda **kw: calls_a.append(1))
        hooks.on("event.b", lambda **kw: calls_b.append(1))

        hooks.emit("event.a")
        assert len(calls_a) == 1
        assert len(calls_b) == 0


class TestCollectEvents:
    # collect_events() is the emit variant used when the core code needs
    # to splice plugin-contributed values into its own return — e.g.
    # auth_state.signup() accumulating Reflex events from a plugin hook
    # before returning [*events, rx.redirect(...)]. It must stay simple:
    # call every handler, flatten list returns, skip None, preserve order.

    def test_no_handlers_returns_empty_list(self):
        result = hooks.collect_events("nope", x=1)
        assert result == []

    def test_single_scalar_return_is_appended(self):
        hooks.on("evt", lambda **kw: "value")
        assert hooks.collect_events("evt") == ["value"]

    def test_none_return_is_skipped(self):
        hooks.on("evt", lambda **kw: None)
        assert hooks.collect_events("evt") == []

    def test_list_return_is_flattened(self):
        hooks.on("evt", lambda **kw: ["a", "b"])
        assert hooks.collect_events("evt") == ["a", "b"]

    def test_mixed_handlers_preserve_order(self):
        hooks.on("evt", lambda **kw: "first")
        hooks.on("evt", lambda **kw: None)
        hooks.on("evt", lambda **kw: ["second", "third"])
        hooks.on("evt", lambda **kw: "fourth")
        assert hooks.collect_events("evt") == ["first", "second", "third", "fourth"]

    def test_kwargs_passed_through(self):
        seen = {}
        hooks.on("evt", lambda **kw: seen.update(kw) or "ok")
        hooks.collect_events("evt", user_id=42, name="alice")
        assert seen == {"user_id": 42, "name": "alice"}

    def test_empty_list_return_contributes_nothing(self):
        hooks.on("evt", lambda **kw: [])
        hooks.on("evt", lambda **kw: "x")
        assert hooks.collect_events("evt") == ["x"]

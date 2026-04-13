"""Generic hook / event system for plugin extensibility."""

from collections.abc import Callable

_handlers: dict[str, list[Callable]] = {}


def on(event: str, handler: Callable) -> None:
    """Register a handler for an event."""
    _handlers.setdefault(event, []).append(handler)


def off(event: str, handler: Callable) -> None:
    """Remove a handler for an event."""
    handlers = _handlers.get(event, [])
    if handler in handlers:
        handlers.remove(handler)


def emit(event: str, **kwargs) -> None:
    """Emit an event, calling all registered handlers."""
    for handler in _handlers.get(event, []):
        handler(**kwargs)


def collect_events(event: str, **kwargs) -> list:
    """Emit an event and collect non-None handler returns into a flat list.

    Used when core code needs to splice plugin-contributed values into
    its own return — e.g. ``auth_state.signup()`` accumulating Reflex
    events from a cloud plugin hook before returning
    ``[*events, rx.redirect(...)]``.

    Contract: list returns are flattened one level, ``None`` returns are
    skipped, scalar returns are appended as-is. Handler order is
    preserved.
    """
    results: list = []
    for handler in _handlers.get(event, []):
        r = handler(**kwargs)
        if r is None:
            continue
        if isinstance(r, list):
            results.extend(r)
        else:
            results.append(r)
    return results


def clear() -> None:
    """Remove all handlers. Useful for testing."""
    _handlers.clear()

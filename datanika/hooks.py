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


def clear() -> None:
    """Remove all handlers. Useful for testing."""
    _handlers.clear()

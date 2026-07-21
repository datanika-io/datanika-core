"""Generic hook / event system for plugin extensibility.

Two dispatch verbs, because the events fall into two kinds and conflating them
caused core#456:

* :func:`emit` — a subscriber **may veto** by raising. The ``*.before_create``
  quota hooks depend on this: cloud raises ``QuotaExceeded`` to stop a
  connection being created. Exceptions must propagate or enforcement silently
  dies.
* :func:`announce` — the thing already happened; a subscriber may **not** veto
  it, and may not starve the ones behind it. Handler failures are logged and
  dispatch continues.

The distinction is not stylistic. ``run.*_completed`` was announced with
``emit``, so one handler raising both (a) turned a completed run into a
``FAILED`` row via the caller's ``except``, and (b) starved every handler
registered after it — including cloud's byte metering, which is why
``bytes_processed`` was never recorded.
"""

import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)

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
    """Emit an event, calling all registered handlers.

    Exceptions **propagate** — this is the dispatch for gates, where a
    subscriber refusing is the point (quota checks on ``*.before_create``).
    For an event describing something that already happened, use
    :func:`announce`.
    """
    for handler in _handlers.get(event, []):
        handler(**kwargs)


def announce(event: str, **kwargs) -> None:
    """Tell every subscriber that something already happened.

    No subscriber can veto the event or starve the ones behind it: each
    handler's failure is logged and dispatch continues. Two consequences worth
    stating, both learned from core#456:

    * a broken notification handler cannot turn a completed run into a failed
      one, no matter where the caller put its ``try``;
    * handler **registration order stops being load-bearing** — cloud's
      metering no longer depends on every core handler ahead of it behaving.
      Reordering would only have changed which subscriber gets starved.
    """
    for handler in _handlers.get(event, []):
        try:
            handler(**kwargs)
        except Exception:
            # Logged, not swallowed silently: a subscriber that is failing
            # every time should be visible without taking the run down.
            logger.exception(
                "Hook handler %r failed for announced event %r",
                getattr(handler, "__qualname__", handler),
                event,
            )


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

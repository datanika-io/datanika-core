"""Thin registry for plugin-contributed UI components.

Core stays open-source and self-contained; SaaS concerns live in the
``datanika-cloud`` plugin. The seam between the two is this module plus
the ``datanika.hooks`` event system:

- **Head components** — plugin-contributed ``rx.Component`` items that
  ``datanika.datanika`` splices into ``rx.App(head_components=[...])``
  at construction time. Registered during the plugin's early bootstrap
  phase (before ``rx.App`` is constructed).
- **Page scripts** — plugin-contributed inline scripts keyed by page
  name. Named page functions call ``get_page_scripts(page_key)`` and
  splat the result into their own children, so the plugin can attach
  behavior to specific pages without those pages importing the plugin.

Both halves are additive — there's no ``unregister`` API. Plugins
register once at import time and stay registered for the process
lifetime. Tests clear internal state via the private ``_head_components``
/ ``_page_scripts`` module attributes; production code must not touch
those directly.
"""

from __future__ import annotations

import reflex as rx

_head_components: list[rx.Component] = []
_page_scripts: dict[str, list[str]] = {}


def register_head_component(component: rx.Component) -> None:
    """Append a component that ``datanika.datanika`` will include in
    ``rx.App(head_components=[...])``. Must be called before ``rx.App``
    is constructed — typically from a plugin's bootstrap function.
    """
    _head_components.append(component)


def plugin_head_components() -> list[rx.Component]:
    """Return a copy of the registered head components. Copy, not view,
    so callers can freely mutate the returned list without affecting
    future registrations.
    """
    return list(_head_components)


def register_page_script(page_key: str, js: str) -> None:
    """Register a block of inline JavaScript for a named page.

    The page function reads it back via ``get_page_scripts(page_key)``
    and renders each entry as an ``rx.script`` child. Page keys are
    agreed by convention between core pages and the plugin — keep them
    short and stable (e.g. ``pipeline_templates``, ``connections``).
    """
    _page_scripts.setdefault(page_key, []).append(js)


def get_page_scripts(page_key: str) -> list[rx.Component]:
    """Return fresh ``rx.script`` components for the given page key.

    Returns ``[]`` when no plugin has registered anything for the key,
    which is the open-source / ``DATANIKA_EDITION`` unset case. A fresh
    component list is built on every call so that Reflex re-renders
    don't share component identity across mount cycles.
    """
    return [rx.script(js) for js in _page_scripts.get(page_key, [])]

"""Plausible analytics instrumentation for the Reflex app (issue #92).

Two public surfaces:

1. ``plausible_head_component()`` — emits the Plausible ``<script>`` tag
   into ``rx.App(head_components=[...])`` when both ``analytics_domain``
   and ``analytics_script_src`` are configured. Returns ``None`` otherwise
   so ``datanika.py`` can splice it into the head list conditionally.

2. ``ANALYTICS_EVENTS`` — the canonical set of custom event names we fire
   from inline ``rx.script`` blocks in the UI pages. A test scanner in
   ``tests/test_ui/test_analytics.py`` walks every ``.py`` file under
   ``datanika/ui/`` looking for ``window.plausible('NAME', ...)`` calls
   and asserts ``NAME`` is in this set — that's how we catch typos before
   they silently break a Growth dashboard.

Design notes:

- Analytics is **gated off by default**. Both settings default to empty.
  This lets the instrumentation code ship before Infra creates the
  ``app.datanika.io`` site in Plausible CE — the inline event scripts
  guard ``window.plausible && window.plausible(...)`` so they're no-ops
  when the global isn't defined.
- The Plausible script tag uses the standard (non-manual) form so SPA
  pageviews are auto-tracked. Manual pageview firing is a follow-up if
  we find Reflex's router doesn't trigger the history events Plausible
  listens for.
- We intentionally do NOT wrap event firing in Python. JavaScript emits
  the events directly via inline ``rx.script`` blocks at the call sites,
  because event firing must happen synchronously on the client before
  navigation. A server round-trip would race the browser's link follow.
"""

from __future__ import annotations

import reflex as rx

from datanika.config import settings

# Canonical event name catalog. Add an entry here when you wire a new
# ``window.plausible('...')`` call in the UI, and also update
# ``tests/test_ui/test_analytics.py::TestAnalyticsEvents::test_catalog_has_no_surprising_extras``.
# The set is intentionally small — each entry is a real funnel step Growth
# measures. Don't dump raw UI interactions in here.
ANALYTICS_EVENTS: set[str] = {
    # User clicked a card on /pipelines/templates (top of funnel).
    "template_selected",
    # /connections page loaded with ?template=<slug> and prefill ran
    # (middle of funnel — credential step is imminent).
    "template_prefill_applied",
    # "template_first_run_triggered" is a follow-up — see issue #92 body
    # for the design trade-off (needs DB column or cross-state threading).
}


def plausible_head_component() -> rx.Component | None:
    """Return a Reflex script tag for Plausible, or None if disabled.

    Called once from ``datanika.py`` when building ``rx.App``. Callers
    splice the return value into the ``head_components`` list and filter
    out ``None``::

        head = [favicon]
        plausible = plausible_head_component()
        if plausible is not None:
            head.append(plausible)
        app = rx.App(head_components=head)
    """
    domain = settings.analytics_domain.strip()
    src = settings.analytics_script_src.strip()
    if not domain or not src:
        return None

    # ``defer`` so the script doesn't block rendering. ``data-domain`` is
    # what Plausible uses to bucket events into the right site dashboard.
    return rx.script(
        src=src,
        defer=True,
        custom_attrs={"data-domain": domain},
    )

"""Analytics instrumentation for the Reflex app.

Two providers, both dormant-by-default (empty config = no script tags,
inline event calls become JS no-ops via ``&&`` guards):

**Plausible** (issue #92, shipped PR #94):
- ``plausible_head_component()`` — returns an ``rx.script`` loading
  Plausible's JS when both ``analytics_domain`` and ``analytics_script_src``
  are configured, ``None`` otherwise.
- ``ANALYTICS_EVENTS`` — canonical set of Plausible custom event names.
  A scanner test enforces every ``window.plausible('NAME', ...)`` call
  in ``datanika/ui/`` references a name in this set.

**Google Ads** (issue #96, Phase 2 conversion tracking):
- ``google_ads_head_components()`` — returns a list of ``rx.script``
  components (gtag.js loader + init) when ``google_ads_tag_id`` is set,
  empty list otherwise.
- ``google_ads_conversion_event_js(label)`` — pure function returning a
  JS one-liner that fires ``gtag('event', 'conversion', {...})`` for the
  given conversion label. Returns empty string when the label or tag ID
  is unset. Callers pass the result to ``rx.call_script`` inside an
  event handler (e.g. signup success in ``auth_state.signup``).

Design notes:

- **Gated off by default**. All settings default to empty. This lets
  instrumentation code ship before Infra wires the prod env vars.
  Plausible inline events guard on ``window.plausible &&``; Google Ads
  conversion events guard on ``window.gtag &&``. Both are silent no-ops
  when the corresponding global isn't defined.
- **Plausible** uses the standard non-manual script form so SPA
  pageviews are auto-tracked.
- **Google Ads** loads gtag.js asynchronously + an inline init script
  that calls ``gtag('config', <TAG_ID>)``. Matches the exact snippet
  Google Ads provides in the Tag setup UI, so the rendered HTML is
  bit-identical to what the Google Ads debugger expects.
- **Event firing**: for Plausible we use inline ``rx.script`` blocks at
  the call site (synchronous, beats browser navigation). For Google Ads
  signup conversion we use ``rx.call_script`` returned from the signup
  event handler alongside ``rx.redirect("/")`` — Reflex processes the
  event list in order, so the JS fires before the redirect.
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


# ---------------------------------------------------------------------------
# Google Ads (issue #96 — Phase 2 conversion tracking)
# ---------------------------------------------------------------------------


def google_ads_head_components() -> list[rx.Component]:
    """Return the gtag.js loader + init script tags, or an empty list.

    Dormant-by-default: when ``settings.google_ads_tag_id`` is empty,
    returns ``[]`` so callers can simply ``extend`` the head components
    list without null checks. When set, returns two script components:

    1. The async gtag.js loader pointing at googletagmanager.com with
       the configured tag ID.
    2. The standard inline init snippet that creates ``window.dataLayer``
       and calls ``gtag('js', new Date())`` + ``gtag('config', <TAG_ID>)``.

    The two scripts together match the exact HTML snippet Google Ads
    shows in the *Tag setup → Install the tag yourself* dialog. Keeping
    it bit-identical means Google's conversion debugger recognizes it
    without extra configuration.

    The conversion label is NOT read here — that lives on a per-event
    basis via ``google_ads_conversion_event_js()``. This function only
    emits the base tag, which is a prerequisite for any conversion event
    to fire.
    """
    tag_id = settings.google_ads_tag_id.strip()
    if not tag_id:
        return []

    loader = rx.script(
        src=f"https://www.googletagmanager.com/gtag/js?id={tag_id}",
        custom_attrs={"async": "true"},
    )
    init = rx.script(
        "window.dataLayer = window.dataLayer || [];"
        "function gtag(){dataLayer.push(arguments);}"
        "gtag('js', new Date());"
        f"gtag('config', '{tag_id}');"
    )
    return [loader, init]


def google_ads_conversion_event_js(label: str) -> str:
    """Return a JS one-liner that fires a Google Ads conversion event.

    Pure function: no side effects, no Reflex imports. Takes the
    conversion label (e.g. ``"EHgmCLWVmpscEM_1-K1D"``) and returns a
    JS string like::

        window.gtag && window.gtag('event', 'conversion', {
          'send_to': 'AW-18081528527/EHgmCLWVmpscEM_1-K1D',
          'value': 1.0,
          'currency': 'EUR'
        });

    The ``window.gtag &&`` prefix makes this a silent no-op when gtag.js
    hasn't loaded — which happens in two cases:
    1. ``settings.google_ads_tag_id`` is empty (no head components
       emitted, so the gtag global never exists)
    2. The user clicks signup before gtag.js finishes loading (rare but
       possible on slow networks)

    Returns an empty string when either ``label`` is empty OR
    ``settings.google_ads_tag_id`` is empty. Callers should check for
    the empty string and skip emitting ``rx.call_script`` entirely in
    that case — firing an empty script is wasteful WebSocket traffic.

    Why a pure function (not an rx.Component): conversion events fire
    from event handlers via ``rx.call_script``, not from the rendered
    DOM. The Reflex runtime expects a JS string payload, not a
    component. This also makes the function trivially unit-testable
    without spinning up a Reflex app.
    """
    if not label:
        return ""
    tag_id = settings.google_ads_tag_id.strip()
    if not tag_id:
        return ""
    # Currency is hardcoded EUR because the Datanika pricing page lists
    # all tiers in EUR and the Google Ads account is billed in EUR. If
    # we ever localize pricing, this should read from a new setting.
    # Value is a placeholder 1.0 — we don't have a per-signup dollar
    # value at this point in the funnel; Google's bidding can still
    # optimize against conversion *count* regardless of value.
    return (
        "window.gtag && window.gtag('event', 'conversion', {"
        f"'send_to': '{tag_id}/{label}', "
        "'value': 1.0, "
        "'currency': 'EUR'"
        "});"
    )

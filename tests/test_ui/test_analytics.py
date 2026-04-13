"""Tests for the Plausible analytics instrumentation (issue #92).

The analytics module exposes two things the rest of the app uses:

1. ``plausible_head_component()`` — returns an ``rx.script`` when analytics
   is fully configured, ``None`` otherwise. Called from ``datanika.py``'s
   ``rx.App(head_components=[...])`` list, where ``None`` is filtered out.
2. ``ANALYTICS_EVENTS`` — a frozen set of canonical event names. Any inline
   ``rx.script`` in the UI that calls ``window.plausible('...')`` must
   reference a name in this set. The scanner test below enforces that so
   a typo can't silently break a Growth dashboard.
"""

import re
from pathlib import Path

import pytest

import datanika.ui.analytics as analytics_module
from datanika.config import settings
from datanika.ui.analytics import (
    ANALYTICS_EVENTS,
    plausible_head_component,
)


@pytest.fixture
def reset_analytics_settings():
    """Snapshot + restore analytics settings around each test."""
    original_domain = settings.analytics_domain
    original_src = settings.analytics_script_src
    yield
    settings.analytics_domain = original_domain
    settings.analytics_script_src = original_src


class TestAnalyticsEvents:
    def test_catalog_is_a_set(self):
        assert isinstance(ANALYTICS_EVENTS, (set, frozenset))

    def test_catalog_contains_template_selected(self):
        assert "template_selected" in ANALYTICS_EVENTS

    def test_catalog_contains_template_prefill_applied(self):
        assert "template_prefill_applied" in ANALYTICS_EVENTS

    def test_catalog_has_no_surprising_extras(self):
        # If you're adding a new event, update this list AND the PR description
        # AND ping Growth so the dashboard picks it up. The catalog is the
        # single source of truth — don't let it drift from the UI scripts.
        expected = {
            "template_selected",
            "template_prefill_applied",
            # "template_first_run_triggered" is intentionally excluded — it's
            # a follow-up issue, not part of issue #92.
        }
        assert expected == ANALYTICS_EVENTS

    def test_event_names_are_snake_case_strings(self):
        for name in ANALYTICS_EVENTS:
            assert isinstance(name, str)
            assert re.fullmatch(r"[a-z][a-z0-9_]*", name), (
                f"{name!r} is not a valid snake_case event name"
            )


class TestPlausibleHeadComponent:
    def test_returns_none_when_both_config_values_empty(self, reset_analytics_settings):
        settings.analytics_domain = ""
        settings.analytics_script_src = ""
        assert plausible_head_component() is None

    def test_returns_none_when_only_domain_set(self, reset_analytics_settings):
        settings.analytics_domain = "app.datanika.io"
        settings.analytics_script_src = ""
        assert plausible_head_component() is None

    def test_returns_none_when_only_script_src_set(self, reset_analytics_settings):
        settings.analytics_domain = ""
        settings.analytics_script_src = "https://plausible.datanika.io/js/script.js"
        assert plausible_head_component() is None

    def test_returns_script_component_when_both_set(self, reset_analytics_settings):
        settings.analytics_domain = "app.datanika.io"
        settings.analytics_script_src = "https://plausible.datanika.io/js/script.js"
        component = plausible_head_component()
        assert component is not None

    def test_script_component_carries_domain_and_src(self, reset_analytics_settings):
        settings.analytics_domain = "app.datanika.io"
        settings.analytics_script_src = "https://plausible.datanika.io/js/script.js"
        component = plausible_head_component()
        rendered = str(component)
        # The rendered string should mention the configured domain and src.
        assert "app.datanika.io" in rendered
        assert "plausible.datanika.io" in rendered


# ---------------------------------------------------------------------------
# Scanner: every window.plausible('EVENT', ...) call in the UI code must
# reference an event name from ANALYTICS_EVENTS. Prevents typos from silently
# sending junk events to Growth's dashboard.
# ---------------------------------------------------------------------------

_UI_ROOT = Path(analytics_module.__file__).resolve().parent.parent / "ui"
_PLAUSIBLE_CALL_RE = re.compile(r"""window\.plausible\(\s*['"]([^'"]+)['"]""")


class TestInlineScriptEventCatalogSync:
    def test_every_plausible_call_uses_a_known_event_name(self):
        """Scan every .py file under datanika/ui/ for window.plausible(...)
        calls and assert the event name is in ANALYTICS_EVENTS. Catches
        typos like 'template_selected' → 'tempalte_selected' immediately.
        """
        found_names: set[str] = set()
        for py_file in _UI_ROOT.rglob("*.py"):
            # analytics.py is the *definition* of the catalog — its docstring
            # mentions example event names like 'NAME' that the regex would
            # otherwise pick up as real consumers. Skip it.
            if py_file.name == "analytics.py":
                continue
            text = py_file.read_text(encoding="utf-8")
            for match in _PLAUSIBLE_CALL_RE.finditer(text):
                found_names.add(match.group(1))

        # Sanity: if the scanner finds zero, either the instrumentation
        # isn't wired yet (before this PR) or the regex is broken. After
        # this PR lands there should be at least one plausible() call.
        assert found_names, (
            "No window.plausible(...) calls found in datanika/ui/*.py — "
            "instrumentation may have been removed by mistake"
        )

        unknown = found_names - ANALYTICS_EVENTS
        assert not unknown, (
            f"Unknown plausible event names used in UI code: {sorted(unknown)}. "
            f"Either add them to ANALYTICS_EVENTS in datanika/ui/analytics.py "
            f"or fix the typo. Known names: {sorted(ANALYTICS_EVENTS)}"
        )

"""Open-core regression guard.

The cloud plugin (private repo) owns all Plausible / Google Ads
instrumentation. Self-hosters cloning the open-source core must see
zero SaaS-specific references in the datanika/ package tree.

Originally defined in datanika-cloud/tests/test_analytics.py as
TestNoAnalyticsLeakIntoCore — ported to core CI so a core PR that
reintroduces analytics trips the test here, not only when a cloud PR
happens to run.

Regression for datanika-io/datanika-core issue #99.
"""

from pathlib import Path

FORBIDDEN_STRINGS = [
    "plausible_head_component",
    "google_ads_head_components",
    "google_ads_conversion_event_js",
    "analytics_domain",
    "analytics_script_src",
    "google_ads_tag_id",
    "google_ads_conversion_label_signup",
    "AW-18081528527",
    "plausible.datanika.io",
]


class TestNoAnalyticsLeakIntoCore:
    def test_no_plausible_or_gtag_references_in_core(self):
        import datanika as core_module

        core_root = Path(core_module.__file__).resolve().parent
        offenders: list[tuple[Path, str]] = []
        for py_file in core_root.rglob("*.py"):
            text = py_file.read_text(encoding="utf-8")
            for needle in FORBIDDEN_STRINGS:
                if needle in text:
                    offenders.append((py_file.relative_to(core_root), needle))
        assert not offenders, (
            "Open-core refactor regression (issue #99): the following "
            "SaaS-specific references leaked back into core:\n"
            + "\n".join(f"  {p} -> {n}" for p, n in offenders)
        )

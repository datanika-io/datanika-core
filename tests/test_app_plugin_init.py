"""Pins the two-phase cloud plugin init ordering in ``datanika.datanika``.

Issue #99. Plugin-contributed head components (analytics scripts) have
to reach ``rx.App(head_components=[...])`` at construction time, which
means the cloud plugin's ``bootstrap_cloud()`` must run BEFORE
``rx.App(...)`` is constructed, and the page/route-registration step
``init_cloud(app)`` must run AFTER.

If a future refactor collapses the two calls into one (or reverses
them), plugin head components silently disappear from the rendered
``<head>`` — which would break Plausible and Google Ads without
triggering any runtime error. That's the exact failure mode this test
catches.

The test is a source-scan (not a runtime assertion) because the
``datanika.datanika`` module has heavy top-level side effects
(scheduler, DB sessions) and we don't want to import it in a unit
test.
"""

import inspect
import re
from pathlib import Path

import datanika


def _datanika_py_source() -> str:
    path = Path(inspect.getfile(datanika)).parent / "datanika.py"
    return path.read_text(encoding="utf-8")


class TestCloudInitOrdering:
    def test_bootstrap_cloud_called_before_rx_app_construction(self):
        src = _datanika_py_source()
        # The bootstrap call registers head components via the plugin
        # registry, which datanika.py then reads into _head_components
        # BEFORE calling rx.App(...). Capture the three offsets and
        # assert the order.
        m_bootstrap = re.search(r"\bbootstrap_cloud\s*\(", src)
        m_app = re.search(r"\brx\.App\s*\(", src)
        m_init = re.search(r"\binit_cloud\s*\(\s*app\b", src)
        assert m_bootstrap, "datanika.py must call bootstrap_cloud() during module import"
        assert m_app, "datanika.py must construct rx.App(...)"
        assert m_init, "datanika.py must call init_cloud(app) after constructing rx.App"
        assert m_bootstrap.start() < m_app.start(), (
            "bootstrap_cloud() must run BEFORE rx.App(...) so plugin-contributed "
            "head components make it into the initial head_components list"
        )
        assert m_app.start() < m_init.start(), (
            "init_cloud(app) must run AFTER rx.App(...) — it needs the app instance "
            "to register pages and Starlette routes"
        )

    def test_no_direct_analytics_import_in_datanika_py(self):
        # Regression: issue #99. After the refactor core must not import
        # from datanika.ui.analytics (module deleted) and must not
        # reference Plausible / Google Ads by name. The plugin registry
        # is the only acceptable seam.
        src = _datanika_py_source()
        assert "datanika.ui.analytics" not in src
        assert "plausible_head_component" not in src
        assert "google_ads_head_components" not in src

    def test_head_components_list_includes_plugin_contributions(self):
        # Source-scan: the _head_components list (or equivalent) must be
        # extended with plugin_head_components() before rx.App is called,
        # otherwise registered components never reach the app.
        src = _datanika_py_source()
        assert "plugin_head_components" in src, (
            "datanika.py must read plugin_registry.plugin_head_components() "
            "and include the result in rx.App(head_components=[...])"
        )

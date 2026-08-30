"""The upgrade CTAs and the billing page must name the same route (#654).

Four call sites in core linked `/settings?tab=billing`. The billing page is
registered by the cloud plugin at `/settings/billing`, and core's `/settings`
reads no `tab` parameter at all — so every click that converts free → paid
landed on a page with no billing information on it, at the moment of highest
intent.

This is the same shape as #651: **two hand-maintained copies of one string.**
So the guard is that there is now one copy, in the seam module core and the
plugin already share, and these tests assert the link rather than the value.
The cloud side of the link is asserted in `datanika-cloud`'s own suite, which
is the only place that can see the route the page actually registers at.
"""

import ast
import inspect
import pathlib

import pytest

import datanika.services.email_service as email_service_module
import datanika.ui.components.quota_callout as quota_callout_module
import datanika.ui.components.volume_quota_modal as volume_quota_modal_module
import datanika.ui.pages.dashboard as dashboard_module
from datanika.plugin_registry import BILLING_ROUTE

#: Every module that links a user to billing. Named, not discovered, because a
#: discovery pass that silently finds nothing is the failure mode this whole
#: file exists to prevent.
CTA_MODULES = [
    quota_callout_module,
    volume_quota_modal_module,
    dashboard_module,
    email_service_module,
]


def _core_sources() -> list[pathlib.Path]:
    root = pathlib.Path(inspect.getfile(quota_callout_module)).parents[3] / "datanika"
    return sorted(root.rglob("*.py"))


class TestOneRouteNotTwo:
    def test_the_probe_reads_real_files(self):
        """Guard the guard — a bad root makes every scan below vacuous."""
        files = _core_sources()
        assert len(files) > 50, f"only found {len(files)} source files"

    def test_the_dead_url_is_in_no_string_core_actually_uses(self):
        """Count the instruction, not the phrase.

        The fixed files *mention* `?tab=billing` — in the comment explaining why
        the redirect exists, and in the docstring on the redirect itself. A
        substring scan flags those and reports work that is already done
        (WORKFLOW_RULES §4). So this looks at string **literals that are not
        docstrings**, which is what a link can actually be built from.
        """
        offenders = []
        for path in _core_sources():
            tree = ast.parse(path.read_text(encoding="utf-8"))
            docstrings = {
                id(node.body[0].value)
                for node in ast.walk(tree)
                if isinstance(
                    node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
                )
                and node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            }
            offenders += [
                f"{path.name}:{node.lineno}"
                for node in ast.walk(tree)
                if isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and "?tab=billing" in node.value
                and id(node) not in docstrings
            ]
        assert not offenders, (
            "`/settings?tab=billing` is not a route this app serves — core's "
            f"/settings reads no `tab` parameter: {offenders}"
        )

    def test_that_scan_would_catch_a_real_one(self):
        """Guard the guard: prove the AST walk sees a non-docstring literal."""
        tree = ast.parse('X = "/settings?tab=billing"\n')
        found = [
            n.value
            for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
        ]
        assert "/settings?tab=billing" in found

    @pytest.mark.parametrize("module", CTA_MODULES, ids=lambda m: m.__name__)
    def test_every_cta_module_names_the_shared_constant(self, module):
        tree = ast.parse(inspect.getsource(module))
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert "BILLING_ROUTE" in names, (
            f"{module.__name__} links to billing without using the shared constant; "
            "a second copy of the route is how #654 happened"
        )

    def test_the_route_is_absolute(self):
        assert BILLING_ROUTE.startswith("/"), BILLING_ROUTE
        assert "?" not in BILLING_ROUTE, "a query string is not a route"


class TestTheEmailCarriesAWorkingUrl:
    def test_the_upgrade_url_is_the_billing_route(self):
        """Read the URL the **service** builds, never one the test builds (#682 §3).

        This used to format `_QUOTA_WARNING_TEMPLATE` with an `upgrade_url` the
        test supplied and then assert that same string came back — so
        `email_service.py`'s own construction, the line the fix touched, was
        never executed and the assertion could not fail. Capturing `send` runs it.
        """
        sent = {}
        svc = email_service_module.EmailService(
            smtp_host="smtp.example.com",
            smtp_port=25,
            smtp_user="",
            smtp_password="",
            smtp_from_email="a@b.c",
            smtp_from_name="Datanika",
            smtp_use_tls=False,
            frontend_url="https://app.datanika.io/",
        )
        svc.send = lambda to, subject, html_body, text_body=None: (
            sent.update(html=html_body) or True
        )
        svc.send_quota_warning_email(
            "owner@example.com", "Pro", "runs", 9, 10, billing_enabled=True
        )
        assert f"https://app.datanika.io{BILLING_ROUTE}" in sent["html"]


class TestTheAlreadySentUrlStillWorks:
    """`/settings?tab=billing` is in email cloud has already queued.

    `datanika_cloud/billing/meter.py` calls `send_quota_warning_email_task` per
    org owner, and that template carried the wrong URL. Fixing the source does
    not reach an inbox, so `/settings` redirects the parameter — but only in the
    edition where there is a billing page to redirect to.
    """

    @staticmethod
    def _state(tab: str, edition: str):
        from unittest.mock import MagicMock, patch

        from datanika.ui.state.settings_state import SettingsState

        st = MagicMock()
        st.router.page.params = {"tab": tab} if tab else {}
        with patch("datanika.ui.state.settings_state.app_settings") as cfg:
            cfg.datanika_edition = edition
            # `.fn` — Reflex wraps a public method as an EventHandler, and
            # calling that with a stand-in `self` runs event-argument
            # validation instead of the body.
            return SettingsState.redirect_legacy_billing_tab.fn(st)

    def test_the_cloud_edition_redirects_to_billing(self):
        assert BILLING_ROUTE in str(self._state("billing", "cloud"))

    def test_the_core_edition_does_nothing(self):
        """There is no billing page in core — the plugin registers it."""
        assert self._state("billing", "core") is None

    def test_an_unrelated_tab_parameter_is_left_alone(self):
        assert self._state("members", "cloud") is None

    def test_no_tab_parameter_is_left_alone(self):
        assert self._state("", "cloud") is None

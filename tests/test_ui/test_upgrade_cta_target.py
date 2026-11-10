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
from types import SimpleNamespace

import pytest

import datanika.services.email_service as email_service_module
import datanika.ui.components.quota_callout as quota_callout_module
import datanika.ui.components.volume_quota_modal as volume_quota_modal_module
import datanika.ui.pages.dashboard as dashboard_module
from datanika import hooks
from datanika.errors import UserFacingError
from datanika.plugin_registry import BILLING_ROUTE
from datanika.ui.state.base_state import BaseState
from datanika.ui.state.dashboard_state import DashboardState, usage_context

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


def _quota_carrier():
    """Stand-in with the real ``BaseState._set_error`` bound on.

    Instantiating a Reflex state is not what this guard is. The method only
    assigns attributes. A copy of the predicate on the stand-in would stay
    green when the production line changed, which is the hole #711 exists
    to close.
    """

    class _Carrier:
        error_message = ""
        is_quota_error = False
        quota_metric = ""

    _Carrier._set_error = BaseState._set_error
    return _Carrier()


def _expr_names_quota(node: ast.AST | None) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "QuotaExceededError"
    if isinstance(node, ast.Attribute):
        return node.attr == "QuotaExceededError"
    if isinstance(node, ast.Call):
        return _expr_names_quota(node.func)
    return False


def _quota_exceeded_sites() -> list[str]:
    """Class definitions and raises. A docstring that names the class is not one.

    The production comparison is a string literal (``== "QuotaExceededError"``).
    Counting that string would make this red on the guard it is pinning.
    """
    sites: list[str] = []
    for path in _core_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            defined = isinstance(node, ast.ClassDef) and node.name == "QuotaExceededError"
            raised = isinstance(node, (ast.Raise, ast.Call)) and _expr_names_quota(
                node.exc if isinstance(node, ast.Raise) else node.func
            )
            if defined or raised:
                sites.append(f"{path.name}:{node.lineno}")
    return sites


def _dashboard_state_class() -> ast.ClassDef:
    path = pathlib.Path(inspect.getfile(DashboardState))
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "DashboardState":
            return node
    raise AssertionError("DashboardState is not in its own module")


def _field_default(state_cls: ast.ClassDef, field: str):
    for item in state_cls.body:
        if (
            isinstance(item, ast.AnnAssign)
            and isinstance(item.target, ast.Name)
            and item.target.id == field
        ):
            if isinstance(item.value, ast.Constant):
                return item.value.value
            return item.value
    return None


def _runs_limit_rhs(state_cls: ast.ClassDef) -> ast.AST | None:
    fn = next(
        (
            item
            for item in state_cls.body
            if isinstance(item, ast.AsyncFunctionDef) and item.name == "load_dashboard"
        ),
        None,
    )
    if fn is None:
        return None
    found = [
        node.value
        for node in ast.walk(fn)
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Attribute)
        and isinstance(node.targets[0].value, ast.Name)
        and node.targets[0].value.id == "self"
        and node.targets[0].attr == "runs_limit"
    ]
    if len(found) != 1:
        return None
    return found[0]


def _copies_context_key(node: ast.AST | None, var: str, key: str) -> bool:
    return (
        isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name)
        and node.value.id == var
        and isinstance(node.slice, ast.Constant)
        and node.slice.value == key
    )


def _runs_limit_core_supplies() -> int:
    """The allowance core's own handlers leave for the dashboard to copy.

    A cloud handler, or one a sibling test registered, is not this edition.
    A handler whose module is ``datanika.*`` is, and it runs.
    """
    event = "usage.get_summary"
    ctx = usage_context(1)
    saved = hooks._handlers.get(event)
    core_only = [
        handler
        for handler in (saved or [])
        if str(getattr(handler, "__module__", "")).startswith("datanika.")
    ]
    hooks._handlers[event] = list(core_only)
    try:
        hooks.emit(event, context=ctx)
    finally:
        if saved is None:
            hooks._handlers.pop(event, None)
        else:
            hooks._handlers[event] = saved
    return ctx["runs_limit"]


class TestCoreEditionPreconditionsAreUnreachable:
    """#711. One assertion per guard, on the precondition, not on a render.

    The core edition has no billing page. Each upgrade CTA is unreachable for
    its own reason. A rendered-tree absence would stay green if the page
    failed to build. Relaxing one guard has to turn exactly one of these red.
    """

    def test_a_core_user_facing_error_does_not_arm_the_quota_callout(self):
        """``is_quota_error`` arms the callout and the volume modal.

        Both read the same flag. A core ``UserFacingError`` is the refusal
        core actually raises; it must not set the flag. Core also must not
        define or raise ``QuotaExceededError``, which is the only name that does.
        """
        state = _quota_carrier()
        state._set_error(UserFacingError("connection limit reached"))
        sites = _quota_exceeded_sites()
        assert state.is_quota_error is False and sites == [], (state.is_quota_error, sites)

    def test_runs_percent_is_zero_when_the_allowance_is_not_positive(self):
        """The upgrade link renders at ``runs_percent >= 80``.

        The limit is set on the stand-in. This does not read the field
        default — a non-zero default is the other guard, and coupling them
        would turn both red together.
        """
        standin = SimpleNamespace(runs_limit=0, runs_used=80)
        assert DashboardState.runs_percent.fget(standin) == 0

    def test_the_core_edition_supplies_no_runs_allowance(self):
        """``runs_limit`` stays 0 unless the cloud hook fills it.

        Three clauses, one assertion: the field default, the value left after
        core's own ``usage.get_summary`` handlers run, and ``load_dashboard``
        copying that key rather than a literal. A test that checked only one
        of those would stay green when another was relaxed.
        """
        state_cls = _dashboard_state_class()
        reading = (
            _field_default(state_cls, "runs_limit"),
            _runs_limit_core_supplies(),
            _copies_context_key(_runs_limit_rhs(state_cls), "usage_ctx", "runs_limit"),
        )
        assert reading == (0, 0, True), reading

    def test_the_core_edition_quota_email_builds_no_upgrade_link(self, monkeypatch):
        """The Celery task passes no ``billing_enabled``. The default is the gate.

        Passing ``billing_enabled=False`` stays green if that default becomes
        True, and that call is already covered elsewhere. This one omits it.
        """
        monkeypatch.setattr(email_service_module.settings, "datanika_edition", "core")
        sent: dict = {}
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
        svc.send_quota_warning_email("owner@example.com", "Pro", "runs", 9, 10)
        html = sent.get("html", "")
        assert html and BILLING_ROUTE not in html, html

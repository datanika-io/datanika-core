import logging

import reflex as rx

from datanika.config import settings as _settings
from datanika.logging_config import setup_logging
from datanika.plugin_registry import plugin_head_components
from datanika.scheduler import scheduler_integration
from datanika.ui.pages.audit_logs import audit_logs_page
from datanika.ui.pages.auth_complete import auth_complete_page
from datanika.ui.pages.connections import connections_page
from datanika.ui.pages.dag import dag_page
from datanika.ui.pages.dashboard import dashboard_page
from datanika.ui.pages.forgot_password import forgot_password_page
from datanika.ui.pages.login import login_page
from datanika.ui.pages.model_detail import model_detail_page
from datanika.ui.pages.models import models_page
from datanika.ui.pages.oauth_consent import oauth_consent_page
from datanika.ui.pages.pipeline_templates import pipeline_templates_page
from datanika.ui.pages.pipelines import pipelines_page
from datanika.ui.pages.reset_password import reset_password_page
from datanika.ui.pages.runs import runs_page
from datanika.ui.pages.schedules import schedules_page
from datanika.ui.pages.settings import settings_page
from datanika.ui.pages.signup import signup_page
from datanika.ui.pages.sql_editor import sql_editor_page
from datanika.ui.pages.transformations import transformations_page
from datanika.ui.pages.uploads import uploads_page
from datanika.ui.state.account_state import AccountState
from datanika.ui.state.api_key_state import ApiKeyState
from datanika.ui.state.audit_state import AuditState
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.base_state import get_sync_session
from datanika.ui.state.connection_state import ConnectionState
from datanika.ui.state.dag_state import DagState
from datanika.ui.state.dashboard_state import DashboardState
from datanika.ui.state.mcp_consent_state import McpConsentState
from datanika.ui.state.model_detail_state import ModelDetailState
from datanika.ui.state.model_state import ModelState
from datanika.ui.state.notification_center_state import NotificationCenterState
from datanika.ui.state.notification_state import NotificationState
from datanika.ui.state.onboarding_state import OnboardingState
from datanika.ui.state.password_reset_state import PasswordResetState
from datanika.ui.state.pipeline_state import PipelineState
from datanika.ui.state.run_state import RunState
from datanika.ui.state.schedule_state import ScheduleState
from datanika.ui.state.settings_state import SettingsState
from datanika.ui.state.transformation_state import TransformationState
from datanika.ui.state.upload_state import UploadState

setup_logging(debug=_settings.debug)

# Two-phase plugin init (issue #99):
#
# Phase 1 — ``bootstrap_cloud()`` runs BEFORE ``rx.App(...)``. It
# registers plugin-contributed head components into ``plugin_registry``
# and subscribes hook handlers. No ``app`` instance is required. This is
# where SaaS-specific instrumentation (Plausible, Google Ads) is wired
# — all of it lives in ``datanika-cloud`` and reaches the rendered head
# via the registry seam below.
#
# Phase 2 — ``init_cloud(app)`` runs AFTER ``rx.App(...)``. It registers
# pages, Starlette routes, sidebar links, and i18n overrides that need
# the app instance.
if _settings.datanika_edition == "cloud":
    from datanika_cloud.plugin import bootstrap_cloud  # noqa: E402

    bootstrap_cloud()

_head_components: list[rx.Component] = [
    rx.el.link(rel="icon", href="/favicon.ico", type="image/x-icon"),
]
_head_components.extend(plugin_head_components())

app = rx.App(head_components=_head_components)

if _settings.datanika_edition == "cloud":
    from datanika_cloud.plugin import init_cloud  # noqa: E402

    init_cloud(app)

# Start APScheduler and sync all active schedules from DB
scheduler_integration.start()
with get_sync_session() as _session:
    scheduler_integration.sync_all(_session)

# Public pages
app.add_page(
    login_page,
    route="/login",
    title="Login | Datanika",
)
app.add_page(
    signup_page,
    route="/signup",
    title="Sign Up | Datanika",
    on_load=[AuthState.prefill_invite_email],
)
# Account recovery (core#623). Public by design — a signed-out user is the only
# kind that can need them, so neither carries AuthState.check_auth.
#
# These are Reflex *pages*, not backend Starlette routes. The Apache vhost
# forwards an explicit prefix list to :8000 and everything else to the frontend,
# so a backend route outside /api/ silently serves the SPA instead of itself.
# A page needs no vhost change at all.
app.add_page(
    forgot_password_page,
    route="/forgot-password",
    title="Reset your password | Datanika",
    on_load=[PasswordResetState.check_availability],
)
app.add_page(
    reset_password_page,
    route="/reset-password",
    title="Set a new password | Datanika",
    # Validates the token for rendering only, and never consumes it — mail
    # scanners prefetch this URL before the recipient ever clicks it.
    on_load=[PasswordResetState.load_token],
)

# Protected pages
app.add_page(
    dashboard_page,
    route="/",
    title="Dashboard | Datanika",
    on_load=[
        AuthState.check_auth,
        DashboardState.load_dashboard,
        OnboardingState.load_checklist,
        NotificationCenterState.load_notifications,
    ],
)
app.add_page(
    connections_page,
    route="/connections",
    title="Connections | Datanika",
    on_load=[
        AuthState.check_auth,
        ConnectionState.load_connections,
        ConnectionState.load_template_from_query,
    ],
)
app.add_page(
    uploads_page,
    route="/uploads",
    title="Uploads | Datanika",
    on_load=[AuthState.check_auth, UploadState.load_uploads],
)
app.add_page(
    transformations_page,
    route="/transformations",
    title="Transformations | Datanika",
    on_load=[AuthState.check_auth, TransformationState.load_transformations],
)
app.add_page(
    sql_editor_page,
    route="/transformations/sql-editor",
    title="SQL Editor | Datanika",
    on_load=[AuthState.check_auth, TransformationState.load_transformations],
)
app.add_page(
    pipelines_page,
    route="/pipelines",
    title="Pipelines | Datanika",
    on_load=[AuthState.check_auth, PipelineState.load_pipelines],
)
app.add_page(
    pipeline_templates_page,
    route="/pipelines/templates",
    title="Pipeline Templates | Datanika",
    on_load=[AuthState.check_auth],
)
app.add_page(
    schedules_page,
    route="/schedules",
    title="Schedules | Datanika",
    on_load=[AuthState.check_auth, ScheduleState.load_schedules],
)
app.add_page(
    runs_page,
    route="/runs",
    title="Runs | Datanika",
    on_load=[AuthState.check_auth, RunState.load_runs],
)
app.add_page(
    dag_page,
    route="/dag",
    title="Dependencies | Datanika",
    on_load=[AuthState.check_auth, DagState.load_dependencies],
)
app.add_page(
    models_page,
    route="/models",
    title="Models | Datanika",
    on_load=[AuthState.check_auth, ModelState.load_models],
)
app.add_page(
    model_detail_page,
    route="/models/[id]",
    title="Model Detail | Datanika",
    on_load=[AuthState.check_auth, ModelDetailState.load_model_detail],
)
app.add_page(
    settings_page,
    route="/settings",
    title="Settings | Datanika",
    on_load=[
        AuthState.check_auth,
        # `/settings?tab=billing` is in already-sent quota-warning email (#654).
        SettingsState.redirect_legacy_billing_tab,
        # Decides between "Change password" and "Set a password" (core#623).
        AccountState.load_account,
        SettingsState.load_settings,
        ApiKeyState.load_api_keys,
        NotificationState.load_channels,
    ],
)
app.add_page(
    audit_logs_page,
    route="/audit-log",
    title="Audit Log | Datanika",
    on_load=[AuthState.check_auth, AuditState.load_audit_logs],
)

# OAuth completion page (public — picks up tokens from URL after OAuth callback)
app.add_page(
    auth_complete_page,
    route="/auth/complete",
    title="Signing In... | Datanika",
    on_load=[AuthState.handle_oauth_complete],
)

# MCP OAuth consent screen (Remote-MCP P2, #394). Not behind AuthState.check_auth:
# the request lives in this page's query string, and check_auth's bare
# rx.redirect("/login") would drop it. load_consent runs the same gate and
# bounces to /login?next=<this URL> so the flow resumes instead of stranding
# the MCP client mid-handshake.
app.add_page(
    oauth_consent_page,
    route="/oauth/consent",
    title="Authorize Access | Datanika",
    on_load=[McpConsentState.load_consent],
)

# Mount OAuth API routes on the Starlette backend
from datanika.services.oauth_routes import oauth_routes  # noqa: E402

for _route in oauth_routes:
    app._api.routes.append(_route)

# Mount email verification routes
from datanika.services.email_routes import email_routes  # noqa: E402

for _route in email_routes:
    app._api.routes.append(_route)

# Mount SSO (SAML/OIDC) routes
from datanika.services.sso_routes import sso_routes  # noqa: E402

for _route in sso_routes:
    app._api.routes.append(_route)

# Mount REST API v1 routes
from datanika.services.api_v1_routes import api_v1_routes  # noqa: E402

for _route in api_v1_routes:
    app._api.routes.append(_route)

# Mount discovery (meta) routes — /api/v1/meta/*
from datanika.services.meta_routes import meta_routes  # noqa: E402

for _route in meta_routes:
    app._api.routes.append(_route)

# Mount health check routes
from datanika.services.health_routes import health_routes  # noqa: E402

for _route in health_routes:
    app._api.routes.append(_route)

# Mount OpenAPI/Swagger docs
from datanika.services.openapi import openapi_routes  # noqa: E402

for _route in openapi_routes:
    app._api.routes.append(_route)

# Mount agent discovery docs (/llms.txt, /api/v1/agent-guide.md).
# Also materialise /llms.txt into the Reflex assets dir so the frontend
# (nginx → :3000) serves it at the root URL published in our docs.
# See the module docstring in services/agent_docs.py for why this is
# needed on top of the Starlette route registration. Issue #124.
from datanika.services.agent_docs import agent_doc_routes, write_llms_txt_asset  # noqa: E402

for _route in agent_doc_routes:
    app._api.routes.append(_route)

write_llms_txt_asset()

# Mount Prometheus metrics endpoint and middleware
from datanika.services.metrics import PrometheusMiddleware, metrics_routes  # noqa: E402

for _route in metrics_routes:
    app._api.routes.append(_route)
app._api.add_middleware(PrometheusMiddleware)

# Mount remote MCP endpoint (/mcp) — Streamable HTTP, bearer=API-key, read-only
# (Remote-MCP P1, #370). The datanika-mcp tool-surface package is installed in
# the Docker image (``uv pip install ./datanika-mcp``) but is optional in
# dev/CI, where it's exercised via its own tests — so skip cleanly if it isn't
# importable rather than failing app startup.
try:
    from datanika.services.mcp_routes import mcp_lifespan, mcp_routes  # noqa: E402

    for _route in mcp_routes:
        app._api.routes.append(_route)
    app.register_lifespan_task(mcp_lifespan)
    logging.getLogger(__name__).info("Mounted remote MCP endpoint at /mcp (read-only)")
except ImportError as _mcp_exc:
    logging.getLogger(__name__).warning(
        "datanika-mcp not installed; /mcp endpoint not mounted (%s)", _mcp_exc
    )

# Mount the MCP OAuth 2.1 authorization server (Remote-MCP P2, #393) —
# discovery, dynamic client registration, authorize/consent, token. Unlike
# /mcp this has no dependency on the datanika-mcp package, so it mounts
# unconditionally: the AS is pure protocol over our own models.
from datanika.services.mcp_oauth_routes import mcp_oauth_routes  # noqa: E402

for _route in mcp_oauth_routes:
    app._api.routes.append(_route)

# Register every core-side hook subscriber (runs + quota + V2 P5 charge
# events + external-channel dispatch). Delegated to
# ``services._register_hooks`` so the Celery worker can call the same
# function from ``datanika/tasks/celery_app.py`` — without that, hooks
# emitted from Celery tasks (e.g. ``charge_cycle_overages``) would fire
# into an empty handler dict in the worker process. See #287.
from datanika.services._register_hooks import register_all_core_hooks  # noqa: E402

register_all_core_hooks()

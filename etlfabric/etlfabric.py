import reflex as rx

from etlfabric.ui.pages.auth_complete import auth_complete_page
from etlfabric.ui.pages.connections import connections_page
from etlfabric.ui.pages.dag import dag_page
from etlfabric.ui.pages.dashboard import dashboard_page
from etlfabric.ui.pages.login import login_page
from etlfabric.ui.pages.pipelines import pipelines_page
from etlfabric.ui.pages.runs import runs_page
from etlfabric.ui.pages.schedules import schedules_page
from etlfabric.ui.pages.settings import settings_page
from etlfabric.ui.pages.signup import signup_page
from etlfabric.ui.pages.transformations import transformations_page
from etlfabric.ui.state.auth_state import AuthState
from etlfabric.ui.state.connection_state import ConnectionState
from etlfabric.ui.state.dag_state import DagState
from etlfabric.ui.state.dashboard_state import DashboardState
from etlfabric.ui.state.pipeline_state import PipelineState
from etlfabric.ui.state.run_state import RunState
from etlfabric.ui.state.schedule_state import ScheduleState
from etlfabric.ui.state.settings_state import SettingsState
from etlfabric.ui.state.transformation_state import TransformationState

app = rx.App()

# Public pages
app.add_page(
    login_page,
    route="/login",
    title="Login | ETL Fabric",
    on_load=[AuthState.clear_auth_error],
)
app.add_page(
    signup_page,
    route="/signup",
    title="Sign Up | ETL Fabric",
    on_load=[AuthState.clear_auth_error],
)

# Protected pages
app.add_page(
    dashboard_page,
    route="/",
    title="Dashboard | ETL Fabric",
    on_load=[AuthState.check_auth, DashboardState.load_dashboard],
)
app.add_page(
    connections_page,
    route="/connections",
    title="Connections | ETL Fabric",
    on_load=[AuthState.check_auth, ConnectionState.load_connections],
)
app.add_page(
    pipelines_page,
    route="/pipelines",
    title="Pipelines | ETL Fabric",
    on_load=[AuthState.check_auth, PipelineState.load_pipelines],
)
app.add_page(
    transformations_page,
    route="/transformations",
    title="Transformations | ETL Fabric",
    on_load=[AuthState.check_auth, TransformationState.load_transformations],
)
app.add_page(
    schedules_page,
    route="/schedules",
    title="Schedules | ETL Fabric",
    on_load=[AuthState.check_auth, ScheduleState.load_schedules],
)
app.add_page(
    runs_page,
    route="/runs",
    title="Runs | ETL Fabric",
    on_load=[AuthState.check_auth, RunState.load_runs],
)
app.add_page(
    dag_page,
    route="/dag",
    title="Dependencies | ETL Fabric",
    on_load=[AuthState.check_auth, DagState.load_dependencies],
)
app.add_page(
    settings_page,
    route="/settings",
    title="Settings | ETL Fabric",
    on_load=[AuthState.check_auth, SettingsState.load_settings],
)

# OAuth completion page (public — picks up tokens from URL after OAuth callback)
app.add_page(
    auth_complete_page,
    route="/auth/complete",
    title="Signing In... | ETL Fabric",
    on_load=[AuthState.handle_oauth_complete],
)

# Mount OAuth API routes on the Starlette backend
from etlfabric.services.oauth_routes import oauth_routes  # noqa: E402

for _route in oauth_routes:
    app._api.routes.append(_route)

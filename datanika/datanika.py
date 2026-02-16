import reflex as rx

from datanika.ui.pages.auth_complete import auth_complete_page
from datanika.ui.pages.connections import connections_page
from datanika.ui.pages.dag import dag_page
from datanika.ui.pages.dashboard import dashboard_page
from datanika.ui.pages.login import login_page
from datanika.ui.pages.pipelines import pipelines_page
from datanika.ui.pages.runs import runs_page
from datanika.ui.pages.schedules import schedules_page
from datanika.ui.pages.settings import settings_page
from datanika.ui.pages.signup import signup_page
from datanika.ui.pages.transformations import transformations_page
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.connection_state import ConnectionState
from datanika.ui.state.dag_state import DagState
from datanika.ui.state.dashboard_state import DashboardState
from datanika.ui.state.pipeline_state import PipelineState
from datanika.ui.state.run_state import RunState
from datanika.ui.state.schedule_state import ScheduleState
from datanika.ui.state.settings_state import SettingsState
from datanika.ui.state.transformation_state import TransformationState

app = rx.App()

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
)

# Protected pages
app.add_page(
    dashboard_page,
    route="/",
    title="Dashboard | Datanika",
    on_load=[AuthState.check_auth, DashboardState.load_dashboard],
)
app.add_page(
    connections_page,
    route="/connections",
    title="Connections | Datanika",
    on_load=[AuthState.check_auth, ConnectionState.load_connections],
)
app.add_page(
    pipelines_page,
    route="/pipelines",
    title="Pipelines | Datanika",
    on_load=[AuthState.check_auth, PipelineState.load_pipelines],
)
app.add_page(
    transformations_page,
    route="/transformations",
    title="Transformations | Datanika",
    on_load=[AuthState.check_auth, TransformationState.load_transformations],
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
    settings_page,
    route="/settings",
    title="Settings | Datanika",
    on_load=[AuthState.check_auth, SettingsState.load_settings],
)

# OAuth completion page (public — picks up tokens from URL after OAuth callback)
app.add_page(
    auth_complete_page,
    route="/auth/complete",
    title="Signing In... | Datanika",
    on_load=[AuthState.handle_oauth_complete],
)

# Mount OAuth API routes on the Starlette backend
from datanika.services.oauth_routes import oauth_routes  # noqa: E402

for _route in oauth_routes:
    app._api.routes.append(_route)

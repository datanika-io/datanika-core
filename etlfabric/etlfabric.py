import reflex as rx

from etlfabric.ui.pages.connections import connections_page
from etlfabric.ui.pages.dag import dag_page
from etlfabric.ui.pages.dashboard import dashboard_page
from etlfabric.ui.pages.pipelines import pipelines_page
from etlfabric.ui.pages.runs import runs_page
from etlfabric.ui.pages.schedules import schedules_page
from etlfabric.ui.pages.transformations import transformations_page
from etlfabric.ui.state.connection_state import ConnectionState
from etlfabric.ui.state.dag_state import DagState
from etlfabric.ui.state.dashboard_state import DashboardState
from etlfabric.ui.state.pipeline_state import PipelineState
from etlfabric.ui.state.run_state import RunState
from etlfabric.ui.state.schedule_state import ScheduleState
from etlfabric.ui.state.transformation_state import TransformationState

app = rx.App()
app.add_page(
    dashboard_page,
    route="/",
    title="Dashboard | ETL Fabric",
    on_load=DashboardState.load_dashboard,
)
app.add_page(
    connections_page,
    route="/connections",
    title="Connections | ETL Fabric",
    on_load=ConnectionState.load_connections,
)
app.add_page(
    pipelines_page,
    route="/pipelines",
    title="Pipelines | ETL Fabric",
    on_load=PipelineState.load_pipelines,
)
app.add_page(
    transformations_page,
    route="/transformations",
    title="Transformations | ETL Fabric",
    on_load=TransformationState.load_transformations,
)
app.add_page(
    schedules_page,
    route="/schedules",
    title="Schedules | ETL Fabric",
    on_load=ScheduleState.load_schedules,
)
app.add_page(
    runs_page,
    route="/runs",
    title="Runs | ETL Fabric",
    on_load=RunState.load_runs,
)
app.add_page(
    dag_page,
    route="/dag",
    title="Dependencies | ETL Fabric",
    on_load=DagState.load_dependencies,
)

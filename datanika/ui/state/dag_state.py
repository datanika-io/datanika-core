"""DAG (dependency) state for Reflex UI."""

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.services.connection_service import ConnectionService
from datanika.services.dependency_service import DependencyService
from datanika.services.encryption import EncryptionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from datanika.ui.state.base_state import BaseState, get_sync_session


class DependencyItem(BaseModel):
    id: int = 0
    upstream_type: str = ""
    upstream_id: int = 0
    upstream_name: str = ""
    downstream_type: str = ""
    downstream_id: int = 0
    downstream_name: str = ""


class DagState(BaseState):
    dependencies: list[DependencyItem] = []
    form_upstream_type: str = "upload"
    form_upstream_name: str = ""
    form_downstream_type: str = "transformation"
    form_downstream_name: str = ""
    # Upstream combobox
    upstream_options: list[str] = []
    upstream_suggestions: list[str] = []
    show_upstream_suggestions: bool = False
    upstream_suggestion_index: int = -1
    # Downstream combobox
    downstream_options: list[str] = []
    downstream_suggestions: list[str] = []
    show_downstream_suggestions: bool = False
    downstream_suggestion_index: int = -1
    # Internal name→ID lookup: {"upload": {"name": id}, ...}
    _name_to_id: dict[str, dict[str, int]] = {}

    async def set_form_upstream_type(self, value: str):
        self.form_upstream_type = value
        self.form_upstream_name = ""
        self.upstream_options = sorted(self._name_to_id.get(value, {}).keys())
        self.upstream_suggestions = []
        self.show_upstream_suggestions = False
        self.upstream_suggestion_index = -1

    def set_form_upstream_name(self, value: str):
        self.form_upstream_name = value
        if value.strip():
            query = value.strip().lower()
            self.upstream_suggestions = [
                n for n in self.upstream_options if query in n.lower()
            ]
            self.show_upstream_suggestions = len(self.upstream_suggestions) > 0
            self.upstream_suggestion_index = 0 if self.upstream_suggestions else -1
        else:
            self.upstream_suggestions = []
            self.show_upstream_suggestions = False
            self.upstream_suggestion_index = -1

    def select_upstream_suggestion(self, name: str):
        self.form_upstream_name = name
        self.upstream_suggestions = []
        self.show_upstream_suggestions = False
        self.upstream_suggestion_index = -1

    def upstream_nav_up(self):
        if not self.show_upstream_suggestions or not self.upstream_suggestions:
            return
        self.upstream_suggestion_index = max(self.upstream_suggestion_index - 1, 0)

    def upstream_nav_down(self):
        if not self.show_upstream_suggestions or not self.upstream_suggestions:
            return
        self.upstream_suggestion_index = min(
            self.upstream_suggestion_index + 1, len(self.upstream_suggestions) - 1
        )

    def upstream_select_current(self):
        if self.show_upstream_suggestions and 0 <= self.upstream_suggestion_index < len(
            self.upstream_suggestions
        ):
            self.select_upstream_suggestion(
                self.upstream_suggestions[self.upstream_suggestion_index]
            )

    def upstream_dismiss(self):
        self.show_upstream_suggestions = False
        self.upstream_suggestions = []
        self.upstream_suggestion_index = -1

    async def set_form_downstream_type(self, value: str):
        self.form_downstream_type = value
        self.form_downstream_name = ""
        self.downstream_options = sorted(self._name_to_id.get(value, {}).keys())
        self.downstream_suggestions = []
        self.show_downstream_suggestions = False
        self.downstream_suggestion_index = -1

    def set_form_downstream_name(self, value: str):
        self.form_downstream_name = value
        if value.strip():
            query = value.strip().lower()
            self.downstream_suggestions = [
                n for n in self.downstream_options if query in n.lower()
            ]
            self.show_downstream_suggestions = len(self.downstream_suggestions) > 0
            self.downstream_suggestion_index = (
                0 if self.downstream_suggestions else -1
            )
        else:
            self.downstream_suggestions = []
            self.show_downstream_suggestions = False
            self.downstream_suggestion_index = -1

    def select_downstream_suggestion(self, name: str):
        self.form_downstream_name = name
        self.downstream_suggestions = []
        self.show_downstream_suggestions = False
        self.downstream_suggestion_index = -1

    def downstream_nav_up(self):
        if not self.show_downstream_suggestions or not self.downstream_suggestions:
            return
        self.downstream_suggestion_index = max(self.downstream_suggestion_index - 1, 0)

    def downstream_nav_down(self):
        if not self.show_downstream_suggestions or not self.downstream_suggestions:
            return
        self.downstream_suggestion_index = min(
            self.downstream_suggestion_index + 1,
            len(self.downstream_suggestions) - 1,
        )

    def downstream_select_current(self):
        if self.show_downstream_suggestions and 0 <= self.downstream_suggestion_index < len(
            self.downstream_suggestions
        ):
            self.select_downstream_suggestion(
                self.downstream_suggestions[self.downstream_suggestion_index]
            )

    def downstream_dismiss(self):
        self.show_downstream_suggestions = False
        self.downstream_suggestions = []
        self.downstream_suggestion_index = -1

    def _get_service(self) -> DependencyService:
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()
        return DependencyService(upload_svc, transform_svc)

    @staticmethod
    def _resolve_node_name(
        node_type: str, node_id: int,
        upload_names: dict, trans_names: dict, pipeline_names: dict | None = None,
    ) -> str:
        if node_type == "upload":
            name = upload_names.get(node_id, f"#{node_id}")
            return f"upload: {name}"
        if node_type == "pipeline":
            name = (pipeline_names or {}).get(node_id, f"#{node_id}")
            return f"pipeline: {name}"
        name = trans_names.get(node_id, f"#{node_id}")
        return f"transformation: {name}"

    async def _load_node_options(self):
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()
        pipeline_svc = PipelineService()
        lookup: dict[str, dict[str, int]] = {
            "upload": {},
            "transformation": {},
            "pipeline": {},
        }
        with get_sync_session() as session:
            for u in upload_svc.list_uploads(session, org_id):
                lookup["upload"][u.name] = u.id
            for t in transform_svc.list_transformations(session, org_id):
                lookup["transformation"][t.name] = t.id
            for p in pipeline_svc.list_pipelines(session, org_id):
                lookup["pipeline"][p.name] = p.id
        self._name_to_id = lookup
        self.upstream_options = sorted(lookup.get(self.form_upstream_type, {}).keys())
        self.downstream_options = sorted(
            lookup.get(self.form_downstream_type, {}).keys()
        )
        self.upstream_suggestions = []
        self.show_upstream_suggestions = False
        self.upstream_suggestion_index = -1
        self.downstream_suggestions = []
        self.show_downstream_suggestions = False
        self.downstream_suggestion_index = -1

    async def load_dependencies(self):
        org_id = await self._get_org_id()
        svc = self._get_service()

        # Build name lookups
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()
        pipeline_svc = PipelineService()

        with get_sync_session() as session:
            uploads = upload_svc.list_uploads(session, org_id)
            upload_names = {u.id: u.name for u in uploads}
            transformations = transform_svc.list_transformations(session, org_id)
            trans_names = {t.id: t.name for t in transformations}
            pipelines = pipeline_svc.list_pipelines(session, org_id)
            pipeline_names = {p.id: p.name for p in pipelines}

            rows = svc.list_dependencies(session, org_id)
            self.dependencies = [
                DependencyItem(
                    id=d.id,
                    upstream_type=d.upstream_type.value,
                    upstream_id=d.upstream_id,
                    upstream_name=self._resolve_node_name(
                        d.upstream_type.value, d.upstream_id,
                        upload_names, trans_names, pipeline_names,
                    ),
                    downstream_type=d.downstream_type.value,
                    downstream_id=d.downstream_id,
                    downstream_name=self._resolve_node_name(
                        d.downstream_type.value, d.downstream_id,
                        upload_names, trans_names, pipeline_names,
                    ),
                )
                for d in rows
            ]
        self.error_message = ""
        await self._load_node_options()

    async def add_dependency(self):
        org_id = await self._get_org_id()
        svc = self._get_service()
        up_lookup = self._name_to_id.get(self.form_upstream_type, {})
        down_lookup = self._name_to_id.get(self.form_downstream_type, {})
        upstream_id = up_lookup.get(self.form_upstream_name)
        downstream_id = down_lookup.get(self.form_downstream_name)
        if upstream_id is None or downstream_id is None:
            self.error_message = "Node not found — select a name from the list"
            return
        try:
            with get_sync_session() as session:
                svc.add_dependency(
                    session,
                    org_id,
                    NodeType(self.form_upstream_type),
                    upstream_id,
                    NodeType(self.form_downstream_type),
                    downstream_id,
                )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to add dependency")
            return
        self.form_upstream_name = ""
        self.form_downstream_name = ""
        self.error_message = ""
        await self.load_dependencies()

    async def remove_dependency(self, dep_id: int):
        org_id = await self._get_org_id()
        svc = self._get_service()
        with get_sync_session() as session:
            svc.remove_dependency(session, org_id, dep_id)
            session.commit()
        await self.load_dependencies()

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
    check_timeframe_value: str = ""
    check_timeframe_unit: str = ""
    check_timeframe_display: str = ""


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
    # Timeframe form fields
    form_check_timeframe_value: str = ""
    form_check_timeframe_unit: str = "minutes"
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
            self.upstream_suggestions = [n for n in self.upstream_options if query in n.lower()]
            self.show_upstream_suggestions = len(self.upstream_suggestions) > 0
            self.upstream_suggestion_index = 0 if self.upstream_suggestions else -1
        else:
            self.upstream_suggestions = list(self.upstream_options)
            self.show_upstream_suggestions = len(self.upstream_suggestions) > 0
            self.upstream_suggestion_index = 0 if self.upstream_suggestions else -1

    def show_upstream_all(self):
        self.upstream_suggestions = list(self.upstream_options)
        self.show_upstream_suggestions = len(self.upstream_suggestions) > 0
        self.upstream_suggestion_index = 0 if self.upstream_suggestions else -1

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
            self.downstream_suggestions = [n for n in self.downstream_options if query in n.lower()]
            self.show_downstream_suggestions = len(self.downstream_suggestions) > 0
            self.downstream_suggestion_index = 0 if self.downstream_suggestions else -1
        else:
            self.downstream_suggestions = list(self.downstream_options)
            self.show_downstream_suggestions = len(self.downstream_suggestions) > 0
            self.downstream_suggestion_index = 0 if self.downstream_suggestions else -1

    def show_downstream_all(self):
        self.downstream_suggestions = list(self.downstream_options)
        self.show_downstream_suggestions = len(self.downstream_suggestions) > 0
        self.downstream_suggestion_index = 0 if self.downstream_suggestions else -1

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

    async def _actor_id(self) -> int:
        """The acting user, for the audit row's ``user_id`` (core#934).

        Mirrors ``BaseState._get_org_id`` — same local import, for the same reason: a
        module-level ``AuthState`` import here would close an import cycle through
        ``base_state``.
        """
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        return auth.current_user.id

    @staticmethod
    def _edge_payload(
        upstream_type: str,
        upstream_id: int,
        upstream_name: str,
        downstream_type: str,
        downstream_id: int,
        downstream_name: str,
        tf_value: int | None = None,
        tf_unit: str | None = None,
    ) -> dict:
        """The audit payload for one edge (SPEC_AUDIT_TRAIL §2.5, §3.2).

        Both ends are named as well as identified. ``#12`` identifies an edge to nobody —
        which is the argument ``_remove_dependency_dialog`` already makes for the *prompt*;
        core#851 accepted it there and left the *record* with nothing but the id.

        ⚠️ **A name is a label at a point in time; the id is what stays resolvable.** Both
        belong in the row and neither substitutes for the other, so a name that cannot be
        resolved is written as ``""`` rather than omitted — a key that is *sometimes absent*
        makes every future reader write a ``.get()``, and core#694 establishes there are no
        readers yet, which makes this the cheapest moment to fix the shape.

        ⚠️ Keys are **prefixed and specific** (§2.4). ``PII_PAYLOAD_KEYS`` is nominal, so a
        bare ``name`` would silently become a redaction target the day any ``*_pii`` table
        gains a column called ``name`` — and the payload would start writing ``[REDACTED]``
        into a table nothing reads, so nothing would contradict it.
        """
        payload = {
            "upstream_type": upstream_type,
            "upstream_id": upstream_id,
            "upstream_name": upstream_name,
            "downstream_type": downstream_type,
            "downstream_id": downstream_id,
            "downstream_name": downstream_name,
        }
        if tf_value is not None:
            payload["check_timeframe_value"] = tf_value
            payload["check_timeframe_unit"] = tf_unit
        return payload

    def _get_service(self) -> DependencyService:
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()
        return DependencyService(upload_svc, transform_svc)

    @staticmethod
    def _resolve_node_name(
        node_type: str,
        node_id: int,
        upload_names: dict,
        trans_names: dict,
        pipeline_names: dict | None = None,
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
        self.downstream_options = sorted(lookup.get(self.form_downstream_type, {}).keys())
        self.upstream_suggestions = []
        self.show_upstream_suggestions = False
        self.upstream_suggestion_index = -1
        self.downstream_suggestions = []
        self.show_downstream_suggestions = False
        self.downstream_suggestion_index = -1

    async def load_dependencies(self):
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        user_id = auth.current_user.id or 0
        if org_id == 0 or user_id == 0:
            return

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
                        d.upstream_type.value,
                        d.upstream_id,
                        upload_names,
                        trans_names,
                        pipeline_names,
                    ),
                    downstream_type=d.downstream_type.value,
                    downstream_id=d.downstream_id,
                    downstream_name=self._resolve_node_name(
                        d.downstream_type.value,
                        d.downstream_id,
                        upload_names,
                        trans_names,
                        pipeline_names,
                    ),
                    check_timeframe_value=str(d.check_timeframe_value or ""),
                    check_timeframe_unit=d.check_timeframe_unit or "",
                    check_timeframe_display=(
                        f"{d.check_timeframe_value} {d.check_timeframe_unit}"
                        if d.check_timeframe_value
                        else ""
                    ),
                )
                for d in rows
            ]
        self.error_message = ""
        await self._load_node_options()

    async def add_dependency(self):
        # core#851. `DagState` was the only state class in the product with no
        # role check on either of its mutating handlers, and
        # `test_rbac_enforcement.py`'s EXPECTED_ROLES had no `dag_state` entry,
        # so nothing had ever looked. Editing the run graph is an editor
        # action for the same reason saving a pipeline is; removing an edge is
        # an admin action for the same reason deleting one is.
        if not await self._check_role("editor"):
            return
        org_id = await self._get_org_id()
        svc = self._get_service()
        up_lookup = self._name_to_id.get(self.form_upstream_type, {})
        down_lookup = self._name_to_id.get(self.form_downstream_type, {})
        upstream_id = up_lookup.get(self.form_upstream_name)
        downstream_id = down_lookup.get(self.form_downstream_name)
        if upstream_id is None or downstream_id is None:
            self.error_message = "Node not found — select a name from the list"
            return
        # Parse optional timeframe
        tf_value = None
        tf_unit = None
        if self.form_check_timeframe_value.strip():
            try:
                tf_value = int(self.form_check_timeframe_value.strip())
            except ValueError:
                self.error_message = "Timeframe value must be a number"
                return
            tf_unit = self.form_check_timeframe_unit or "minutes"

        actor_id = await self._actor_id()
        try:
            with get_sync_session() as session:
                # core#934. The return value was discarded; the flushed row's id is what
                # the audit row needs, and `add_dependency` already flushes before it
                # returns, so it is available inside this transaction.
                dep = svc.add_dependency(
                    session,
                    org_id,
                    NodeType(self.form_upstream_type),
                    upstream_id,
                    NodeType(self.form_downstream_type),
                    downstream_id,
                    check_timeframe_value=tf_value,
                    check_timeframe_unit=tf_unit,
                )
                self._audit(
                    session,
                    org_id,
                    actor_id,
                    "create",
                    "dependency",
                    resource_id=dep.id,
                    new_values=self._edge_payload(
                        self.form_upstream_type,
                        upstream_id,
                        self.form_upstream_name,
                        self.form_downstream_type,
                        downstream_id,
                        self.form_downstream_name,
                        tf_value,
                        tf_unit,
                    ),
                )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to add dependency")
            return
        self.form_upstream_name = ""
        self.form_downstream_name = ""
        self.form_check_timeframe_value = ""
        self.form_check_timeframe_unit = "minutes"
        self.error_message = ""
        yield await self._saved_toast("dag.created_toast", "Dependency added")
        await self.load_dependencies()

    async def remove_dependency(self, dep_id: int):
        # See `add_dependency`. The confirmation dialog core#851 adds to
        # /dag is a claim the client makes; this is the refusal.
        if not await self._check_role("admin"):
            return
        org_id = await self._get_org_id()
        actor_id = await self._actor_id()
        svc = self._get_service()
        # The row is read BEFORE the removal, because the payload needs what is about to
        # stop existing. Ids and node types come from the persisted row — the authoritative
        # half; the two names come from state already loaded (`self.dependencies`), so this
        # adds no query (SPEC_AUDIT_TRAIL §3.4).
        item = next((d for d in self.dependencies if d.id == dep_id), None)
        with get_sync_session() as session:
            dep = svc.get_dependency(session, org_id, dep_id)
            old_values = (
                None
                if dep is None
                else self._edge_payload(
                    dep.upstream_type.value,
                    dep.upstream_id,
                    item.upstream_name if item else "",
                    dep.downstream_type.value,
                    dep.downstream_id,
                    item.downstream_name if item else "",
                    dep.check_timeframe_value,
                    dep.check_timeframe_unit,
                )
            )
            # 🚨 core#934 / SPEC_AUDIT_TRAIL §3.3. `remove_dependency` returns False when the
            # row does not exist, is already soft-deleted, or belongs to another org — and
            # this handler DISCARDED that return while yielding "Dependency removed"
            # regardless. Auditing conditionally and toasting unconditionally is worse than
            # either half alone: the user is told the edge is gone and the record says it is
            # not. So the toast and the audit row move together, or neither does.
            removed = svc.remove_dependency(session, org_id, dep_id)
            if removed:
                self._audit(
                    session,
                    org_id,
                    actor_id,
                    "delete",
                    "dependency",
                    resource_id=dep_id,
                    old_values=old_values,
                )
            session.commit()
        await self.load_dependencies()
        if not removed:
            self.error_message = await self._translated(
                "dag.remove_missing",
                "That dependency was not removed — it may already be gone.",
            )
            return
        self.error_message = ""
        yield await self._deleted_toast("dag.deleted_toast", "Dependency removed")

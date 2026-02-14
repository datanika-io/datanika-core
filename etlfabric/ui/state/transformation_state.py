"""Transformation state for Reflex UI."""

import reflex as rx

from etlfabric.models.transformation import Materialization
from etlfabric.services.transformation_service import TransformationService
from etlfabric.ui.state.base_state import BaseState, get_sync_session


class TransformationItem(rx.Base):
    id: int = 0
    name: str = ""
    description: str = ""
    materialization: str = ""
    schema_name: str = ""


class TransformationState(BaseState):
    transformations: list[TransformationItem] = []
    form_name: str = ""
    form_sql_body: str = ""
    form_materialization: str = "view"
    form_description: str = ""
    form_schema_name: str = "staging"

    def load_transformations(self):
        svc = TransformationService()
        with get_sync_session() as session:
            rows = svc.list_transformations(session, self.org_id)
            self.transformations = [
                TransformationItem(
                    id=t.id,
                    name=t.name,
                    description=t.description or "",
                    materialization=t.materialization.value,
                    schema_name=t.schema_name,
                )
                for t in rows
            ]
        self.error_message = ""

    def create_transformation(self):
        svc = TransformationService()
        try:
            with get_sync_session() as session:
                svc.create_transformation(
                    session,
                    self.org_id,
                    self.form_name,
                    self.form_sql_body,
                    Materialization(self.form_materialization),
                    description=self.form_description or None,
                    schema_name=self.form_schema_name,
                )
                session.commit()
        except Exception as e:
            self.error_message = str(e)
            return
        self.form_name = ""
        self.form_sql_body = ""
        self.form_description = ""
        self.form_schema_name = "staging"
        self.error_message = ""
        self.load_transformations()

    def delete_transformation(self, transformation_id: int):
        svc = TransformationService()
        with get_sync_session() as session:
            svc.delete_transformation(session, self.org_id, transformation_id)
            session.commit()
        self.load_transformations()

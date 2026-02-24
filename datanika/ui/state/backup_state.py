"""Backup & restore state — export/import connections and uploads."""

import json

import reflex as rx

from datanika.config import settings
from datanika.services.backup_service import BackupService
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.upload_service import UploadService
from datanika.ui.state.base_state import BaseState, get_sync_session


class BackupState(BaseState):
    restore_conflicts: list[dict] = []
    restore_data: dict = {}
    restore_result: str = ""

    def set_conflict_resolution(self, key: str, value: str):
        self.restore_conflicts = [
            {**c, "resolution": value} if c.get("key") == key else c
            for c in self.restore_conflicts
        ]

    def cancel_restore(self):
        self.restore_conflicts = []
        self.restore_data = {}
        self.restore_result = ""

    async def export_backup(self):
        org_id = await self._get_org_id()
        if not org_id:
            return
        encryption = EncryptionService(settings.credential_encryption_key)
        try:
            with get_sync_session() as session:
                backup = BackupService.export_backup(session, org_id, encryption)
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to export backup")
            return
        self.error_message = ""
        json_str = json.dumps(backup, indent=2, ensure_ascii=False)
        return rx.download(data=json_str, filename="backup.json")

    async def handle_restore_upload(self, files: list[rx.UploadFile]):
        if not files:
            return
        self.restore_result = ""
        self.error_message = ""

        upload_file = files[0]
        content = await upload_file.read()
        try:
            data = json.loads(content.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self.error_message = f"Invalid JSON file: {e}"
            return

        org_id = await self._get_org_id()
        if not org_id:
            return

        try:
            with get_sync_session() as session:
                conflicts = BackupService.detect_conflicts(session, org_id, data)
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to detect conflicts")
            return

        if conflicts:
            self.restore_data = data
            self.restore_conflicts = [
                {**c, "key": f"{c['type']}:{c['name']}", "resolution": "skip"}
                for c in conflicts
            ]
        else:
            await self._do_import(org_id, data, {})

    async def confirm_restore(self):
        org_id = await self._get_org_id()
        if not org_id or not self.restore_data:
            return
        resolutions: dict[tuple[str, str], str] = {}
        for c in self.restore_conflicts:
            typ, name = c["key"].split(":", 1)
            resolutions[(typ, name)] = c.get("resolution", "skip")
        await self._do_import(org_id, self.restore_data, resolutions)
        self.restore_conflicts = []
        self.restore_data = {}

    async def _do_import(self, org_id: int, data: dict, resolutions: dict[tuple[str, str], str]):
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        try:
            with get_sync_session() as session:
                result = BackupService.import_backup(
                    session, org_id, encryption, conn_svc, upload_svc, data, resolutions
                )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to import backup")
            return
        self.error_message = ""
        self.restore_result = (
            f"Imported {result['connections_imported']} connections, "
            f"{result['uploads_imported']} uploads. "
            f"Skipped {result['skipped']}."
        )

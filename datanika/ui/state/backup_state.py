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
    #: Name of the org an uploaded backup was exported from, when that is not
    #: this org. Blank otherwise, including for v1/v2 files that carry no
    #: provenance at all.
    restore_foreign_org: str = ""
    #: Whether a restore is held awaiting confirmation. Set by *either* a name
    #: conflict or a foreign-org warning, so the confirm/cancel block does not
    #: have to know which reason held it.
    restore_pending: bool = False

    def set_conflict_resolution(self, key: str, value: str):
        self.restore_conflicts = [
            {**c, "resolution": value} if c.get("key") == key else c for c in self.restore_conflicts
        ]

    def cancel_restore(self):
        self.restore_conflicts = []
        self.restore_data = {}
        self.restore_result = ""
        self.restore_foreign_org = ""
        self.restore_pending = False

    async def export_backup(self):
        # An export decrypts every connection config in the org. Redaction keeps
        # credentials out of the file, but the rest — hostnames, database names,
        # bucket paths, account ids — is still the org's infrastructure map, and
        # restore already requires admin. #651.
        if not await self._check_role("admin"):
            return
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
        if not await self._check_role("admin"):
            return
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

        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        if not org_id:
            return

        try:
            with get_sync_session() as session:
                conflicts = BackupService.detect_conflicts(session, org_id, data)
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to detect conflicts")
            return

        # Slug as well as id: two deployments can each have an org with id 5,
        # and comparing ids alone would call that file "same org".
        foreign = BackupService.foreign_org(data, org_id, auth.current_org.slug)
        self.restore_foreign_org = foreign

        if conflicts or foreign:
            self.restore_data = data
            self.restore_conflicts = [
                {**c, "key": f"{c['type']}:{c['name']}", "resolution": "skip"} for c in conflicts
            ]
            self.restore_pending = True
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
        self.restore_foreign_org = ""
        self.restore_pending = False

    async def _do_import(self, org_id: int, data: dict, resolutions: dict[tuple[str, str], str]):
        from datanika.ui.state.auth_state import AuthState

        auth_state = await self.get_state(AuthState)
        user_id = auth_state.current_user.id
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        try:
            with get_sync_session() as session:
                result = BackupService.import_backup(
                    session, org_id, encryption, conn_svc, upload_svc, data, resolutions
                )
                self._audit(
                    session,
                    org_id,
                    user_id,
                    "create",
                    "import",
                    new_values={
                        "connections_imported": result["connections_imported"],
                        "uploads_imported": result["uploads_imported"],
                        "skipped": result["skipped"],
                    },
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
        # A backup never contains credentials, so a restored connection cannot
        # connect until someone re-enters them. Saying which ones is the
        # difference between "it restored" and "it restored and half of it is
        # inert". Names only — a dynamic list, so not an i18n key.
        needs = result.get("credentials_required") or []
        if needs:
            self.restore_result += " Credentials must be re-entered for: " + ", ".join(needs) + "."

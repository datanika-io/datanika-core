"""Audit log state — list and filter."""

import contextlib
import json

from pydantic import BaseModel

from datanika.models.audit_log import AuditAction
from datanika.services.audit_service import REDACTED, AuditService, redact_pii_payload
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.base_state import BaseState, get_sync_session

#: How much of a change summary the cell shows before the tooltip carries the rest.
_MAX_CELL = 160
_ARROW = " \u2192 "

#: Distinguishes "this key was absent on one side" from "this key was present and null".
#: Using ``None`` for both is the conflation this whole spec family is about.
_ABSENT = object()


def _render_value(value: object, redacted: str, not_set: str) -> str:
    if value is None:
        return not_set
    if value == REDACTED:
        return redacted
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def format_changes(old: dict | None, new: dict | None, redacted: str, not_set: str) -> str:
    """Render an audit payload pair as a human-readable change (core#694).

    🚨 **The payloads are passed through the D12 redactor a SECOND time, here at
    read.** ``AuditService.log_action`` already redacts at write, and that
    chokepoint remains the real control — this is defence in depth, so that a row
    written before the redactor shipped, or by a future path that bypasses the
    service, cannot put a personal value on an org admin's screen.

    ⚠️ **This does NOT reduce the importance of D12.2's guard, and must not be
    cited as a reason to weaken it.** ``SPEC_PII_SEPARATION`` §2c deleted the
    backfill and says so explicitly: with it gone, that guard is the *only*
    witness to a redactor regression. Redacting again at read makes the **page**
    safe; it does not make the **table** safe, and the table is what a support
    export, an MCP tool or a future reader would reach.

    An update renders as ``field: before -> after`` for the fields that actually
    changed; a create or a delete has only one payload and renders as
    ``field: value``, because an arrow with nothing on one side reads as data
    loss rather than as creation.
    """
    old = redact_pii_payload(old) or {}
    new = redact_pii_payload(new) or {}

    if old and new:
        parts: list[str] = []
        for key in sorted(set(old) | set(new)):
            before = old.get(key, _ABSENT)
            after = new.get(key, _ABSENT)
            # 🚨 A redacted field is NEVER skipped, even though both sides compare
            # equal. Redaction happens at WRITE, so `old` and `new` both hold the
            # marker whether or not the value changed — the information is gone
            # before this code sees it. Skipping on equality would therefore turn
            # *"the email changed"* into **silence**, and `SPEC_PII_SEPARATION`
            # D12.3 is explicit that the log "records that a value changed and who
            # changed it, never what a personal value was".
            #
            # ⚠️ Rendering it as *changed* would be the opposite lie for a snapshot
            # that merely included the field. So it renders as
            # `email: (redacted) -> (redacted)`: this field is part of the record,
            # and neither side of it is ours to show. That is the most that is true.
            if (
                before is not _ABSENT
                and after is not _ABSENT
                and before == after
                and before != REDACTED
            ):
                continue
            rendered_before = (
                not_set if before is _ABSENT else _render_value(before, redacted, not_set)
            )
            rendered_after = (
                not_set if after is _ABSENT else _render_value(after, redacted, not_set)
            )
            parts.append(f"{key}: {rendered_before}{_ARROW}{rendered_after}")
    else:
        source = new or old
        parts = [
            f"{key}: {_render_value(value, redacted, not_set)}"
            for key, value in sorted(source.items())
        ]
    return "; ".join(parts)


def _change_fields(log, redacted: str, not_set: str) -> dict:
    """``changes`` / ``changes_full`` / ``truncated`` for one row."""
    full = format_changes(log.old_values, log.new_values, redacted, not_set)
    if len(full) <= _MAX_CELL:
        return {"changes": full, "changes_full": "", "truncated": False}
    return {
        "changes": full[: _MAX_CELL - 1].rstrip() + "\u2026",
        "changes_full": full,
        "truncated": True,
    }


class AuditLogItem(BaseModel):
    id: int = 0
    action: str = ""
    resource_type: str = ""
    resource_id: str = ""
    #: What actually changed. 30 call sites write ``old_values``/``new_values`` and,
    #: until core#694, **nothing read them** — the page rendered ``ip_address``
    #: instead, the one column that has never held a value in any production row.
    #:
    #: ⚠️ ``ip_address`` is gone from this model, from the page, and from the nine
    #: locale files. Keeping the translations was the first instinct — core#670 may
    #: yet populate that column — but ``test_no_orphan_keys_in_json`` has no allowlist
    #: and it is **right**: with the column gone the translation is dead, and an
    #: allowlist is how a gate gets turned off one line at a time. This deletion is
    #: not the core#872 hazard, which is about a *silent* sweep: it is argued, it is
    #: on core#670, and the nine strings are one ``git show`` away.
    changes: str = ""
    #: Untruncated, for the tooltip. Empty when nothing was elided.
    changes_full: str = ""
    truncated: bool = False
    created_at: str = ""


class AuditState(BaseState):
    logs: list[AuditLogItem] = []
    filter_action: str = ""
    filter_resource_type: str = ""

    def set_filter_action(self, value: str):
        self.filter_action = value

    def set_filter_resource_type(self, value: str):
        self.filter_resource_type = value

    async def load_audit_logs(self):
        auth_state = await self.get_state(AuthState)
        if not auth_state.current_org.id:
            return
        svc = AuditService()
        # Services raise plain English and have no locale (base_state `_translated`),
        # so the two labels the renderer substitutes are resolved here, once per load,
        # rather than per row.
        redacted = await self._translated("audit.redacted", "(redacted)")
        not_set = await self._translated("audit.not_set", "(not set)")
        action = None
        if self.filter_action and self.filter_action != "all":
            with contextlib.suppress(ValueError):
                action = AuditAction(self.filter_action)
        resource_type = None
        if self.filter_resource_type and self.filter_resource_type != "all":
            resource_type = self.filter_resource_type

        with get_sync_session() as session:
            rows = svc.list_logs(
                session,
                org_id=auth_state.current_org.id,
                action=action,
                resource_type=resource_type,
                limit=200,
            )
            self.logs = [
                AuditLogItem(
                    id=log.id,
                    action=log.action.value if hasattr(log.action, "value") else str(log.action),
                    resource_type=log.resource_type,
                    resource_id=str(log.resource_id) if log.resource_id else "",
                    **_change_fields(log, redacted, not_set),
                    created_at=(
                        log.created_at.strftime("%Y-%m-%d %H:%M:%S") if log.created_at else ""
                    ),
                )
                for log in rows
            ]

    async def apply_filters(self):
        await self.load_audit_logs()

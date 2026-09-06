"""AuditService — action logging and querying, plus the D12 payload redactor."""

import json
import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

# Imported for its SIDE EFFECT as well as for the names: ``PII_PAYLOAD_KEYS`` below is
# derived from ``Base.metadata``, which is populated only for models that have actually
# been imported. A redactor whose module loads before the PII models does not raise — it
# silently derives ``frozenset()`` and redacts nothing, in a table nothing reads, so
# nothing would ever contradict it. Importing the models package (the one place that
# imports every model) is what makes the derivation total rather than
# order-of-import-dependent, and ``test_the_derived_key_set_is_exposed_and_exact`` pins
# the result so this cannot silently regress.
import datanika.models  # noqa: F401
from datanika.errors import UserFacingError
from datanika.models.audit_log import AuditAction, AuditLog
from datanika.models.base import Base
from datanika.services.backup_service import REDACTED

logger = logging.getLogger(__name__)


def _derive_pii_payload_keys() -> frozenset[str]:
    """Every non-key column of every ``*_pii`` table, plus ``ip_address``.

    Derived rather than hand-listed, because a hand list is what this whole change is
    correcting. It grows on its own the day a fourth PII table or a fifth PII column
    lands, which is the only version that survives contact with future call sites.

    ⚠️ It is **nominal** — it matches key *names*, so it cannot see personal data stored
    under a non-PII key. ``{"name": "My Postgres"}`` and ``{"name": "Anna's Org"}`` are
    indistinguishable to it; the discriminator there is ``(resource_type, key)``, not
    ``key``. That residual is D11's, which is why D12.1 is *both* mechanisms and not a
    choice between them.

    ⚠️ ``SECRET_CONFIG_KEYS`` was proposed as the source and is the wrong object. It is a
    **connector-credential** set derived from ``CONFIG_SCHEMAS`` — 17 keys, ``password``,
    ``api_key``, ``keyfile_json`` and so on — describing a universe with no email
    addresses in it. **Not one of them is a PII key.** A redactor built on it would be
    derived, superset-tested, green, and would redact exactly zero personal data.
    """
    return frozenset(
        col.name
        for table in Base.metadata.tables.values()
        if table.name.endswith("_pii")
        for col in table.columns
        if not col.primary_key and not col.foreign_keys
    ) | {
        # The one hand-added key, and it has a stated expiry. ``ip_address`` is not a
        # ``*_pii`` column because D11 declines to build ``audit_log_pii`` for a column
        # empty in every production row; it belongs in its own column and never in a
        # payload. When core#670 decides to start collecting client IPs and creates that
        # sidecar, the derivation picks it up and this literal becomes redundant — delete
        # it then.
        "ip_address"
    }


#: D12.2. Pinned by name because a contract a test cannot address is not a contract.
PII_PAYLOAD_KEYS: frozenset[str] = _derive_pii_payload_keys()

#: Written into the row when redaction itself fails, so the failure is greppable rather
#: than silent. See ``redact_pii_payload``.
REDACTION_FAILED_KEY = "__redaction_failed__"

#: Depth beyond which a payload is treated as malformed. Every payload today is a flat
#: dict of scalars and nothing enforces that, so the recursion needs a floor that is not
#: Python's own recursion limit — hitting that leaves the interpreter fragile and the
#: traceback unreadable, and a RecursionError inside an audit write is a worse outcome
#: than a marker.
_MAX_DEPTH = 20


def _redact(value: object, depth: int, seen: frozenset[int]) -> object:
    if depth > _MAX_DEPTH:
        # core#1113: internal invariant text under a marker that says user-facing.
        # Converted here only to keep core#1094 step 2 behaviour-neutral; the three
        # audit_service sites are named on that issue.
        raise UserFacingError(f"audit payload nested deeper than {_MAX_DEPTH} levels")
    if isinstance(value, dict):
        if id(value) in seen:
            raise UserFacingError("audit payload contains a cycle")
        inner = seen | {id(value)}
        return {
            key: (REDACTED if key in PII_PAYLOAD_KEYS else _redact(val, depth + 1, inner))
            for key, val in value.items()
        }
    if isinstance(value, list | tuple):
        if id(value) in seen:
            raise UserFacingError("audit payload contains a cycle")
        inner = seen | {id(value)}
        return [_redact(item, depth + 1, inner) for item in value]
    return value


def redact_pii_payload(payload: dict | None) -> dict | None:
    """Replace every PII-keyed value with the redaction marker. Never raises.

    **Replacement, not deletion.** A redacted key keeps its key and takes
    ``backup_service.REDACTED``, so the trail still shows *an email was here*. A dropped
    key is indistinguishable from a call site that never wrote one.

    **Key-level, not blanket**, and the reason is not squeamishness. Nothing reads these
    payloads today (§2b), so a blanket redactor would satisfy every other assertion in the
    suite while destroying the only surviving record of what deleted resources were — the
    ``{"name": ..., "connection_type": ...}`` written by the 25 label-carrying call sites,
    including the five production connections deleted in one session by a page-wide
    ``.last()``. It would destroy them invisibly, because there is no reader to notice.

    🚨 **It must not raise, and it must not pass the payload through either.**
    ``BaseState._audit`` ends in ``except Exception:`` — audit logging must never break the
    operation it describes — so a redactor that throws does not surface an error, it
    **silently deletes the audit row**, turning a PII bug into a missing-trail bug with no
    signal at all. Failing open leaks; failing hard loses the row. Replacing the payload
    with a greppable marker and letting the row be written is the only third option:
    who/what/when survive, and the marker says the payload did not.
    """
    if payload is None:
        return None
    try:
        redacted = _redact(payload, 0, frozenset())
        # The row still has to be storable. A value that survives redaction but cannot be
        # serialized would raise at flush instead — i.e. inside the caller's transaction,
        # after this function has already reported success.
        json.dumps(redacted)
        return redacted
    except Exception:
        # No payload values in the log line: an erasure trail that names what it erased
        # defeats itself, and this function exists to keep values out of this table.
        logger.warning(
            "Audit payload redaction failed; storing the %s marker instead.",
            REDACTION_FAILED_KEY,
        )
        return {REDACTION_FAILED_KEY: True}


class AuditService:
    def log_action(
        self,
        session: Session,
        org_id: int,
        user_id: int | None,
        action: AuditAction,
        resource_type: str,
        resource_id: int | None = None,
        old_values: dict | None = None,
        new_values: dict | None = None,
        ip_address: str | None = None,
    ) -> AuditLog:
        """Record an audit log entry.

        This is the single true chokepoint: ``BaseState._audit`` (the ~30 payload call
        sites) and the two direct callers in ``auth_state`` all pass through here, so no
        new call site can bypass redaction by forgetting to ask for it.

        ⚠️ ``redact_pii_payload`` is called through a **module-global lookup** and must
        stay that way — not inlined, not a method, not imported into a local name. §2c
        leaves ``test_audit_pii_redaction.py`` as the only thing that could ever notice a
        redactor regression, and the only way to show that guard is *sensitive to the
        redactor* is to substitute a no-op and watch it fail. An inlined redactor cannot
        be substituted, so the negative control would quietly stop being a control while
        still passing — the same defect one level up.
        """
        log = AuditLog(
            org_id=org_id,
            user_id=user_id,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            old_values=redact_pii_payload(old_values),
            new_values=redact_pii_payload(new_values),
            ip_address=ip_address,
        )
        session.add(log)
        session.flush()
        return log

    def list_logs(
        self,
        session: Session,
        org_id: int,
        action: AuditAction | None = None,
        resource_type: str | None = None,
        user_id: int | None = None,
        limit: int = 100,
    ) -> list[AuditLog]:
        """List audit logs with optional filters."""
        stmt = select(AuditLog).where(AuditLog.org_id == org_id)

        if action is not None:
            stmt = stmt.where(AuditLog.action == action)
        if resource_type is not None:
            stmt = stmt.where(AuditLog.resource_type == resource_type)
        if user_id is not None:
            stmt = stmt.where(AuditLog.user_id == user_id)

        stmt = stmt.order_by(AuditLog.created_at.desc()).limit(limit)
        return list(session.execute(stmt).scalars().all())

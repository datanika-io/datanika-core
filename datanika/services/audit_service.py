"""AuditService — action logging and querying."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from etlfabric.models.audit_log import AuditAction, AuditLog


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
        """Record an audit log entry."""
        log = AuditLog(
            org_id=org_id,
            user_id=user_id,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            old_values=old_values,
            new_values=new_values,
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

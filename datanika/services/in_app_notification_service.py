"""In-app notification service — CRUD for the Notification Center (#68).

Separate from ``notification_service.py`` which handles external
dispatch channels (Slack, Telegram, email, webhook). This service
manages the in-app notifications that appear in the bell icon /
notification page inside the Datanika UI.
"""

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from datanika.models.notification import Notification, NotificationType
from datanika.services import notification_unread_cache


class InAppNotificationService:
    @staticmethod
    def create(
        session: Session,
        org_id: int,
        notification_type: NotificationType,
        title: str,
        resource_type: str,
        resource_id: int,
        *,
        message: str | None = None,
        user_id: int | None = None,
    ) -> Notification:
        notif = Notification(
            org_id=org_id,
            user_id=user_id,
            type=notification_type,
            title=title,
            message=message,
            resource_type=resource_type,
            resource_id=resource_id,
        )
        session.add(notif)
        session.flush()
        # A new unread row invalidates every user's count in the org: if
        # user_id is set, only that user is affected; if None, the row
        # is org-wide and every user's count goes up. We conservatively
        # drop all — orgs are small, SCAN+DEL is cheap.
        notification_unread_cache.invalidate_org(org_id)
        return notif

    @staticmethod
    def list_for_user(
        session: Session,
        org_id: int,
        user_id: int,
        *,
        unread_only: bool = False,
        limit: int = 20,
        offset: int = 0,
    ) -> list[Notification]:
        stmt = (
            select(Notification)
            .where(
                Notification.org_id == org_id,
                Notification.deleted_at.is_(None),
                # User-specific OR org-wide (user_id is null)
                (Notification.user_id == user_id) | (Notification.user_id.is_(None)),
            )
            .order_by(Notification.created_at.desc())
        )
        if unread_only:
            stmt = stmt.where(Notification.read_at.is_(None))
        stmt = stmt.limit(min(limit, 100)).offset(offset)
        return list(session.execute(stmt).scalars().all())

    @staticmethod
    def unread_count(session: Session, org_id: int, user_id: int) -> int:
        cached = notification_unread_cache.get(org_id, user_id)
        if cached is not None:
            return cached
        stmt = (
            select(func.count())
            .select_from(Notification)
            .where(
                Notification.org_id == org_id,
                Notification.deleted_at.is_(None),
                Notification.read_at.is_(None),
                (Notification.user_id == user_id) | (Notification.user_id.is_(None)),
            )
        )
        count = session.execute(stmt).scalar_one()
        notification_unread_cache.set(org_id, user_id, count)
        return count

    @staticmethod
    def mark_read(session: Session, notification_id: int, org_id: int) -> Notification | None:
        stmt = select(Notification).where(
            Notification.id == notification_id,
            Notification.org_id == org_id,
            Notification.deleted_at.is_(None),
        )
        notif = session.execute(stmt).scalar_one_or_none()
        if notif is None:
            return None
        was_unread = notif.read_at is None
        notif.read_at = datetime.now(UTC)
        session.flush()
        if was_unread:
            # The row may be user-specific or org-wide; either way, the
            # unread count for at least one user dropped. Invalidate org.
            notification_unread_cache.invalidate_org(org_id)
        return notif

    @staticmethod
    def mark_all_read(session: Session, org_id: int, user_id: int) -> int:
        stmt = select(Notification).where(
            Notification.org_id == org_id,
            Notification.deleted_at.is_(None),
            Notification.read_at.is_(None),
            (Notification.user_id == user_id) | (Notification.user_id.is_(None)),
        )
        notifications = list(session.execute(stmt).scalars().all())
        now = datetime.now(UTC)
        for n in notifications:
            n.read_at = now
        session.flush()
        # The mark_all set may include org-wide rows (user_id=None) whose
        # read_at transition affects every user's count, not just this
        # user's. Conservative: invalidate the whole org.
        has_org_wide = any(n.user_id is None for n in notifications)
        if has_org_wide:
            notification_unread_cache.invalidate_org(org_id)
        else:
            notification_unread_cache.invalidate_user(org_id, user_id)
        return len(notifications)

    @staticmethod
    def dismiss(session: Session, notification_id: int, org_id: int) -> bool:
        stmt = select(Notification).where(
            Notification.id == notification_id,
            Notification.org_id == org_id,
            Notification.deleted_at.is_(None),
        )
        notif = session.execute(stmt).scalar_one_or_none()
        if notif is None:
            return False
        was_unread = notif.read_at is None
        notif.deleted_at = datetime.now(UTC)
        session.flush()
        if was_unread:
            notification_unread_cache.invalidate_org(org_id)
        return True

    @staticmethod
    def total_count(
        session: Session,
        org_id: int,
        user_id: int,
        *,
        unread_only: bool = False,
    ) -> int:
        stmt = (
            select(func.count())
            .select_from(Notification)
            .where(
                Notification.org_id == org_id,
                Notification.deleted_at.is_(None),
                (Notification.user_id == user_id) | (Notification.user_id.is_(None)),
            )
        )
        if unread_only:
            stmt = stmt.where(Notification.read_at.is_(None))
        return session.execute(stmt).scalar_one()

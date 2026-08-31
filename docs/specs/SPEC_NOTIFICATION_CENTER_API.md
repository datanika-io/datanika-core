# Notification Center — Backend API Spec (Handoff to Product)

> **Owner**: Engineering → Product handoff.
> **Status**: Backend shipped (core #68). Product can start UI work.
> **Date**: 2026-04-12

---

## Overview

The in-app notification system creates `Notification` records whenever
a pipeline run completes (success or failure). The backend handles:
- Model + DB table (`notifications`)
- Service layer (`InAppNotificationService`)
- Hook handlers that auto-create notifications on run events
- 5 REST API endpoints for listing, reading, and dismissing

Product owns: bell icon UI, notification dropdown/page, Reflex state
class, i18n strings.

---

## Service Interface

```python
from datanika.services.in_app_notification_service import InAppNotificationService

svc = InAppNotificationService()

# Create a notification (usually done by hooks, not UI)
svc.create(session, org_id, NotificationType.RUN_FAILED,
           title="Run #42 failed", resource_type="run", resource_id=42,
           message="Connection timeout", user_id=None)

# List for a user (includes org-wide where user_id is null)
items = svc.list_for_user(session, org_id, user_id,
                          unread_only=False, limit=20, offset=0)

# Badge count
count = svc.unread_count(session, org_id, user_id)

# Mark single as read
svc.mark_read(session, notification_id, org_id)

# Mark all as read
svc.mark_all_read(session, org_id, user_id)

# Dismiss (soft delete)
svc.dismiss(session, notification_id, org_id)

# Total count (for pagination)
total = svc.total_count(session, org_id, user_id, unread_only=False)
```

---

## REST API Endpoints

### `GET /api/v1/notifications`

Query params: `unread_only` (bool), `limit` (int, max 100), `offset` (int).

```json
{
  "items": [
    {
      "id": 1,
      "type": "run_failed",
      "title": "Run #42 failed",
      "message": "Connection timeout",
      "resource_type": "run",
      "resource_id": 42,
      "read_at": null,
      "created_at": "2026-04-12T10:00:00Z"
    }
  ],
  "total": 15,
  "unread_count": 3
}
```

### `GET /api/v1/notifications/unread-count`

Lightweight polling endpoint for the badge.

```json
{ "count": 3 }
```

### `PATCH /api/v1/notifications/{id}/read`

Mark a single notification as read.

```json
{
  "id": 1, "type": "run_failed", "title": "...",
  "read_at": "2026-04-12T10:05:00Z", ...
}
```

### `POST /api/v1/notifications/read-all`

Mark all unread notifications as read for the current user.

```json
{ "marked": 5 }
```

### `DELETE /api/v1/notifications/{id}`

Soft-delete (dismiss) a notification.

```json
{ "deleted": true }
```

---

## Reflex Integration Pattern

```python
from datanika.services.in_app_notification_service import InAppNotificationService
from datanika.ui.state.base_state import BaseState, get_sync_session
from datanika.ui.state.auth_state import AuthState


class NotificationCenterState(BaseState):
    notifications: list[dict] = []
    unread_count: int = 0

    async def load_notifications(self):
        auth = await self.get_state(AuthState)
        svc = InAppNotificationService()
        with get_sync_session() as session:
            items = svc.list_for_user(
                session, auth.current_org.id, auth.current_user.id,
                limit=20,
            )
            self.notifications = [
                {
                    "id": n.id,
                    "type": n.type.value,
                    "title": n.title,
                    "message": n.message,
                    "resource_type": n.resource_type,
                    "resource_id": n.resource_id,
                    "read_at": n.read_at.isoformat() if n.read_at else None,
                    "created_at": n.created_at.isoformat() if n.created_at else None,
                }
                for n in items
            ]
            self.unread_count = svc.unread_count(
                session, auth.current_org.id, auth.current_user.id,
            )
```

### Badge Polling

For the bell icon unread count, poll `load_notifications` on page load
(`on_load`) plus optionally on a 30-second interval if you want
near-real-time updates.

---

## Notification Types

| Type | When Created | Hook Event |
|------|-------------|------------|
| `run_failed` | Any run finishes with status "failed" | `run.upload_completed`, `run.models_completed`, `run.transformation_completed` |
| `run_succeeded` | Any run finishes with status "success" | Same hooks |
| `quota_warning` | 80% of quota used | (Cloud plugin, already exists) |
| `quota_exceeded` | Quota limit hit | (Cloud plugin, already exists) |

---

## i18n Key Convention

```
notifications.center.title → "Notifications"
notifications.center.empty → "No notifications yet"
notifications.center.mark_all_read → "Mark all as read"
notifications.run_failed.title → "Run failed"
notifications.run_succeeded.title → "Run succeeded"
notifications.quota_warning.title → "Quota warning"
```

---

## What's NOT in the Backend

- **Bell icon UI** — Product builds this in `ui/components/`
- **Notification dropdown/page** — Product owns the Reflex page
- **i18n strings** — Product adds keys to all 9 locale files
- **Real-time push** — Not implemented. Use polling for now. WebSocket
  integration is a future enhancement if polling proves insufficient.
- **Notification preferences** — Not in v1. All notification types
  are created for all users. Per-user muting is a follow-up.

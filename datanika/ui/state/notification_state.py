"""Notification channel state — list, create, update, delete channels."""

from pydantic import BaseModel

from datanika.models.notification_channel import ChannelType
from datanika.services.notification_service import NotificationService
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.base_state import BaseState, get_sync_session


class ChannelItem(BaseModel):
    """One row of the alerting-channel table, as the **client** receives it.

    🚨 **This is a wire format, not a row.** ``NotificationState.channels`` is a
    public Reflex state var, so every field here is serialized to every
    connected browser — and the channel list is deliberately visible to every
    member, not just admins (core#886).

    It used to carry ``config: dict``, copied verbatim from
    ``NotificationChannel.config``, which is where the Slack **webhook URL** and
    the Telegram **bot token** live. The name survived the crossing and the
    protection did not: the delete dialog tells the user those values "are not
    shown again", which was true of the rendered DOM and false of the payload
    behind it (core#972).

    ⚠️ **Add fields one at a time and by name.** A bare ``dict`` hydrated from a
    provider-shaped column is the shape of that defect, and it is the shape
    ``tests/test_security/test_secret_bearing_state.py`` refuses.
    """

    id: int = 0
    name: str = ""
    channel_type: str = ""
    events: list[str] = []
    is_active: bool = True
    #: Delivery record (core#652). ``is_active`` answers "is this switched on?";
    #: the user is asking "is this working?". An empty ``last_status`` means the
    #: channel has never been attempted — rendered as such, never as green.
    last_status: str = ""
    last_error: str = ""
    last_attempt_at: str = ""


class NotificationState(BaseState):
    channels: list[ChannelItem] = []
    show_form: bool = False
    editing_id: int = 0

    # Form fields
    form_name: str = ""
    form_channel_type: str = "slack"
    form_webhook_url: str = ""
    form_email: str = ""
    form_telegram_token: str = ""
    form_telegram_chat_id: str = ""
    form_custom_url: str = ""
    form_on_failure: bool = True
    form_on_success: bool = False

    def set_form_name(self, v: str):
        self.form_name = v

    def set_form_channel_type(self, v: str):
        self.form_channel_type = v

    def set_form_webhook_url(self, v: str):
        self.form_webhook_url = v

    def set_form_email(self, v: str):
        self.form_email = v

    def set_form_telegram_token(self, v: str):
        self.form_telegram_token = v

    def set_form_telegram_chat_id(self, v: str):
        self.form_telegram_chat_id = v

    def set_form_custom_url(self, v: str):
        self.form_custom_url = v

    def set_form_on_failure(self, v: bool):
        self.form_on_failure = v

    def set_form_on_success(self, v: bool):
        self.form_on_success = v

    def toggle_form(self):
        self.show_form = not self.show_form
        if self.show_form:
            self._reset_form()

    def _reset_form(self):
        self.editing_id = 0
        self.form_name = ""
        self.form_channel_type = "slack"
        self.form_webhook_url = ""
        self.form_email = ""
        self.form_telegram_token = ""
        self.form_telegram_chat_id = ""
        self.form_custom_url = ""
        self.form_on_failure = True
        self.form_on_success = False

    def _build_config(self) -> dict:
        ct = self.form_channel_type
        if ct == "slack":
            return {"webhook_url": self.form_webhook_url}
        if ct == "telegram":
            return {"token": self.form_telegram_token, "chat_id": self.form_telegram_chat_id}
        if ct == "email":
            return {"email": self.form_email}
        if ct == "webhook":
            return {"url": self.form_custom_url}
        return {}

    def _build_events(self) -> list[str]:
        events = []
        if self.form_on_failure:
            events.append("run_failure")
        if self.form_on_success:
            events.append("run_success")
        return events

    async def load_channels(self):
        auth = await self.get_state(AuthState)
        if not auth.current_org.id:
            return
        svc = NotificationService()
        with get_sync_session() as session:
            rows = svc.list_channels(session, auth.current_org.id)
            self.channels = [
                ChannelItem(
                    id=ch.id,
                    name=ch.name,
                    channel_type=ch.channel_type.value,
                    events=ch.events or [],
                    is_active=ch.is_active,
                    last_status=ch.last_status or "",
                    last_error=ch.last_error or "",
                    last_attempt_at=(ch.last_attempt_at.isoformat() if ch.last_attempt_at else ""),
                )
                for ch in rows
            ]

    async def save_channel(self):
        if not await self._check_role("admin"):
            return
        auth = await self.get_state(AuthState)
        svc = NotificationService()
        config = self._build_config()
        events = self._build_events()
        try:
            ct = ChannelType(self.form_channel_type)
            with get_sync_session() as session:
                if self.editing_id:
                    svc.update_channel(
                        session,
                        self.editing_id,
                        auth.current_org.id,
                        name=self.form_name,
                        config=config,
                        events=events,
                    )
                    self._audit(
                        session,
                        auth.current_org.id,
                        auth.current_user.id,
                        "update",
                        "notification_channel",
                        resource_id=self.editing_id,
                        new_values={"name": self.form_name},
                    )
                else:
                    ch = svc.create_channel(
                        session,
                        auth.current_org.id,
                        name=self.form_name,
                        channel_type=ct,
                        config=config,
                        events=events,
                    )
                    self._audit(
                        session,
                        auth.current_org.id,
                        auth.current_user.id,
                        "create",
                        "notification_channel",
                        resource_id=ch.id,
                        new_values={"name": self.form_name, "type": self.form_channel_type},
                    )
                session.commit()
            self.show_form = False
            self.error_message = ""
            await self.load_channels()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to save notification channel")
            return
        yield await self._saved_toast("notifications.created_toast", "Channel saved")

    async def delete_channel(self, channel_id: int):
        if not await self._check_role("admin"):
            return
        auth = await self.get_state(AuthState)
        svc = NotificationService()
        try:
            with get_sync_session() as session:
                ch_info = next((c for c in self.channels if c.id == channel_id), None)
                old_values = {"name": ch_info.name} if ch_info else {}
                svc.delete_channel(session, channel_id, auth.current_org.id)
                self._audit(
                    session,
                    auth.current_org.id,
                    auth.current_user.id,
                    "delete",
                    "notification_channel",
                    resource_id=channel_id,
                    old_values=old_values,
                )
                session.commit()
            self.error_message = ""
            await self.load_channels()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to delete channel")
            # See `ApiKeyState.revoke_api_key`: falling through to the toast
            # would report a delete that did not happen.
            return
        yield await self._deleted_toast("notifications.deleted_toast", "Channel deleted")

    async def toggle_channel_active(self, channel_id: int):
        if not await self._check_role("admin"):
            return
        auth = await self.get_state(AuthState)
        svc = NotificationService()
        try:
            ch_item = next((c for c in self.channels if c.id == channel_id), None)
            if ch_item is None:
                return
            with get_sync_session() as session:
                svc.update_channel(
                    session,
                    channel_id,
                    auth.current_org.id,
                    is_active=not ch_item.is_active,
                )
                session.commit()
            await self.load_channels()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to toggle channel")

    async def edit_channel(self, channel_id: int):
        """Load one channel into the edit form, re-reading the row server-side.

        Two changes from the original, and they are one fix (core#972).

        **It re-reads.** The old version took the credential out of
        ``self.channels`` — which was only possible because ``ChannelItem``
        carried ``config``, i.e. because every member's browser already had the
        bot token. Reading it back from the database is what lets the wire
        format stop carrying it.

        **It checks the role.** ``_channel_actions``'s docstring used to justify
        leaving this ungated: *"it persists nothing — it copies the row into the
        form"*, gated in the UI only so nobody is handed a form whose Save would
        refuse. That reasoning is wrong in a way worth naming, because the same
        reasoning left ``ConnectionState.edit_connection`` open: **reading the
        secret is the harm.** Persistence is beside the point, and a Reflex
        event handler is dispatched by name over the websocket, so hiding the
        pencil button gates nothing.
        """
        if not await self._check_role("admin"):
            return
        auth = await self.get_state(AuthState)
        if not auth.current_org.id:
            return
        svc = NotificationService()
        with get_sync_session() as session:
            ch = svc.get_channel(session, channel_id, auth.current_org.id)
            if ch is None:
                return
            config = ch.config or {}
            self.editing_id = ch.id
            self.form_name = ch.name
            ct = ch.channel_type.value
            self.form_channel_type = ct
            events = ch.events or []
        if ct == "slack":
            self.form_webhook_url = config.get("webhook_url", "")
        elif ct == "telegram":
            self.form_telegram_token = config.get("token", "")
            self.form_telegram_chat_id = config.get("chat_id", "")
        elif ct == "email":
            self.form_email = config.get("email", "")
        elif ct == "webhook":
            self.form_custom_url = config.get("url", "")
        self.form_on_failure = "run_failure" in events
        self.form_on_success = "run_success" in events
        self.show_form = True

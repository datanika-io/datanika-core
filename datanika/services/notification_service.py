"""NotificationService."""

import logging
from datetime import UTC, datetime

import httpx
from sqlalchemy import select

from datanika.models.notification_channel import ChannelType, NotificationChannel

logger = logging.getLogger(__name__)
VALID_EVENTS = frozenset(
    [
        "run_failure",
        "run_success",
        "quota_warning",
        # V2 P5 Option B cycle-boundary charge events (core#249). Users opt
        # channels into these so Slack/Telegram/webhook/email deliver overage
        # billing notifications alongside the in-app row.
        "charge_incoming",
        "charge_issued",
        "charge_failed",
    ]
)
_CONFIG_REQUIRED = {
    ChannelType.EMAIL: ["email"],
    ChannelType.SLACK: ["webhook_url"],
    ChannelType.TELEGRAM: ["token", "chat_id"],
    ChannelType.WEBHOOK: ["url"],
}


def _validate_config(ct, config):
    for f in _CONFIG_REQUIRED[ct]:
        if not config.get(f):
            raise ValueError(f"config.{f} is required for {ct} channels")


def _validate_events(events):
    bad = set(events) - VALID_EVENTS
    if bad:
        raise ValueError(f"Invalid event(s): {bad!r}. Valid: {set(VALID_EVENTS)!r}")


class NotificationService:
    def create_channel(self, session, org_id, name, channel_type, config, events):
        if not name or not name.strip():
            raise ValueError("Channel name cannot be empty")
        _validate_config(channel_type, config)
        _validate_events(events)
        ch = NotificationChannel(
            org_id=org_id,
            name=name.strip(),
            channel_type=channel_type,
            config=config,
            events=events,
            is_active=True,
        )
        session.add(ch)
        session.flush()
        return ch

    def _get_channel(self, session, channel_id, org_id):
        stmt = select(NotificationChannel).where(
            NotificationChannel.id == channel_id,
            NotificationChannel.org_id == org_id,
            NotificationChannel.deleted_at.is_(None),
        )
        return session.execute(stmt).scalar_one_or_none()

    def list_channels(self, session, org_id):
        stmt = (
            select(NotificationChannel)
            .where(
                NotificationChannel.org_id == org_id,
                NotificationChannel.deleted_at.is_(None),
            )
            .order_by(NotificationChannel.created_at.desc())
        )
        return list(session.execute(stmt).scalars().all())

    def update_channel(self, session, channel_id, org_id, **kwargs):
        ch = self._get_channel(session, channel_id, org_id)
        if ch is None:
            return None
        if "name" in kwargs:
            n = kwargs["name"]
            if not n or not str(n).strip():
                raise ValueError("Channel name cannot be empty")
            ch.name = str(n).strip()
        if "config" in kwargs:
            _validate_config(ch.channel_type, kwargs["config"])
            ch.config = kwargs["config"]
        if "events" in kwargs:
            _validate_events(kwargs["events"])
            ch.events = kwargs["events"]
        if "is_active" in kwargs:
            ch.is_active = kwargs["is_active"]
        session.flush()
        return ch

    def delete_channel(self, session, channel_id, org_id):
        ch = self._get_channel(session, channel_id, org_id)
        if ch is None:
            return False
        ch.deleted_at = datetime.now(UTC)
        session.flush()
        return True

    def notify(self, session, org_id, event_type, payload, *, email_service=None):
        """Dispatch notification to all active channels subscribed to event_type."""
        for ch in self.list_channels(session, org_id):
            if not ch.is_active:
                continue
            if event_type not in ch.events:
                continue
            try:
                self._dispatch(ch, event_type, payload, email_service=email_service)
            except Exception:
                logger.exception(
                    "Failed to dispatch %s notification via channel %s (id=%s)",
                    event_type,
                    ch.name,
                    ch.id,
                )

    def _dispatch(self, channel, event_type, payload, *, email_service=None):
        if channel.channel_type == ChannelType.EMAIL:
            self._dispatch_email(channel, event_type, payload, email_service)
        elif channel.channel_type == ChannelType.SLACK:
            self._dispatch_slack(channel, event_type, payload)
        elif channel.channel_type == ChannelType.TELEGRAM:
            self._dispatch_telegram(channel, event_type, payload)
        elif channel.channel_type == ChannelType.WEBHOOK:
            self._dispatch_webhook(channel, event_type, payload)

    def _dispatch_email(self, channel, event_type, payload, email_service):
        if email_service is None or not email_service.is_enabled():
            logger.warning(
                "Email service disabled; skipping notification for channel %s", channel.id
            )
            return
        to = channel.config["email"]
        if event_type == "quota_warning":
            subject, body = _build_quota_warning_email(payload)
        else:
            run_id = payload.get("run_id", "?")
            status = payload.get("status", event_type)
            error = payload.get("error_message") or payload.get("error", "")
            subject = f"Datanika run {status} (run #{run_id})"
            body = _build_email_html(event_type, run_id, status, error)
        email_service.send(to, subject, body)

    def _dispatch_slack(self, channel, event_type, payload):
        webhook_url = channel.config["webhook_url"]
        if event_type == "quota_warning":
            text = _build_quota_warning_slack_text(payload)
        else:
            run_id = payload.get("run_id", "?")
            status = payload.get("status", event_type)
            error = payload.get("error_message") or payload.get("error", "")
            icon = ":x:" if event_type == "run_failure" else ":white_check_mark:"
            text = f"{icon} *Datanika run {status}* (run #{run_id})"
            if error:
                text += "\n> " + error
        httpx.post(webhook_url, json={"text": text}, timeout=10)

    def _dispatch_telegram(self, channel, event_type, payload):
        token = channel.config["token"]
        chat_id = channel.config["chat_id"]
        if event_type == "quota_warning":
            text = _build_quota_warning_telegram_text(payload)
        else:
            run_id = payload.get("run_id", "?")
            status = payload.get("status", event_type)
            error = payload.get("error_message") or payload.get("error", "")
            icon = "X" if event_type == "run_failure" else "OK"
            text = f"[{icon}] Datanika run {status} (run #{run_id})"
            if error:
                text += "\n" + error
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        httpx.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)

    def _dispatch_webhook(self, channel, event_type, payload):
        url = channel.config["url"]
        body = {"event": event_type, **payload}
        httpx.post(url, json=body, timeout=10)


def _build_quota_warning_email(payload):
    metric_label = payload.get("metric_label", payload.get("metric", "usage"))
    used = payload.get("used", 0)
    limit = payload.get("limit", 0)
    plan_name = payload.get("plan_name", "your")
    pct = payload.get("pct", 0)
    subject = f"Datanika quota warning - 80% of {metric_label} used"
    body = (
        "<!DOCTYPE html><html><body>"
        "<h2>Quota warning</h2>"
        f"<p>Your {plan_name} plan has used <strong>{used:,} of {limit:,} {metric_label}</strong> "
        f"({pct}%) for the current billing period.</p>"
        "<p>Head to your billing dashboard to review usage or upgrade.</p>"
        "<p>Sent by Datanika.</p>"
        "</body></html>"
    )
    return subject, body


def _build_quota_warning_slack_text(payload):
    metric_label = payload.get("metric_label", payload.get("metric", "usage"))
    used = payload.get("used", 0)
    limit = payload.get("limit", 0)
    plan_name = payload.get("plan_name", "your")
    pct = payload.get("pct", 0)
    return (
        f":warning: *Datanika quota warning* - your {plan_name} plan has used "
        f"*{used:,} of {limit:,} {metric_label}* ({pct}%)."
    )


def _build_quota_warning_telegram_text(payload):
    metric_label = payload.get("metric_label", payload.get("metric", "usage"))
    used = payload.get("used", 0)
    limit = payload.get("limit", 0)
    plan_name = payload.get("plan_name", "your")
    pct = payload.get("pct", 0)
    return (
        f"[!] Datanika quota warning - {plan_name} plan has used "
        f"{used:,} of {limit:,} {metric_label} ({pct}%)."
    )


def _build_email_html(event_type, run_id, status, error):
    label = "FAILED" if event_type == "run_failure" else "SUCCEEDED"
    err_html = ""
    if error:
        err_html = "<p>Error: " + str(error) + "</p>"
    parts = [
        "<!DOCTYPE html><html><body>",
        f"<h2>Run {label}</h2>",
        f"<p>Run #{run_id} has {status}.</p>",
        err_html,
        "<p>Sent by Datanika.</p>",
        "</body></html>",
    ]
    return "".join(parts)


def register_hooks(service):
    """Register run completion hook handlers on the global hook bus."""
    from datanika import hooks

    def _on_run_completed(session, org_id, run_id, status, error_message=None, **kw):
        evt = "run_failure" if status == "failed" else "run_success"
        pl = {"run_id": run_id, "status": status, "error_message": error_message}
        service.notify(session, org_id, evt, pl)

    hooks.on("run.upload_completed", _on_run_completed)
    hooks.on("run.models_completed", _on_run_completed)
    hooks.on("run.transformation_completed", _on_run_completed)

"""NotificationService."""

import logging
from datetime import UTC, datetime

import httpx
from sqlalchemy import select

from datanika.models.notification_channel import (
    MAX_LAST_ERROR,
    ChannelType,
    DeliveryStatus,
    NotificationChannel,
)
from datanika.models.pii import NotificationChannelPII

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
        self._sync_channel_pii(session, ch)
        return ch

    @staticmethod
    def _sync_channel_pii(session, ch) -> None:
        """Mirror the delivery address into ``notification_channel_pii`` (release N).

        ``config`` mixes a **personal datum** with **secrets** in one JSON column — the
        recipient sits beside the Slack webhook URL and the Telegram bot token. Only the
        recipient moves: secrets are an org property, not personal data, and folding them
        in would imply that erasure must decrypt them, which it must not.

        Dual-write. ``config`` keeps its copy through N because the previously deployed
        code reads ``channel.config["email"]`` directly.

        ⚠️ Unlike the other two sidecars, **the spec's §4 contract release never removes
        this copy** — it drops five *columns* and says nothing about the JSON, so without a
        data step in N+1/N+2 the address stays in ``config`` after the chain finishes.
        Raised on core#655; recorded here because this is where the second copy is made.
        """
        recipient = None
        if isinstance(ch.config, dict):
            recipient = ch.config.get("email") or ch.config.get("chat_id")
        existing = session.get(NotificationChannelPII, ch.id)
        if not recipient:
            if existing is not None:
                session.delete(existing)
                session.flush()
            return
        if existing is None:
            session.add(NotificationChannelPII(channel_id=ch.id, recipient=recipient))
        else:
            existing.recipient = recipient
        session.flush()

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
        self._sync_channel_pii(session, ch)
        return ch

    def delete_channel(self, session, channel_id, org_id):
        ch = self._get_channel(session, channel_id, org_id)
        if ch is None:
            return False
        ch.deleted_at = datetime.now(UTC)
        session.flush()
        return True

    # ------------------------------------------------------------------
    # Delivery record (core#652)
    # ------------------------------------------------------------------

    @staticmethod
    def _redact(text, channel) -> str:
        """Strip every secret this channel holds out of an error string.

        ``httpx`` puts the request URL in the message of every ``HTTPStatusError``
        it raises, and the Telegram URL embeds the bot token — so the naive
        ``str(exc)`` is a credential. Redaction works off ``config``'s own values
        rather than a URL pattern: the config *is* the list of secrets, so a new
        channel type gets this for free instead of needing a new regex.
        """
        out = str(text)
        config = channel.config if isinstance(channel.config, dict) else {}
        # Longest first. If a short config value is a substring of a longer one
        # (a Telegram `chat_id` inside its own bot token, say), replacing the
        # short one first breaks up the long one so its own pass no longer
        # matches — leaving most of the longer secret sitting in the column.
        for value in sorted((v for v in config.values() if v), key=lambda v: -len(str(v))):
            out = out.replace(str(value), "[redacted]")
        if len(out) > MAX_LAST_ERROR:
            out = out[: MAX_LAST_ERROR - 3] + "..."
        return out

    def _record_delivery(self, session, channel, status, error=None) -> None:
        """Write the outcome of one attempt. **The only writer of these columns.**

        Never raises: an observability write that can break the thing it observes
        is worse than no observability. A failure here is logged and dropped, and
        the delivery itself has already happened either way.
        """
        try:
            channel.last_attempt_at = datetime.now(UTC)
            channel.last_status = str(status)
            channel.last_error = self._redact(error, channel) if error else None
            session.flush()
        except Exception:
            logger.exception("Could not record delivery state for channel %s", channel.id)

    def notify(self, session, org_id, event_type, payload):
        """Dispatch to every active channel subscribed to ``event_type``.

        ⚠️ There is deliberately **no** ``email_service`` parameter. One existed,
        keyword-only and defaulted to ``None``, and no caller anywhere ever passed
        it — so ``_dispatch_email`` returned at its first guard on every org, in
        every edition, for the life of the feature. A parameter no caller passes
        is not a seam, it is a defect that compiles, and wiring it would have left
        the same defect one new caller away. Email goes through the Celery task
        instead (core#652 D1).

        The ``except`` below still catches, because one dead webhook must not
        silence a healthy Slack channel — but it now **records** before moving on,
        which is the difference between resilience and a swallow.
        """
        for ch in self.list_channels(session, org_id):
            if not ch.is_active:
                continue
            if event_type not in ch.events:
                continue
            try:
                status, detail = self._dispatch(ch, event_type, payload)
                # ⚠️ A `None` status means "the outcome is not mine to record" —
                # the email path hands the row to the Celery task, which knows
                # whether the message actually left. Recording here anyway
                # overwrites the task's verdict with a placeholder, and in eager
                # execution it does so *after* the task has already written the
                # truth. Silently stamping `str(None)` over `success` is how an
                # observability column starts lying.
                if status is not None:
                    self._record_delivery(session, ch, status, detail)
            except Exception as exc:
                logger.exception(
                    "Failed to dispatch %s notification via channel %s (id=%s)",
                    event_type,
                    ch.name,
                    ch.id,
                )
                self._record_delivery(
                    session, ch, DeliveryStatus.FAILED, f"{type(exc).__name__}: {exc}"
                )

    def _dispatch(self, channel, event_type, payload):
        """Return ``(status, detail)``. Raising is also allowed — ``notify``
        records that as a failure with the exception as the detail."""
        if channel.channel_type == ChannelType.EMAIL:
            return self._dispatch_email(channel, event_type, payload)
        if channel.channel_type == ChannelType.SLACK:
            return self._dispatch_slack(channel, event_type, payload)
        if channel.channel_type == ChannelType.TELEGRAM:
            return self._dispatch_telegram(channel, event_type, payload)
        if channel.channel_type == ChannelType.WEBHOOK:
            return self._dispatch_webhook(channel, event_type, payload)
        return DeliveryStatus.SKIPPED, f"unknown channel type {channel.channel_type!r}"

    def _dispatch_email(self, channel, event_type, payload):
        """Hand the message to the mail task, synchronously enough to report.

        Dispatch runs inside the run-completion hook, which runs inside the
        worker's task: a blocking SMTP round trip there delays the task that is
        reporting a *finished* run, and a transient relay failure would have no
        retry. ``send_channel_notification_email_task`` carries ``autoretry_for``
        and writes the terminal outcome back to this row itself, so what this
        method reports is only the part it can actually know.
        """
        from datanika.config import settings

        if not settings.smtp_host:
            # ⚠️ Not "email service disabled" — that phrasing named a cause that
            # was never the one that fired and sent every reader to inspect a
            # working relay. This branch means exactly one thing: no SMTP relay
            # is configured on this deployment.
            logger.info(
                "No SMTP relay configured; skipping email notification for channel %s",
                channel.id,
            )
            return DeliveryStatus.SKIPPED, "SMTP is not configured on this deployment"

        to = channel.config["email"]
        if event_type == "quota_warning":
            subject, body = _build_quota_warning_email(payload)
        elif event_type == "charge_incoming":
            subject, body = _build_charge_incoming_email(payload)
        else:
            run_id = payload.get("run_id", "?")
            status = payload.get("status", event_type)
            error = payload.get("error_message") or payload.get("error", "")
            subject = f"Datanika run {status} (run #{run_id})"
            body = _build_email_html(event_type, run_id, status, error)

        from datanika.tasks.email_tasks import send_channel_notification_email_task

        send_channel_notification_email_task.delay(channel.id, channel.org_id, to, subject, body)
        # The task owns the terminal status. Returning None leaves the row for it
        # to write rather than stamping a "success" that only means "enqueued".
        return None, None

    def _dispatch_slack(self, channel, event_type, payload):
        webhook_url = channel.config["webhook_url"]
        if event_type == "quota_warning":
            text = _build_quota_warning_slack_text(payload)
        elif event_type == "charge_incoming":
            text = _build_charge_incoming_slack_text(payload)
        else:
            run_id = payload.get("run_id", "?")
            status = payload.get("status", event_type)
            error = payload.get("error_message") or payload.get("error", "")
            icon = ":x:" if event_type == "run_failure" else ":white_check_mark:"
            text = f"{icon} *Datanika run {status}* (run #{run_id})"
            if error:
                text += "\n> " + error
        return _post(webhook_url, {"text": text})

    def _dispatch_telegram(self, channel, event_type, payload):
        token = channel.config["token"]
        chat_id = channel.config["chat_id"]
        if event_type == "quota_warning":
            text = _build_quota_warning_telegram_text(payload)
        elif event_type == "charge_incoming":
            text = _build_charge_incoming_telegram_text(payload)
        else:
            run_id = payload.get("run_id", "?")
            status = payload.get("status", event_type)
            error = payload.get("error_message") or payload.get("error", "")
            icon = "X" if event_type == "run_failure" else "OK"
            text = f"[{icon}] Datanika run {status} (run #{run_id})"
            if error:
                text += "\n" + error
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        return _post(url, {"chat_id": chat_id, "text": text})

    def _dispatch_webhook(self, channel, event_type, payload):
        url = channel.config["url"]
        body = {"event": event_type, **payload}
        return _post(url, body)


def _post(url, json_body):
    """POST and **judge the response**.

    There was no ``raise_for_status()`` anywhere in this module, so a 401 from
    Slack, a 404 webhook and a revoked Telegram token each returned a response
    object and the code carried on as though delivered. Checking the status here
    rather than at each call site is what stops the fourth channel type from
    forgetting.
    """
    response = httpx.post(url, json=json_body, timeout=10)
    status_code = getattr(response, "status_code", None)
    # ⚠️ `isinstance(..., int)`, not a truthiness or `is not None` check. A real
    # httpx response always carries an int, but several existing tests patch
    # `httpx.post` with a bare MagicMock, whose auto-created `.status_code`
    # answers comparisons with `NotImplemented` — so `status_code >= 400` raises
    # `TypeError`, which `notify` would then dutifully record as a delivery
    # failure. Unmeasurable is not the same as failed, and a status we cannot
    # read is not evidence of anything.
    if isinstance(status_code, int) and status_code >= 400:
        return DeliveryStatus.FAILED, f"endpoint returned HTTP {status_code}"
    return DeliveryStatus.SUCCESS, None


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


def _build_charge_incoming_email(payload):
    amount = payload.get("amount_display", "")
    gb = payload.get("gb_display", "0")
    cycle = payload.get("cycle_ends_at", "")
    # Cloud emits plan_name optionally; when missing/empty we switch to a
    # plan-agnostic phrase rather than render an empty <strong/> or the
    # doubled-up "Your your plan" you get from a naive default.
    plan_name = payload.get("plan_name") or ""
    plan_phrase = f"Your <strong>{plan_name}</strong> plan" if plan_name else "Your subscription"
    subject = f"Datanika upcoming overage charge - {amount}"
    body = (
        "<!DOCTYPE html><html><body>"
        "<h2>Upcoming overage charge</h2>"
        f"<p>{plan_phrase} will be charged approximately "
        f"<strong>{amount}</strong> for <strong>{gb} GB</strong> of overage "
        f"when your billing cycle closes on <strong>{cycle}</strong>.</p>"
        "<p>Review current usage and the projected invoice in "
        '<a href="/settings">Settings &rarr; Billing</a>.</p>'
        "<p>Sent by Datanika.</p>"
        "</body></html>"
    )
    return subject, body


def _build_charge_incoming_slack_text(payload):
    amount = payload.get("amount_display", "")
    gb = payload.get("gb_display", "0")
    cycle = payload.get("cycle_ends_at", "")
    plan_name = payload.get("plan_name") or ""
    plan_phrase = f"your {plan_name} plan" if plan_name else "your subscription"
    return (
        f":moneybag: *Datanika upcoming overage charge* - {plan_phrase} "
        f"will be charged *{amount}* for *{gb} GB* of overage when the cycle "
        f"closes on *{cycle}*."
    )


def _build_charge_incoming_telegram_text(payload):
    amount = payload.get("amount_display", "")
    gb = payload.get("gb_display", "0")
    cycle = payload.get("cycle_ends_at", "")
    plan_name = payload.get("plan_name") or ""
    plan_phrase = f"your {plan_name} plan" if plan_name else "your subscription"
    return (
        f"[$] Datanika upcoming overage charge - {plan_phrase} will be "
        f"charged {amount} for {gb} GB of overage when the cycle closes on {cycle}."
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
    # Failures arrive on their own event, not on `run.*_completed` with
    # status="failed" (core#465). datanika-cloud's four metering handlers
    # subscribe to those three and call `record_usage` unconditionally —
    # none of them check `status` — so reusing them would bill the user for
    # a run that failed. The separation is structural rather than a status
    # check we would be trusting another repo's handlers to keep.
    hooks.on("run.failed", _on_run_completed)

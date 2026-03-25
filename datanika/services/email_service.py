"""Email sending service using stdlib smtplib."""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


class EmailService:
    """Sends emails via SMTP.  Disabled when ``smtp_host`` is empty."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        smtp_user: str,
        smtp_password: str,
        smtp_from_email: str,
        smtp_from_name: str,
        smtp_use_tls: bool,
        frontend_url: str,
    ):
        self._host = smtp_host
        self._port = smtp_port
        self._user = smtp_user
        self._password = smtp_password
        self._from_email = smtp_from_email
        self._from_name = smtp_from_name
        self._use_tls = smtp_use_tls
        self._frontend_url = frontend_url.rstrip("/")

    def is_enabled(self) -> bool:
        return bool(self._host)

    def send(self, to: str, subject: str, html_body: str) -> bool:
        """Send an HTML email.  Returns True on success, False on failure."""
        if not self.is_enabled():
            return False

        msg = MIMEMultipart("alternative")
        msg["From"] = f"{self._from_name} <{self._from_email}>"
        msg["To"] = to
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))

        try:
            with smtplib.SMTP(self._host, self._port) as server:
                if self._use_tls:
                    server.starttls()
                if self._user:
                    server.login(self._user, self._password)
                server.sendmail(self._from_email, to, msg.as_string())
            return True
        except Exception:
            logger.exception("Failed to send email to %s", to)
            return False

    # ------------------------------------------------------------------
    # Templated emails
    # ------------------------------------------------------------------

    def send_verification_email(self, to: str, token: str) -> bool:
        url = f"{self._frontend_url}/api/verify-email?token={token}"
        html = _VERIFY_EMAIL_TEMPLATE.format(url=url, app_name="Datanika")
        return self.send(to, "Verify your email — Datanika", html)

    def send_invitation_email(
        self, to: str, org_name: str, inviter_name: str, token: str
    ) -> bool:
        url = f"{self._frontend_url}/api/accept-invite?token={token}"
        html = _INVITATION_TEMPLATE.format(
            url=url, org_name=org_name, inviter_name=inviter_name, app_name="Datanika"
        )
        return self.send(to, f"You're invited to {org_name} — Datanika", html)


# ------------------------------------------------------------------
# HTML templates (simple string.format — no Jinja dependency)
# ------------------------------------------------------------------

_VERIFY_EMAIL_TEMPLATE = """\
<!DOCTYPE html>
<html>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; \
max-width: 560px; margin: 0 auto; padding: 40px 20px;">
  <h2 style="color: #1a1a2e;">Verify your email</h2>
  <p style="color: #444; line-height: 1.6;">
    Thanks for signing up for {app_name}. Click the button below to verify your email address.
  </p>
  <p style="margin: 32px 0;">
    <a href="{url}" style="background: #7c3aed; color: #fff; padding: 12px 28px; \
border-radius: 6px; text-decoration: none; font-weight: 600;">Verify Email</a>
  </p>
  <p style="color: #888; font-size: 13px;">
    If you didn't create an account, you can safely ignore this email.
  </p>
  <p style="color: #888; font-size: 13px;">
    Or copy this link: {url}
  </p>
</body>
</html>"""

_INVITATION_TEMPLATE = """\
<!DOCTYPE html>
<html>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; \
max-width: 560px; margin: 0 auto; padding: 40px 20px;">
  <h2 style="color: #1a1a2e;">You're invited to {org_name}</h2>
  <p style="color: #444; line-height: 1.6;">
    {inviter_name} has invited you to join <strong>{org_name}</strong> on {app_name}.
  </p>
  <p style="margin: 32px 0;">
    <a href="{url}" style="background: #7c3aed; color: #fff; padding: 12px 28px; \
border-radius: 6px; text-decoration: none; font-weight: 600;">Accept Invitation</a>
  </p>
  <p style="color: #888; font-size: 13px;">
    If you don't have an account yet, you'll be asked to sign up first.
  </p>
  <p style="color: #888; font-size: 13px;">
    Or copy this link: {url}
  </p>
</body>
</html>"""

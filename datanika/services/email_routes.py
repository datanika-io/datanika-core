"""Starlette routes for email verification and invitation acceptance."""

import logging

from sqlalchemy import select as sa_select
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.routing import Route

from datanika.config import settings
from datanika.services.auth import AuthService

logger = logging.getLogger(__name__)

_auth = AuthService(settings.secret_key)


async def verify_email(request: Request) -> RedirectResponse:
    """GET /api/verify-email?token=... — marks user email as verified."""
    token = request.query_params.get("token", "")
    payload = _auth.decode_token(token, expected_type="email_verify")

    if not payload:
        return RedirectResponse(
            url=f"{settings.frontend_url}/login?verify_error=1", status_code=302
        )

    user_id = payload.get("user_id")
    if not user_id:
        return RedirectResponse(
            url=f"{settings.frontend_url}/login?verify_error=1", status_code=302
        )

    try:
        from datanika.models.user import User
        from datanika.ui.state.base_state import get_sync_session

        with get_sync_session() as session:
            user = session.get(User, user_id)
            if user and not user.email_verified:
                user.email_verified = True
                session.commit()
    except Exception:
        logger.exception("Failed to verify email for user %s", user_id)
        return RedirectResponse(
            url=f"{settings.frontend_url}/login?verify_error=1", status_code=302
        )

    return RedirectResponse(
        url=f"{settings.frontend_url}/login?verified=1", status_code=302
    )


async def accept_invite(request: Request) -> RedirectResponse:
    """GET /api/accept-invite?token=... — accept an org invitation."""
    token = request.query_params.get("token", "")

    if not token:
        return RedirectResponse(
            url=f"{settings.frontend_url}/login?invite_error=1", status_code=302
        )

    try:
        from datanika.models.user import User
        from datanika.services.invitation_service import InvitationService
        from datanika.ui.state.base_state import get_sync_session

        inv_svc = InvitationService(_auth)

        with get_sync_session() as session:
            invitation = inv_svc.get_invitation_by_token(session, token)
            if invitation is None:
                return RedirectResponse(
                    url=f"{settings.frontend_url}/login?invite_error=1", status_code=302
                )

            # Check if user exists
            user = session.execute(
                sa_select(User).where(User.email == invitation.email)
            ).scalar_one_or_none()

            if user is None:
                # User needs to register first — redirect to signup with token + email
                from urllib.parse import quote

                email_param = quote(invitation.email)
                return RedirectResponse(
                    url=f"{settings.frontend_url}/signup?invite_token={token}&email={email_param}",
                    status_code=302,
                )

            # User exists — accept the invitation
            membership = inv_svc.accept_invitation(session, token)
            if membership is None:
                return RedirectResponse(
                    url=f"{settings.frontend_url}/login?invite_error=1", status_code=302
                )
            session.commit()

    except Exception:
        logger.exception("Failed to process invitation")
        return RedirectResponse(
            url=f"{settings.frontend_url}/login?invite_error=1", status_code=302
        )

    return RedirectResponse(
        url=f"{settings.frontend_url}/login?invite_accepted=1", status_code=302
    )


email_routes = [
    Route("/api/verify-email", endpoint=verify_email, methods=["GET"]),
    Route("/api/accept-invite", endpoint=accept_invite, methods=["GET"]),
]

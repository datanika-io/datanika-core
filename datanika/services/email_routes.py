"""Starlette routes for email verification and invitation acceptance."""

import logging

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


email_routes = [
    Route("/api/verify-email", endpoint=verify_email, methods=["GET"]),
]

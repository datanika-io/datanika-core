"""Base state with auth-based org_id and sync session helper."""

import logging

import reflex as rx
from pydantic import ValidationError
from sqlalchemy.orm import Session

from datanika.db import get_sync_session  # noqa: F401 — re-exported

_log = logging.getLogger(__name__)

ROLE_HIERARCHY = {"owner": 4, "admin": 3, "editor": 2, "viewer": 1}


def check_role_hierarchy(current_role: str, required_role: str) -> bool:
    """Check if current_role meets or exceeds required_role."""
    return ROLE_HIERARCHY.get(current_role, 0) >= ROLE_HIERARCHY.get(required_role, 0)


def is_user_facing(exc: Exception) -> bool:
    """Is this exception's own text something we authored, and safe to show? (core#1032)

    ``ValueError`` is this codebase's deliberate carrier for user-facing text —
    quota refusals, validation refusals, ``connection_service`` says so in a
    docstring — so ``isinstance(exc, ValueError)`` was the right *intent*. It is
    the wrong *boundary*, because **pydantic's ``ValidationError`` is a subclass
    of ``ValueError``**, and it is a class we do not author.

    The consequence was measured on the unfixed billing page (cloud#164), where
    ``error_message`` became::

        1 validation error for SubscriptionInfo
        max_schedules
          Input should be a valid integer [type=int_type, input_value=None, ...]

    while the handler's own ``"Failed to load billing data"`` was passed and
    never used. Note ``input_value=``: a pydantic report echoes the offending
    value into a rendered string, which is not what a method called "safe error"
    should do.

    Two things settled by measurement rather than by reading, both of which the
    issue flagged as easy to get wrong:

    * ``pydantic.ValidationError`` **is** ``pydantic_core.ValidationError`` — the
      same object, not an alias to a wrapper — so the documented public name is
      correct here. Pinned by ``test_the_two_import_paths_name_the_same_class``.
    * ``PydanticUserError`` and ``PydanticSchemaGenerationError`` are **not**
      ``ValueError`` subclasses, so they already reached the fallback and this
      needs no widening.

    🔑 **It is a module-level function, and both callers use it, deliberately.**
    ``_safe_error`` and ``_set_error`` carried this branch *twice*; the issue
    named only one of them, and fixing one would have left the identical defect
    live on ``save_connection``, ``save_pipeline``, ``save_schedule``,
    ``add_member`` and ``save_upload``. One predicate is what stops them
    diverging again, and ``TestTheTwoStayInStep`` is what notices if they do.

    A ``UserFacingError`` marker class would be the better long-term shape, but
    that is ~30 raise sites and its own decision; this is the narrowing that
    keeps every intended case working today.
    """
    return isinstance(exc, ValueError) and not isinstance(exc, ValidationError)


class BaseState(rx.State):
    """Base state with org_id from AuthState available to all substates."""

    error_message: str = ""
    is_quota_error: bool = False
    # V2 pricing pivot — QuotaExceededError metric discriminator. Populated
    # from ``getattr(exc, 'metric', '')`` so the attribute is optional on
    # cloud's QuotaExceededError; blank until Engineering adds the attr.
    # Possible values: "bytes_processed", "runs", "connections", "schedules",
    # "seats", "sso".
    quota_metric: str = ""

    async def _get_org_id(self) -> int:
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        return auth.current_org.id if auth.current_org.id else 0

    async def _require_live_session(self) -> bool:
        """Whether this session may still act — the session half of ``_check_role``.

        Extracted so a mutation that every member may perform can check the
        session **without** acquiring a role gate it must not have (#673).
        ``SettingsState.leave_org`` is the case that forces the split: its own
        contract is that leaving is the one member-management action available
        to every member, refused only by the service's owner-count invariant.
        Guarding it with ``_check_role`` would have contradicted that.

        The three classes of committing handler, and why a blanket decorator
        would have been wrong for two of them:

        * **role-gated mutations** — ``_check_role(min_role)``, which now calls
          this first.
        * **mutations every member may perform** — this method, alone:
          ``leave_org``, ``change_password``, dismissing your own notification
          or onboarding checklist, saving a catalog entry, receiving an upload.
        * **unauthenticated entry points** — ``login``, ``signup``, ``logout``,
          password reset. Neither guard. A guard on ``logout`` in particular
          strands a user in a session they cannot end.

        The common case stays free: ``_revalidate_session`` returns on a
        signature check with **no database read**, and only an aged-out token
        pays for a query.

        ⚠️ **``_get_org_id`` is still deliberately NOT guarded** (#673 AC5) and
        this method must not be called from it. It is on the read path and runs
        while rendering, so revalidating there would put a session decision —
        and, on renewal, a database write — inside template evaluation, where
        the failure mode is a half-rendered page rather than a refused action.
        """
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)

        if auth._revalidate_session():
            return True

        auth._clear_session()
        auth.session_expired = True
        # Not an error_message: "Permission denied. Requires admin role or
        # higher." is the wrong thing to tell somebody who needs to sign in
        # — it sends them to ask an admin for access they already have. The
        # layout renders the translated signed-out panel off the flag.
        self.error_message = ""
        auth.action_error = ""
        return False

    async def _check_role(self, min_role: str) -> bool:
        """Check that the session is live **and** carries at least ``min_role``.

        Two questions, in this order, because they have different answers (#673).

        #671 put session revalidation in ``AuthState.check_auth``, which runs
        from ``on_load`` — so it fires on *navigation*. A tab that is already
        open and never navigates again keeps acting on the state object it
        holds, so a mutating handler still executed for a session whose access
        token had aged out, and transitively for one a password change was meant
        to end. Every such handler already routes through here, so this is the
        one place that covers all of them.

        The common case stays free: ``_revalidate_session`` returns on a
        signature check with **no database read**, and only an aged-out token
        pays for a query. That is what makes a per-handler guard affordable, and
        it is asserted by a test.

        ⚠️ **``_get_org_id`` is deliberately NOT guarded** (#673 AC5). It is on
        the read path and is called while rendering, so revalidating there would
        put a session decision — and, on renewal, a database write — inside
        template evaluation, where the failure mode is a half-rendered page
        rather than a refused action. Page loads are already covered by
        ``check_auth``, so the exposure left is *reading* stale data in a tab
        that never navigates, which is bounded by that tab staying open. Writes
        are the thing worth stopping, and writes come through here.
        """
        from datanika.ui.state.auth_state import AuthState

        if not await self._require_live_session():
            return False

        auth = await self.get_state(AuthState)
        role = auth.current_role
        if not check_role_hierarchy(role, min_role):
            self.error_message = f"Permission denied. Requires {min_role} role or higher."
            # ⚠️ ``self.error_message`` is the SUBSTATE's own copy, and for most
            # callers no page renders it — 10 of the 15 state classes that
            # assign ``error_message`` are read by nothing (#887), `uploads.py`
            # among them. So for the majority of the 31 call sites the sentence
            # above went to a var with no reader, and the button did nothing and
            # said nothing (#744). ``AuthState`` is a single object that
            # ``page_layout`` already reads, so record it there too.
            auth.action_error = self.error_message
            return False
        # A permitted action retires the previous refusal. Navigation clears it
        # as well (``check_auth``), but somebody who is granted the role and
        # retries in place must not still be looking at the "no".
        auth.action_error = ""
        return True

    @staticmethod
    def _audit(
        session: Session,
        org_id: int,
        user_id: int,
        action: str,
        resource_type: str,
        resource_id: int | None = None,
        old_values: dict | None = None,
        new_values: dict | None = None,
    ):
        """Log an audit entry. Never raises — but never fails silently either."""
        try:
            from datanika.models.audit_log import AuditAction
            from datanika.services.audit_service import AuditService

            AuditService().log_action(
                session,
                org_id,
                user_id,
                AuditAction(action),
                resource_type,
                resource_id=resource_id,
                old_values=old_values,
                new_values=new_values,
            )
        except Exception:
            # The swallow is deliberate: audit logging must never break the
            # operation it describes. The LOG is what makes it safe -- without
            # it the trail can stop recording and "no audit rows" is
            # indistinguishable from "nothing happened" (core#723).
            _log.exception(
                "Audit write failed and was dropped: action=%s resource=%s org=%s user=%s",
                action,
                resource_type,
                org_id,
                user_id,
            )

    def _set_error(self, exc: Exception, fallback: str = "An error occurred") -> None:
        """Set error_message, is_quota_error, and quota_metric from an exception."""
        _log.exception("Caught exception in state handler")
        self.is_quota_error = type(exc).__name__ == "QuotaExceededError"
        self.quota_metric = (getattr(exc, "metric", None) or "") if self.is_quota_error else ""
        self.error_message = str(exc) if is_user_facing(exc) else fallback

    @staticmethod
    def _safe_error(exc: Exception, fallback: str = "An error occurred") -> str:
        """Return a user-safe error message. Logs the full exception."""
        _log.exception("Caught exception in state handler")
        return str(exc) if is_user_facing(exc) else fallback

    async def _deleted_toast(self, key: str, fallback: str):
        """A translated success toast for a destructive handler (core#804, core#851).

        Every confirmed delete in this product removes a row and says nothing.
        For a **soft** delete — which all of them are — a row silently vanishing
        reads far more alarming than the operation is, and the documented
        consequence is worse than alarm: a Reflex table can render a stale row
        set right after a successful mutation (core#872), so "the row is still
        there" is not evidence the delete failed, and the user's natural next
        move is to click again.

        The lookup goes through the reactive ``I18nState`` dict rather than a
        hardcoded string, so the eight non-English locales are not silently
        English here. ``fallback`` is only reached if the key is missing, which
        the i18n parity test makes impossible — it exists so a missing key
        degrades to a plain word instead of a ``KeyError`` inside a delete.
        """
        return rx.toast.success(
            await self._translated(key, fallback),
            position="top-right",
        )

    async def _saved_toast(self, key: str, fallback: str):
        """The constructive twin of :meth:`_deleted_toast` (core#872).

        Deleting was instrumented by core#804 and core#851 — ten confirmation
        dialogs and nine success toasts. **Creating was not.** Measured on
        production while creating a connection: the create succeeded, and there
        was no toast, no inline confirmation, no error, the same old rows for at
        least five seconds, and an apparently unchanged form. Every signal the
        user had said *nothing happened*, and the move that invites is to click
        Create again.

        🚨 That stopped being cosmetic on 2026-08-31, when connection quota
        enforcement went live. On a Free org at 4 of 5 connections an invisible
        first create **spends the last slot**, so the *second* click is the one
        refused — the success and the failure reach the user in the opposite
        order from the one they perceive, and the error they read describes the
        wrong event.

        ⚠️ **This must be `yield`ed, never returned.** A plain ``async def``
        handler sends a single state update after it returns, so a returned
        toast never reaches the browser and nothing fails. Yielding is also what
        makes the handler a generator, which is the actual lever here: the
        refetch these handlers already end with was never the missing piece.
        """
        return rx.toast.success(
            await self._translated(key, fallback),
            position="top-right",
        )

    async def _translated(self, key: str, fallback: str) -> str:
        """One translated string, read from the reactive dict (core#851, core#862).

        Services raise plain English — they have no locale and no business
        having one. Anything a *user* reads has to be translated here instead,
        or eight of nine locales silently show English.
        """
        from datanika.ui.state.i18n_state import I18nState

        i18n = await self.get_state(I18nState)
        return i18n.translations.get(key, fallback)

"""core#1032 — a "safe error" must not hand pydantic's own report to the user.

``BaseState._safe_error`` and ``BaseState._set_error`` both exist to replace an
exception with a curated message, and both carry the same branch::

    if isinstance(exc, ValueError):
        return str(exc)          # intended: "a message we authored"

**Pydantic's ``ValidationError`` is a subclass of ``ValueError``.** So any state
handler that constructs a pydantic model inside its ``try`` showed the user
pydantic's report — including ``input_value=``, which echoes the offending value
back into a rendered string — while the handler's own fallback was passed and
never used.

Found by cloud#164, on the unfixed billing page, which rendered::

    1 validation error for SubscriptionInfo
    max_schedules
      Input should be a valid integer [type=int_type, input_value=None, ...]

Two things this file is careful about, because both were open questions on the
issue and both are settled by measurement rather than by reading:

* **The exception is raised from a real model, never fabricated.** The whole
  finding is that a *third-party* class sits inside ``ValueError``'s subtree; a
  test that hand-constructs the exception would encode my belief about the class
  hierarchy instead of measuring it. ``test_the_premise_still_holds`` asserts the
  hierarchy separately, so if pydantic ever stops subclassing ``ValueError`` this
  file says which of its assertions became vacuous.
* **The control is as load-bearing as the assertion.** This codebase raises
  ``ValueError`` deliberately to carry user-facing text — quota refusals,
  validation refusals — so a narrowing that catches too much silently replaces
  every one of those with "An error occurred". The quota case is exercised too,
  because ``_set_error`` reads attributes off the exception it is narrowing.
"""

import pydantic
import pydantic_core
import pytest
from pydantic import BaseModel

from datanika.ui.state.base_state import BaseState

#: Substrings that only ever appear in a pydantic report. Asserting on these
#: rather than on equality with the fallback keeps the test meaningful if the
#: fallback wording changes, and names *what* must not leak.
PYDANTIC_TELLS = ("validation error for", "input_value=", "errors.pydantic.dev", "[type=")

#: The message shape this codebase raises on purpose (connection_service.py's
#: docstring calls the ValueError deliberate). It must survive verbatim.
OURS = "Schedule limit reached (2 on Free plan)"


class _SubscriptionInfo(BaseModel):
    """The shape from cloud#164 — the model whose failure found this."""

    max_schedules: int


def _real_validation_error() -> ValueError:
    """Raise a genuine ``ValidationError`` from a genuine model."""
    try:
        _SubscriptionInfo(max_schedules=None)
    except ValueError as exc:
        return exc
    raise AssertionError("the model accepted None, so this file is no longer measuring anything")


def _carrier():
    """A minimal stand-in with ``_set_error`` bound to it.

    Same pattern as ``tests/test_ui/test_volume_quota_modal.py`` — instantiating
    a real Reflex state is not needed to exercise a method that only assigns
    attributes, and doing so would drag in the event system.
    """

    class _Carrier:
        error_message = ""
        is_quota_error = False
        quota_metric = ""

    _Carrier._set_error = BaseState._set_error
    return _Carrier()


class TestThePremise:
    def test_the_premise_still_holds(self):
        """If this goes red, the narrowing below is unnecessary, not broken."""
        exc = _real_validation_error()
        assert isinstance(exc, ValueError), (
            "pydantic no longer subclasses ValueError; _safe_error's plain "
            "isinstance branch would be correct again and this file is obsolete"
        )

    def test_the_two_import_paths_name_the_same_class(self):
        """The issue asked which of the two ``isinstance`` needs. Measured: either.

        ``pydantic.ValidationError`` is ``pydantic_core.ValidationError`` — the
        same object, not an alias to a wrapper — so the production guard can use
        the documented public name. Pinned because the issue explicitly flagged
        it as a thing to get wrong, and because if they ever diverge the guard
        has to be re-derived rather than assumed.
        """
        assert pydantic.ValidationError is pydantic_core.ValidationError
        assert isinstance(_real_validation_error(), pydantic.ValidationError)

    def test_pydantics_other_error_classes_are_not_valueerrors(self):
        """Scope control: only ``ValidationError`` needed narrowing.

        ``PydanticUserError`` and ``PydanticSchemaGenerationError`` are outside
        ``ValueError``'s subtree, so they already reached the fallback. Asserted
        so a future widening of the guard has to justify itself.
        """
        assert not issubclass(pydantic.PydanticUserError, ValueError)
        assert not issubclass(pydantic.PydanticSchemaGenerationError, ValueError)


class TestSafeError:
    def test_a_validation_error_yields_the_handlers_own_fallback(self):
        assert BaseState._safe_error(_real_validation_error(), "Failed to load billing data") == (
            "Failed to load billing data"
        )

    def test_no_pydantic_internal_reaches_the_returned_string(self):
        """Asserts the property, not the wording.

        Equality with the fallback is satisfied by any narrowing; this says what
        must not appear, so a future "helpful" change that appends the exception
        to the fallback still goes red.
        """
        got = BaseState._safe_error(_real_validation_error(), "Failed to load billing data")
        leaked = [t for t in PYDANTIC_TELLS if t in got]
        assert leaked == [], f"pydantic internals reached the user: {leaked} in {got!r}"

    def test_our_own_valueerror_still_reaches_the_user_verbatim(self):
        """The control. A narrowing that catches too much breaks exactly this."""
        assert BaseState._safe_error(ValueError(OURS), "An error occurred") == OURS

    def test_a_valueerror_subclass_of_ours_still_reaches_the_user_verbatim(self):
        """Quota refusals are a ValueError *subclass*, so subclassing must survive."""

        class QuotaExceededError(ValueError):
            pass

        assert BaseState._safe_error(QuotaExceededError(OURS), "An error occurred") == OURS

    def test_a_non_valueerror_still_yields_the_fallback(self):
        assert BaseState._safe_error(RuntimeError("boom"), "An error occurred") == (
            "An error occurred"
        )


class TestSetError:
    """``_set_error`` carries the identical branch and the issue names only its sibling.

    Fixing one and not the other would leave the same defect live on five
    handlers — ``save_connection``, ``save_pipeline``, ``save_schedule``,
    ``add_member`` and ``save_upload`` — which is the shape of a partial fix that
    closes an issue.
    """

    def test_a_validation_error_yields_the_handlers_own_fallback(self):
        state = _carrier()
        state._set_error(_real_validation_error(), "Failed to save connection")
        assert state.error_message == "Failed to save connection"

    def test_no_pydantic_internal_reaches_the_rendered_message(self):
        state = _carrier()
        state._set_error(_real_validation_error(), "Failed to save connection")
        leaked = [t for t in PYDANTIC_TELLS if t in state.error_message]
        assert leaked == [], f"pydantic internals reached the user: {leaked}"

    def test_our_own_valueerror_still_reaches_the_user_verbatim(self):
        state = _carrier()
        state._set_error(ValueError(OURS), "Failed to save schedule")
        assert state.error_message == OURS

    def test_a_quota_error_is_still_flagged_and_still_verbatim(self):
        """``_set_error``'s other two outputs must survive the narrowing.

        It sets ``is_quota_error`` and ``quota_metric`` off the exception, so a
        narrowing written without this control could keep the message correct and
        silently stop raising the upgrade modal.
        """

        class QuotaExceededError(ValueError):
            def __init__(self, message, metric):
                super().__init__(message)
                self.metric = metric

        state = _carrier()
        state._set_error(QuotaExceededError(OURS, "bytes_processed"), "Failed to save upload")
        assert state.error_message == OURS
        assert state.is_quota_error is True
        assert state.quota_metric == "bytes_processed"

    def test_a_validation_error_is_not_mistaken_for_a_quota_error(self):
        state = _carrier()
        state._set_error(_real_validation_error(), "Failed to save connection")
        assert state.is_quota_error is False
        assert state.quota_metric == ""


class TestTheTwoStayInStep:
    """The two methods diverging again is the failure this file most wants to stop."""

    @pytest.mark.parametrize(
        ("exc_factory", "expect_verbatim"),
        [
            (_real_validation_error, False),
            (lambda: ValueError(OURS), True),
            (lambda: RuntimeError("boom"), False),
        ],
        ids=["validation_error", "our_valueerror", "non_valueerror"],
    )
    def test_safe_error_and_set_error_agree(self, exc_factory, expect_verbatim):
        fallback = "Failed to do the thing"
        exc = exc_factory()

        returned = BaseState._safe_error(exc, fallback)
        state = _carrier()
        state._set_error(exc_factory(), fallback)

        assert returned == state.error_message, (
            "_safe_error and _set_error disagree; they carry the same branch and "
            "core#1032 was found because only one of them was looked at"
        )
        assert (returned == str(exc)) is expect_verbatim

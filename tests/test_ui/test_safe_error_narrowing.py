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

🆕 **core#1094 replaced the narrowing with a positive test**, and this file was
rewritten with it rather than deleted. ``is_user_facing`` is now
``isinstance(exc, UserFacingError)``; the pydantic exclusion is **gone**, not
kept "just in case" (cloud#176: a redundant guard also absorbs your ability to
detect the class arriving for real).

⚠️ **The filename still says "narrowing".** Kept deliberately — renaming it would
detach the history from the defect that produced it, and #1032 is the reason
every assertion below exists.

Three things this file is careful about:

* **The exception is raised from a real model, never fabricated.** The whole
  finding is that a *third-party* class sits inside ``ValueError``'s subtree; a
  test that hand-constructs the exception would encode a belief about the class
  hierarchy instead of measuring it. ``test_the_premise_still_holds`` asserts the
  hierarchy separately, so if pydantic ever stops subclassing ``ValueError`` this
  file says which of its assertions became vacuous.
* **The control is as load-bearing as the assertion.** This codebase raises
  ``UserFacingError`` deliberately to carry user-facing text — quota refusals,
  validation refusals — so a rule that catches too *little* silently replaces
  every one of those with "An error occurred". The quota case is exercised too,
  because ``_set_error`` reads attributes off the exception.
* 🆕 **The behaviour change is asserted, not implied.** A bare ``ValueError`` now
  yields the fallback. That is the whole point of the migration and it is the one
  thing a reader would want proved rather than described:
  ``test_a_bare_valueerror_now_yields_the_fallback``.
"""

import json

import pydantic
import pytest
from pydantic import BaseModel

from datanika.errors import UserFacingError
from datanika.ui.state.base_state import BaseState


class _ThirdPartyValueError(ValueError):
    """A stand-in for the NEXT dependency that puts a class under ValueError.

    Named rather than reached for, because the property being asserted is about
    a class nobody has heard of yet -- not about pydantic, which is only the
    instance that happened to bite us in core#1032.
    """


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
        """pydantic's report is a ``ValueError`` and is **not** ours.

        Both halves matter. The first is why #1032 happened at all; the second is
        why the positive rule fixes it *structurally* rather than by listing. If
        pydantic ever stops subclassing ``ValueError`` the first assertion becomes
        vacuous, and this file says so rather than quietly passing.
        """
        exc = _real_validation_error()
        assert isinstance(exc, ValueError), (
            "pydantic no longer subclasses ValueError; #1032's defect is no longer "
            "reachable and the assertions below describe a hazard that has gone"
        )
        assert not isinstance(exc, UserFacingError)

    def test_the_rule_needs_no_list_of_theirs(self):
        """The property the exclusion list was approximating, stated once.

        Any class we did not author is outside the marker **by construction**, not
        because somebody enumerated it. Three unrelated third-party shapes, and
        the point of the middle one is that it stands in for a class nobody has
        heard of yet.
        """
        for cls in (pydantic.ValidationError, _ThirdPartyValueError, json.JSONDecodeError):
            assert issubclass(cls, ValueError), f"{cls} is not even in the subtree"
            assert not issubclass(cls, UserFacingError), cls


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

    def test_our_own_marker_error_reaches_the_user_verbatim(self):
        """The control. A rule that catches too little breaks exactly this."""
        assert BaseState._safe_error(UserFacingError(OURS), "An error occurred") == OURS

    def test_a_marker_subclass_of_ours_still_reaches_the_user_verbatim(self):
        """Quota refusals are a *subclass*, so subclassing must survive."""

        class QuotaExceededError(UserFacingError):
            pass

        assert BaseState._safe_error(QuotaExceededError(OURS), "An error occurred") == OURS

    def test_a_bare_valueerror_now_yields_the_fallback(self):
        """core#1094's one behaviour change, asserted rather than described.

        Before the contract step a bare ``ValueError`` reached the user; now it
        does not. The 39 sites that relied on that were converted first, and
        ``tests/test_errors.py``'s census is what says the set is empty -- this is
        what says the rule actually changed.
        """
        assert BaseState._safe_error(ValueError(OURS), "An error occurred") == "An error occurred"

    def test_a_third_party_valueerror_subclass_yields_the_fallback(self):
        """The general property the exclusion list could only approximate.

        Not pydantic -- a class the old rule had never heard of. Under the
        negative rule this reached the user verbatim; under the positive one it
        cannot, and nobody has to add it to anything.
        """
        assert BaseState._safe_error(_ThirdPartyValueError("vendor internals"), "Nope") == "Nope"

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

    def test_our_own_marker_error_reaches_the_user_verbatim(self):
        state = _carrier()
        state._set_error(UserFacingError(OURS), "Failed to save schedule")
        assert state.error_message == OURS

    def test_a_bare_valueerror_now_yields_the_fallback(self):
        """The sibling of ``_safe_error``'s. Both callers change together or neither."""
        state = _carrier()
        state._set_error(ValueError(OURS), "Failed to save schedule")
        assert state.error_message == "Failed to save schedule"

    def test_a_quota_error_is_still_flagged_and_still_verbatim(self):
        """``_set_error``'s other two outputs must survive the narrowing.

        It sets ``is_quota_error`` and ``quota_metric`` off the exception, so a
        narrowing written without this control could keep the message correct and
        silently stop raising the upgrade modal.
        """

        class QuotaExceededError(UserFacingError):
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
            (lambda: UserFacingError(OURS), True),
            (lambda: ValueError(OURS), False),
            (lambda: _ThirdPartyValueError("vendor internals"), False),
            (lambda: RuntimeError("boom"), False),
        ],
        ids=[
            "validation_error",
            "our_marker_error",
            "bare_valueerror",
            "third_party_valueerror",
            "non_valueerror",
        ],
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

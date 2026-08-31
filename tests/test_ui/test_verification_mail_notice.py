"""The post-signup confirmation banner (core#700 AC1).

Signup authenticates the user and redirects straight to their destination, so a callout on
`/signup` would never be seen. The notice lives in the app shell instead, driven by
`AuthState.verification_mail_state`.

The neighbouring assertions in `tests/test_services/test_email_dispatch_observability.py`
cover the *outcome* being recorded at all; this file covers it being rendered.
"""

import json
from pathlib import Path

import pytest

I18N = Path("datanika/i18n")
LAYOUT = Path("datanika/ui/components/layout.py")


def _build_layout_capturing_output():
    """Return (rendered, captured_stdout_stderr) for a shell built with a body."""
    import contextlib
    import io

    import reflex as rx

    from datanika.ui.components.layout import page_layout

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        rendered = str(page_layout(rx.text("body"), title="T"))
    return rendered, buf.getvalue()


class TestTheShellMountsTheNotice:
    def test_page_layout_mounts_it(self):
        """Rendered from ``page_layout``, not from the banner helper.

        Building the helper directly proves the helper works and says nothing about
        whether the shell mounts it — which is the whole failure mode here.
        """
        rendered, _ = _build_layout_capturing_output()
        assert "verification_mail_state" in rendered, (
            "page_layout does not mount the verification notice, so nothing renders it"
        )

    def test_the_dismiss_control_is_wired(self):
        rendered, _ = _build_layout_capturing_output()
        assert "dismiss_verification_notice" in rendered

    def test_building_the_shell_substitutes_no_icon(self):
        """Reflex swaps an unknown icon for ``circle_help`` and only warns on stderr.

        So "the layout builds" proves nothing about its icons — a typo would render a help
        bubble on every authenticated page with every test still green.
        """
        _, captured = _build_layout_capturing_output()
        assert "Invalid icon tag" not in captured, f"an icon was substituted: {captured}"


class TestNothingRendersForAMissingRelay:
    def test_the_shell_has_no_arm_for_no_relay(self):
        """A self-hoster with no SMTP relay has a normal deployment, not a broken one.

        Telling that operator "we couldn't send your confirmation email" would be a false
        alarm, so ``NO_RELAY`` is recorded in state and deliberately renders nothing. This
        asserts the absence, because a later "handle every case" tidy-up would add it back
        and nothing else would complain.
        """
        source = LAYOUT.read_text(encoding="utf-8")
        assert '"queued"' in source, "the shell stopped reading the queued outcome"
        assert '"failed"' in source, "the shell stopped reading the failed outcome"
        assert '"no_relay"' not in source, (
            "the shell renders something for no_relay; a deployment with no relay "
            "configured must not be told its mail failed"
        )


class TestTheThreeOutcomesAreDistinct:
    def test_the_enum_has_exactly_three_members(self):
        from datanika.services.email_verification import VerificationMailResult

        assert {m.value for m in VerificationMailResult} == {"queued", "no_relay", "failed"}

    def test_no_relay_and_failure_are_not_the_same_value(self):
        """The defect this replaced: one ``False`` for two different things.

        A missing relay and an unreachable broker both returned ``False``, so the caller
        could act on neither and acted on neither.
        """
        from datanika.services.email_verification import VerificationMailResult

        assert VerificationMailResult.NO_RELAY is not VerificationMailResult.FAILED

    def test_the_state_var_holds_the_enum_values(self):
        """A typo'd literal in the shell would render nothing, silently."""
        from datanika.services.email_verification import VerificationMailResult

        source = LAYOUT.read_text(encoding="utf-8")
        for member in (VerificationMailResult.QUEUED, VerificationMailResult.FAILED):
            assert f'"{member.value}"' in source, (
                f"the shell compares against no literal matching {member!r}"
            )

    def test_dismissing_clears_the_notice(self):
        from datanika.ui.state.auth_state import AuthState

        class _Stub:
            verification_mail_state = "queued"

        AuthState.dismiss_verification_notice.fn(_Stub)
        assert _Stub.verification_mail_state == ""


class TestTheCopyIsTranslated:
    @pytest.mark.parametrize(
        "key",
        [
            "auth.verification_mail_sent",
            "auth.verification_mail_failed",
            "auth.verification_mail_failed_help",
            "common.dismiss",
        ],
    )
    def test_every_locale_has_it(self, key):
        locales = sorted(p.stem for p in I18N.glob("*.json"))
        assert len(locales) == 9, f"expected 9 locales, found {locales}"
        for locale in locales:
            data = json.loads((I18N / f"{locale}.json").read_text(encoding="utf-8"))
            assert data.get(key, "").strip(), f"{locale}.json missing/blank: {key}"

    def test_the_failure_copy_says_the_account_still_works(self):
        """The user just signed up successfully. The banner must not read as a failed signup.

        A message that only says "we couldn't send your email" invites the reader to
        conclude their account did not get created and to try again — which then fails on
        the unique-email constraint.
        """
        en = json.loads((I18N / "en.json").read_text(encoding="utf-8"))
        help_text = en["auth.verification_mail_failed_help"].lower()
        assert "account is active" in help_text or "account is created" in help_text, (
            f"failure copy does not reassure the user their account exists: {help_text!r}"
        )

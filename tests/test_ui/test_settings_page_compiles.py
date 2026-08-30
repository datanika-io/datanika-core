"""The settings page must actually construct — nothing else in CI checks that.

Found while shipping core#658: `git grep -n "settings_page()" -- tests/`
returned **nothing**. Every UI test in this suite is AST-based
(`test_rbac_ui_visibility`, `test_upgrade_cta_target`), which is the right
shape for invariants and reads no component. So a whole class was uncovered:

* an invalid prop, or a Var that cannot resolve inside `rx.foreach`, raises at
  **app startup** — in production, on a deploy CI passed;
* an invalid icon tag degrades **silently**, printing to stdout where nothing
  is listening. That is [core#701]: `quota_callout.py` has asked for
  `arrow_up_circle` — not a lucide tag — for as long as it has existed, and
  Reflex has been quietly substituting `circle_help` on the one callout shown
  at the moment of highest purchase intent.

core#658 is the first change here to depend on a **per-row** Var: `member_row`
passes `member.assignable_roles` to `rx.select` inside an `rx.foreach`, where
the row is a Var proxy rather than a `MemberItem`. Constructing the page is the
only thing that can tell you a list attribute survives that.

Scoped to `settings.py` deliberately. A suite-wide "every page constructs"
guard is worth having and is filed as [core#701]; claiming it here without
enumerating the pages would be the discovery pass that silently finds nothing.
"""

import io
from contextlib import redirect_stdout

import pytest

from datanika.ui.pages import settings as settings_module

CARD_FACTORIES = [
    "account_card",
    "org_profile_card",
    "transfer_ownership_card",
    "members_card",
    "backup_restore_card",
    "api_keys_card",
    "notifications_card",
]


@pytest.mark.parametrize("name", CARD_FACTORIES)
def test_every_card_constructs(name):
    factory = getattr(settings_module, name, None)
    assert factory is not None, f"{name} is gone — update this list deliberately"
    assert factory() is not None


def test_the_whole_page_constructs():
    """The one that would have caught a bad `rx.foreach` row Var.

    `members_card` renders `rx.foreach(SettingsState.members, member_row)`, so
    this is also the only execution `member_row` ever gets.
    """
    assert settings_module.settings_page() is not None


def test_the_factory_list_is_not_silently_empty():
    """Guard the guard: a parametrize over an empty list passes as zero tests."""
    assert len(CARD_FACTORIES) >= 7


def test_the_settings_page_asks_for_no_invalid_icons():
    """Reflex substitutes `circle_help` for an unknown tag and warns on stdout.

    Nothing reads that stream, which is why the defect survived: the page looks
    fine to every test and wrong to every user. Capturing stdout turns the one
    signal this produces into an assertion.

    Red before the one-word fix in this PR — `quota_callout.py` asked for
    `arrow_up_circle`, which is not a lucide tag (it orders noun-first:
    `circle_arrow_up`), so the callout at the moment of highest purchase intent
    showed a **question mark** beside "Upgrade".
    """
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        settings_module.settings_page()
    assert "Invalid icon tag" not in buffer.getvalue(), buffer.getvalue()

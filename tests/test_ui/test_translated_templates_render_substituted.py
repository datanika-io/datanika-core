"""The substitution survives into the **rendered component tree**, not just into the source.

``tests/test_i18n/test_placeholders_are_substituted.py`` is an AST guard: it reads the source and
asks whether a substitution call sits in the key's scope. That is the right shape for a census over
fifteen keys, and it is **not** evidence that the substitution reaches the browser. This file is the
other instrument, on the two sites that are live today (#1540).

🚨 **The obvious assertion here is wrong in both directions, and I only found that by rendering.**
The tempting form is *"no ``{org}`` survives into the tree"*. Measured, the correct fix renders::

    translations?.["account.delete_org_too"].replaceAll("{org}", …account_state.sole_member_org…)

The brace **must** be there — it is the needle `replaceAll` searches for. So that assertion is red
on the correct fix and green if somebody deletes the callout outright. It is WORKFLOW_RULES §4's
*"assert the PRESENCE of the right thing, never the absence of the wrong word"* arriving through a
new door: not a corrected artifact quoting what it corrects, but a **mechanism that necessarily
contains its own input**.

So every assertion below is positive: the key's lookup is followed by a ``replaceAll`` for each of
that key's placeholders.
"""

from __future__ import annotations

import json
import pathlib
import re

import pytest

I18N = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "i18n"
PLACEHOLDER = re.compile(r"\{([a-z_]+)\}")


def _placeholders(key: str) -> set[str]:
    values = json.loads((I18N / "en.json").read_text(encoding="utf-8"))
    return set(PLACEHOLDER.findall(values[key]))


def _substituted(tree: str, key: str, placeholder: str) -> bool:
    """Is ``key``'s lookup followed by a ``replaceAll`` for ``placeholder``?

    Reflex compiles ``Var.replace`` to JS ``replaceAll`` and flattens a chain, so the calls for a
    multi-placeholder key appear in sequence after the one lookup. Anchoring to the lookup is what
    stops an unrelated ``replaceAll`` elsewhere in the tree from satisfying this.
    """
    start = tree.find(f'["{key}"]')
    if start == -1:
        return False
    # The chain ends at the next JSX element; 2 KB is far wider than any observed chain (the
    # longest real one, quota.allow_as_overage, is ~900 chars for three placeholders).
    return f'.replaceAll("{{{placeholder}}}"' in tree[start : start + 2000]


@pytest.fixture(scope="module")
def delete_dialog() -> str:
    from datanika.ui.pages import settings

    return str(settings._delete_account_dialog())


@pytest.fixture(scope="module")
def quota_modal() -> str:
    from datanika.ui.components.volume_quota_modal import volume_quota_modal
    from datanika.ui.state.dashboard_state import DashboardState

    return str(volume_quota_modal(DashboardState))


class TestTheInstrumentCanSee:
    """Coverage and sensitivity are different properties. Prove both before believing a pass."""

    def test_it_finds_a_substitution_that_is_there(self, delete_dialog):
        assert _substituted(delete_dialog, "account.delete_org_too", "org")

    def test_it_does_not_find_one_that_is_not(self, delete_dialog):
        """The unfixed shape — a bare lookup — must read as unsubstituted."""
        assert not _substituted(delete_dialog, "account.delete_org_too", "not_a_placeholder")
        assert not _substituted('x?.["account.delete_org_too"]', "account.delete_org_too", "org")

    def test_a_missing_key_is_not_silently_a_pass(self):
        assert not _substituted("anything at all", "account.delete_org_too", "org")

    def test_the_dialog_actually_rendered(self, delete_dialog):
        """A tree that failed to build would make every negative assertion above vacuous."""
        assert len(delete_dialog) > 2000
        assert "account.delete_confirm_heading" in delete_dialog


class TestTheAccountDeletionFlowNamesTheOrg:
    """#1540's AC1 — the two sites that are **live today**.

    This is the screen where someone decides whether to delete their account and everything in
    it, and it was naming their organisation as the literal text ``{org}``.
    """

    @pytest.mark.parametrize(
        ("key", "state_var"),
        [
            ("account.delete_org_too", "sole_member_org"),
            ("account.delete_last_owner", "blocking_org"),
        ],
    )
    def test_org_is_substituted_from_the_state_var_that_holds_it(
        self, delete_dialog, key, state_var
    ):
        for placeholder in _placeholders(key):
            assert _substituted(delete_dialog, key, placeholder), (
                f"{key} paints {{{placeholder}}} raw in the rendered tree"
            )
        # The substitution must draw on the var that actually holds the name — a `replaceAll`
        # wired to the wrong var renders the wrong org, which this file exists to notice.
        start = delete_dialog.find(f'["{key}"]')
        assert state_var in delete_dialog[start : start + 2000], (
            f"{key} substitutes from something other than AccountState.{state_var}"
        )


class TestTheVolumeQuotaModalPaintsNoTemplateItCannotFill:
    """#1540's AC2. The modal is gated today; a flag does not hold a feature still."""

    def test_the_overage_sentence_substitutes_every_pricing_figure(self, quota_modal):
        key = "quota.allow_as_overage"
        for placeholder in _placeholders(key):
            assert _substituted(quota_modal, key, placeholder)

    def test_the_overage_sentence_is_gated_on_the_figures_arriving(self, quota_modal):
        """A partial set would render a price we invented (SPEC_USAGE_VISIBILITY §2.5)."""
        assert "has_overage_figures" in quota_modal

    @pytest.mark.parametrize(
        "key", ["quota.volume_quota_reached_body", "quota.upgrade_to_next_tier"]
    )
    def test_the_unfillable_templates_are_not_rendered_at_all(self, quota_modal, key):
        """Their data has no producer. See RESERVED_UNUSED_KEYS for why each is reserved."""
        assert f'["{key}"]' not in quota_modal

    @pytest.mark.parametrize("key", ["quota.upgrade_hint", "quota.upgrade_button"])
    def test_the_placeholder_free_replacements_are_rendered(self, quota_modal, key):
        """Anti-vacuity for the test above: 'not rendered' must not be 'nothing is rendered'."""
        assert f'["{key}"]' in quota_modal

"""A dimension the plan row carries is a dimension the customer sees (core#1513).

Guard for `docs/specs/SPEC_USAGE_VISIBILITY.md` §2, core's half (§3.1). **Bytes are the billed
dimension and no screen in the product shows a byte count** — that is the defect. This file
pins core's contribution to the fix and, more importantly, pins the *shape* of it, because the
obvious fix is wrong in a way that looks right.

### 🚨 The two gates, and why the obvious fix mounts an empty component

All three issues ([core#1513], [cloud#180], [cloud#174]) name **one** gate — the
``datanika_dual_mode_ux_enabled`` feature flag on ``usage_bar()``. There are **two**, in two
repos:

1. this flag, in core, and
2. ``DashboardState.has_volume_data`` — ``bytes_limit > 0`` — which is ``False`` for every org
   in production because cloud's ``BillingService.fill_usage_summary`` assigns
   ``runs_used``/``runs_limit``/``plan_name`` and **never touches the byte fields**.

Product measured that against the running app with a **fabricated org id as the control**: a real
Free org and a non-existent one both returned ``bytes_limit = 0`` while the ``free`` plan row
carries 10 GiB. So removing the flag alone renders *nothing at all*. Everything here is therefore
necessary and **not sufficient** — the sufficient half is cloud's, and §6 of the spec says neither
half is useful alone.

### What each arm is for

* :class:`TestTheInstrumentSeesTheCard` is the anti-vacuity arm. A tree walk whose match is wrong
  finds nothing and every assertion below passes trivially. It runs first on purpose.
* :class:`TestTheVolumeDimensionIsNotGatedOnAUxFlag` asserts the flag is gone — **stated as
  "the rendered card is identical with the flag on and off"**, not as "the flag is absent from
  the source". An absence-of-the-wrong-word assertion is satisfied by deleting the component
  (`WORKFLOW_RULES` §4), and by *moving* the gate somewhere else, which is the likelier mistake.
* :class:`TestVolumeLeadsAndRunsFollow` pins §2.2's ordering, which reverses today's.
* :class:`TestNeitherDimensionGatesTheOther` pins §2.1 + §2.3: a plan carrying bytes but a
  ``NULL`` runs allowance must still show its volume meter. Today the whole card hangs off
  ``has_usage_data`` (``runs_limit > 0``), so such an org sees nothing.
* :class:`TestACapAndAnAllowanceDoNotLookTheSame` is AC5, and it **requires the two to differ**.
  Asserting that each renders "something" passes on a component that ignores the flag entirely —
  the [core#1492] shape, where a guard that answers both populations the same way looks cautious
  and discriminates nothing.
* :class:`TestTheDivisorIsBinary` is AC6, and it carries its own positive control: it asserts the
  ``1024**3`` reading **and** that the ``1000**3`` reading of the same byte count is a different
  string. A test that hardcodes ``"10 GB"`` for 10 GiB passes under either divisor.

### Why `str(component)` and not the source

The rendered JSX carries both branches of every ``rx.cond`` and the full state path of every
condition, so this reads what gets *painted* rather than what got typed. Same reasoning as
``test_buttons_are_legible.py``: a source-level reducer cannot see a computed value, and axe
cannot see a branch that is not currently mounted.

[core#1513]: https://github.com/datanika-io/datanika-core/issues/1513
[cloud#180]: https://github.com/datanika-io/datanika-cloud/issues/180
[cloud#174]: https://github.com/datanika-io/datanika-cloud/issues/174
"""

from __future__ import annotations

import json
import pathlib

import pytest

from datanika.config import settings
from datanika.ui.pages.dashboard import _volume_dimension, usage_bar
from datanika.ui.state.dashboard_state import DashboardState

GIB = 1024**3
GB_DECIMAL = 1000**3

#: The keys core already ships in all nine locales (spec §3.1 — core needs no new strings).
VOLUME_TITLE = "quota.volume_title"
VOLUME_USAGE = "quota.volume_usage"
VOLUME_OVERAGE = "quota.volume_overage"
VOLUME_WALL = "quota.volume_quota_reached_title"
RUNS_LABEL = "dashboard.usage_runs"

I18N_DIR = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "i18n"
LOCALES = ("en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr")


class _Usage:
    """A stand-in for DashboardState when exercising its computed vars via ``fget``."""

    def __init__(
        self,
        *,
        bytes_used: int = 0,
        bytes_limit: int = 0,
        runs_used: int = 0,
        runs_limit: int = 0,
    ) -> None:
        self.bytes_used = bytes_used
        self.bytes_limit = bytes_limit
        self.runs_used = runs_used
        self.runs_limit = runs_limit


class _Overage:
    """A stand-in for DashboardState's three pre-formatted overage figures."""

    def __init__(self, *, gb: str = "", rate: str = "", total: str = "") -> None:
        self.bytes_overage_gb = gb
        self.bytes_overage_rate = rate
        self.bytes_overage_total = total


def _walk(node):
    """Every node in a built component tree, ``rx.cond`` branches included."""
    yield node
    for kid in getattr(node, "children", None) or []:
        yield from _walk(kid)


def _conds(node):
    """Every ``Cond`` node, paired with the string form of its condition."""
    for n in _walk(node):
        if type(n).__name__ == "Cond":
            yield n, str(n.cond)


def _cond_naming(node, *, includes: tuple[str, ...], excludes: tuple[str, ...] = ()):
    """The single ``Cond`` whose condition mentions every name in ``includes`` and none in
    ``excludes``.

    Selection is by the *full shape* of the condition rather than by one substring, because
    several conditions legitimately mention the same var: ``has_usage_data`` appears both in the
    runs row's own gate and inside anything combining the two dimensions. A looser matcher would
    silently grade whichever it found first.

    Raises rather than returning ``None``: a reducer that returns nothing for a shape it cannot
    find hands that shape to its caller as clean, which is how seven composite colours stayed
    invisible behind a green guard in core#1409.
    """
    hits = [
        c
        for c, expr in _conds(node)
        if all(i in expr for i in includes) and not any(x in expr for x in excludes)
    ]
    if not hits:
        raise AssertionError(
            f"no rx.cond in this tree tests {includes!r} (excluding {excludes!r}). "
            "Conditions present: "
            + "; ".join(sorted({e.rsplit(".", 1)[-1][:60] for _, e in _conds(node)}))
        )
    if len(hits) > 1:
        raise AssertionError(f"{len(hits)} conds match {includes!r}; expected exactly one")
    return hits[0]


def _branches(cond_node) -> tuple[str, str]:
    """(true-branch, false-branch) of a ``Cond``, as rendered JSX."""
    kids = list(cond_node.children or [])
    if len(kids) != 2:
        raise AssertionError(f"Cond has {len(kids)} branches, expected 2")
    return str(kids[0]), str(kids[1])


class TestTheInstrumentSeesTheCard:
    """Anti-vacuity. If this class fails, every verdict below is meaningless."""

    def test_the_rendered_card_carries_i18n_keys_verbatim(self):
        painted = str(usage_bar())
        assert RUNS_LABEL in painted, (
            "the runs label is missing from the rendered card, so this file's instrument is not "
            "reading the component tree and every assertion below passes for the wrong reason"
        )

    def test_the_rendered_card_carries_state_paths_verbatim(self):
        painted = str(usage_bar())
        assert "has_usage_data" in painted

    def test_the_two_wall_strings_are_actually_different(self):
        """Positive control for AC5: comparing two identical strings proves nothing."""
        with open(I18N_DIR / "en.json", encoding="utf-8") as fh:
            en = json.load(fh)
        assert en[VOLUME_WALL] != en[VOLUME_OVERAGE]

    @pytest.mark.parametrize("locale", LOCALES)
    def test_core_needs_no_new_strings_for_this(self, locale):
        """Spec §3.1: the keys this change renders already exist in all nine locales."""
        with open(I18N_DIR / f"{locale}.json", encoding="utf-8") as fh:
            data = json.load(fh)
        for key in (VOLUME_TITLE, VOLUME_USAGE, VOLUME_OVERAGE, VOLUME_WALL):
            assert key in data and data[key], f"{key} missing or empty in {locale}.json"


class TestTheVolumeDimensionIsNotGatedOnAUxFlag:
    """Spec §2.8: the volume dimension stops being gated on the ETL/ELT flag.

    ⚠️ The flag itself is **not** flipped and must not be — it also mounts a mode selector that
    persists nothing (landing#656). What changes is that the meter no longer asks it.
    """

    def test_the_meter_is_mounted_with_the_flag_off(self):
        assert settings.datanika_dual_mode_ux_enabled is False, (
            "this test asserts production's configuration; the flag is not False in this process"
        )
        assert VOLUME_TITLE in str(usage_bar())

    def test_the_card_is_byte_identical_with_the_flag_on_and_off(self, monkeypatch):
        """The strong form. 'The flag is absent from the source' is satisfied by deleting the
        component and by moving the gate elsewhere; this is satisfied by neither."""
        monkeypatch.setattr(settings, "datanika_dual_mode_ux_enabled", False)
        off = str(usage_bar())
        monkeypatch.setattr(settings, "datanika_dual_mode_ux_enabled", True)
        on = str(usage_bar())
        assert off == on, "the usage card still reads datanika_dual_mode_ux_enabled"
        assert VOLUME_TITLE in off


class TestVolumeLeadsAndRunsFollow:
    """Spec §2.2. Bytes are what we charge for; runs are a fair-use line."""

    def test_volume_is_painted_before_runs(self):
        painted = str(usage_bar())
        assert VOLUME_TITLE in painted and RUNS_LABEL in painted
        assert painted.index(VOLUME_TITLE) < painted.index(RUNS_LABEL)


class TestNeitherDimensionGatesTheOther:
    """Spec §2.1 + §2.3.

    A plan row with a volume allowance and a ``NULL`` runs allowance must still show its volume
    meter — and vice versa. Today the *whole card* hangs off ``has_usage_data``
    (``runs_limit > 0``), so the first org is shown nothing at all.
    """

    def test_a_volume_only_plan_still_shows_the_card(self):
        """The behavioural form. A plan row with 10 GiB and a ``NULL`` runs allowance.

        Asserted through the var the card is actually gated on, not through a substring of the
        rendered condition — a name can be present and mean nothing.
        """
        volume_only = _Usage(bytes_limit=10 * GIB, runs_limit=0)
        assert DashboardState.has_any_usage_data.fget(volume_only) is True

    def test_a_runs_only_plan_still_shows_the_card(self):
        runs_only = _Usage(bytes_limit=0, runs_limit=500)
        assert DashboardState.has_any_usage_data.fget(runs_only) is True

    def test_a_plan_with_neither_dimension_shows_no_card(self):
        assert DashboardState.has_any_usage_data.fget(_Usage()) is False

    @pytest.mark.parametrize("runs_limit", [0, 500])
    @pytest.mark.parametrize("bytes_limit", [0, 10 * GIB])
    def test_the_card_gate_never_drifts_from_the_two_dimension_gates(self, runs_limit, bytes_limit):
        """``has_any_usage_data`` reads the raw fields, so it could drift from the two vars it
        stands for. This pins the whole 2x2."""
        st = _Usage(runs_limit=runs_limit, bytes_limit=bytes_limit)
        expected = DashboardState.has_usage_data.fget(st) or DashboardState.has_volume_data.fget(st)
        assert DashboardState.has_any_usage_data.fget(st) is expected
        both = DashboardState.has_usage_data.fget(st) and DashboardState.has_volume_data.fget(st)
        assert DashboardState.shows_both_dimensions.fget(st) is both

    def test_the_runs_row_disappears_on_its_own_when_there_is_no_runs_allowance(self):
        """§2.3: a NULL allowance renders nothing, never ``0 / 0``."""
        node = _cond_naming(
            usage_bar(), includes=("has_usage_data",), excludes=("has_any_usage_data",)
        )
        _, absent = _branches(node)
        assert RUNS_LABEL not in absent

    def test_no_volume_allowance_renders_nothing_rather_than_a_zero_meter(self):
        assert DashboardState.has_volume_data.fget(_Usage(bytes_limit=0)) is False
        _, absent = _branches(_cond_naming(_volume_dimension(), includes=("has_volume_data",)))
        assert VOLUME_TITLE not in absent
        assert "0" not in absent.replace("jsx(Fragment,{},)", "")


class TestACapAndAnAllowanceDoNotLookTheSame:
    """AC5 / spec §2.4.

    ``hard_cap_bytes = true`` means *runs stop*; ``false`` means *overage bills* (core#713). A
    full bar means "blocked" on Free and "now billing" on Pro, and a customer cannot be shown the
    same thing for both. Free is the **only** hard-capped tier and the only one with no meter.
    """

    def test_the_state_carries_the_cap_flag(self):
        assert "bytes_hard_cap" in DashboardState.__annotations__, (
            "the dashboard cannot tell a wall from an overage without knowing which it is"
        )

    def test_the_wall_and_the_overage_are_different_renderings(self):
        true_branch, false_branch = _branches(
            _cond_naming(_volume_dimension(), includes=("bytes_hard_cap",))
        )
        assert true_branch != false_branch, (
            "capped and uncapped render identically — a guard that answers both populations the "
            "same way discriminates nothing (core#1492)"
        )
        assert VOLUME_WALL in true_branch and VOLUME_OVERAGE not in true_branch
        assert VOLUME_OVERAGE in false_branch and VOLUME_WALL not in false_branch

    def test_a_hard_capped_plan_is_never_told_it_will_be_billed_for_overage(self):
        """Free is hard-capped at 10 GiB and `overage_run_price_cents` is 0 on every plan, so
        "Overage: N GB x $rate" is false in both halves for it: nothing is billed AND the run is
        blocked. That is the sentence this branch exists to stop rendering."""
        true_branch, _ = _branches(_cond_naming(_volume_dimension(), includes=("bytes_hard_cap",)))
        assert VOLUME_OVERAGE not in true_branch


class TestNoTemplateKeyIsPaintedWithItsBracesShowing:
    """🚨 i18n substitution here is **per call site**, so ``_t["some.key"]`` paints the raw value.

    Fifteen ``en.json`` values carry ``{placeholder}`` spans. Two of them are on this meter,
    which has never rendered in production because the UX flag has been off since V2 P1 —
    un-gating it without this would put ``{used} / {limit} GB processed this month`` on every
    customer's dashboard.

    🔴 **A correction worth keeping, because the first measurement of this was wrong.** The
    initial census asked *"does this FILE contain ``.replace(``"* and reported **eight**
    unsubstituted keys. Core has a **second** substitution mechanism that heuristic could not
    see — ``ui/components/i18n_text.py::interpolate``, which weaves components into a template
    and is used on the signup sentence. Re-measured per call site against **both** mechanisms,
    the real figure is **five** unsubstituted sites: three in ``volume_quota_modal`` (latent —
    gated by the same UX flag at ``pipelines.py:269-272``) and **two live ones on the account
    deletion flow** in ``settings.py``, which render ``{org}`` to real users today. Tracked
    separately; they are not this card.
    🔑 *An instrument that knows about one mechanism reports the other as absent* — and the
    false positive here was in the flattering direction, making the defect look larger and
    this fix look more complete than it was.

    🔑 **Re-wording the nine values to drop the braces is NOT an equivalent fix**, which is why
    this asserts substitution rather than absence: ru, zh and ar place the numbers *mid-phrase*
    (``"本月已处理 {used} / {limit} GB"``), so a component hardcoding ``"<used> / <limit>
    <sentence>"`` is wrong in those locales with or without braces. The templates are correct;
    the painting was not.
    """

    @staticmethod
    def _placeholders(value: str) -> set[str]:
        import re

        return set(re.findall(r"\{[a-z_]+\}", value))

    def test_the_probe_finds_placeholders_at_all(self):
        """Anti-vacuity: a regex that matches nothing makes every case below pass."""
        with open(I18N_DIR / "en.json", encoding="utf-8") as fh:
            en = json.load(fh)
        assert self._placeholders(en[VOLUME_USAGE]) == {"{used}", "{limit}"}
        assert self._placeholders(en[VOLUME_OVERAGE]) == {"{gb}", "{rate}", "{total}"}
        assert self._placeholders(en[VOLUME_TITLE]) == set()

    @pytest.mark.parametrize("key", [VOLUME_USAGE, VOLUME_OVERAGE])
    def test_every_placeholder_of_every_painted_template_is_substituted(self, key):
        with open(I18N_DIR / "en.json", encoding="utf-8") as fh:
            en = json.load(fh)
        painted = str(usage_bar())
        assert f'["{key}"]' in painted, f"{key} is not painted by this card at all"
        for ph in sorted(self._placeholders(en[key])):
            assert f'replaceAll("{ph}"' in painted, (
                f"{key} is painted but {ph} is never substituted — that brace reaches the DOM"
            )

    @pytest.mark.parametrize("locale", LOCALES)
    def test_every_locale_uses_the_same_placeholder_set(self, locale):
        """A translator who dropped or renamed one would leave an unsubstituted brace in that
        locale only — invisible to an English-only check."""
        with open(I18N_DIR / "en.json", encoding="utf-8") as fh:
            en = json.load(fh)
        with open(I18N_DIR / f"{locale}.json", encoding="utf-8") as fh:
            other = json.load(fh)
        for key in (VOLUME_USAGE, VOLUME_OVERAGE):
            assert self._placeholders(other[key]) == self._placeholders(en[key]), (
                f"{locale}.json's {key} names different placeholders from en.json"
            )

    def test_the_substituted_values_carry_no_unit_of_their_own(self):
        """``quota.volume_usage`` already says "GB". Feeding it the unit-bearing display var
        yields "10 GB GB processed this month" — the bug the unit-free vars exist to avoid."""
        st = _Usage(bytes_used=5 * GIB, bytes_limit=10 * GIB)
        assert DashboardState.bytes_used_gb.fget(st) == "5.0"
        assert DashboardState.bytes_limit_gb.fget(st) == "10"
        assert "GB" in DashboardState.bytes_limit_display.fget(st)
        painted = str(usage_bar())
        assert "bytes_limit_gb" in painted, (
            "the meter substitutes the unit-BEARING var into a template that already says GB"
        )

    def test_the_unit_free_vars_never_drift_from_the_unit_bearing_ones(self):
        for raw in (0, 1, 5 * GIB, 10 * GIB, 137 * GIB):
            st = _Usage(bytes_used=raw, bytes_limit=raw)
            assert DashboardState.bytes_used_display.fget(st) == (
                DashboardState.bytes_used_gb.fget(st) + " GB"
            )
            assert DashboardState.bytes_limit_display.fget(st) == (
                DashboardState.bytes_limit_gb.fget(st) + " GB"
            )


class TestTheOverageSentenceIsAllOrNothing:
    """A figure the biller did not supply must not be invented.

    The template names three. Painting it with any of them missing gives either a visible
    ``{gb}`` or a confident ``$0.00`` — and ``$0.00`` is the worse of the two, because it says
    *free* on the one line whose job is to say what something costs. Core deliberately does not
    compute them: the biller bills a **started** GB (``-(-q // 1024**3)``), so a division here
    would disagree with the invoice. ``SPEC_USAGE_VISIBILITY`` §2.5.
    """

    def test_no_figures_means_no_line(self):
        assert DashboardState.has_overage_figures.fget(_Overage()) is False

    @pytest.mark.parametrize(
        "gb,rate,total",
        [("3", "0.50", ""), ("3", "", "1.50"), ("", "0.50", "1.50"), ("", "", "")],
    )
    def test_a_partial_set_is_not_enough(self, gb, rate, total):
        assert (
            DashboardState.has_overage_figures.fget(_Overage(gb=gb, rate=rate, total=total))
            is False
        )

    def test_a_complete_set_draws_the_line(self):
        assert (
            DashboardState.has_overage_figures.fget(_Overage(gb="3", rate="0.50", total="1.50"))
            is True
        )

    def test_the_line_is_behind_that_gate_in_the_tree(self):
        node = _cond_naming(_volume_dimension(), includes=("has_overage_figures",))
        drawn, not_drawn = _branches(node)
        assert VOLUME_OVERAGE in drawn
        assert VOLUME_OVERAGE not in not_drawn


class TestTheDivisorIsBinary:
    """AC6. ``billing/tasks.py`` converts with ``1024**3`` and ``/pricing`` publishes binary GB,
    so a surface picking ``1000**3`` disagrees with the invoice."""

    def test_ten_gibibytes_render_as_ten_gb(self):
        assert DashboardState.bytes_limit_display.fget(_Usage(bytes_limit=10 * GIB)) == "10 GB"

    def test_the_test_can_tell_the_two_divisors_apart(self):
        """Positive control. Without this, ``"10 GB"`` passes under either divisor for some
        inputs and the assertion above is decoration."""
        raw = 10 * GIB
        binary = f"{raw / GIB:.0f} GB"
        decimal = f"{raw / GB_DECIMAL:.0f} GB"
        assert binary != decimal, "chosen fixture cannot distinguish the divisors"
        assert DashboardState.bytes_limit_display.fget(_Usage(bytes_limit=raw)) == binary

    def test_used_display_uses_the_same_divisor(self):
        used = 5 * GIB
        assert DashboardState.bytes_used_display.fget(_Usage(bytes_used=used)) == "5.0 GB"
        assert f"{used / GB_DECIMAL:.1f} GB" != "5.0 GB"


class TestTheUsageContextOffersEveryFieldTheCardReads:
    """The hook contract. Core creates the dict; cloud fills it (spec §3.2).

    A field the card renders but the context never offers is a field cloud has no way to supply —
    which is [core#1513]'s defect one level down.
    """

    def test_the_context_names_every_field_the_state_stores(self):
        from datanika.ui.state.dashboard_state import usage_context

        ctx = usage_context(org_id=1)
        for field in ("runs_used", "runs_limit", "plan_name", "bytes_used", "bytes_limit"):
            assert field in ctx, f"{field} is not offered to the cloud handler"
        assert "bytes_hard_cap" in ctx, (
            "cloud has no way to tell the dashboard whether the volume allowance is a wall"
        )
        for figure in ("bytes_overage_gb", "bytes_overage_rate", "bytes_overage_total"):
            assert figure in ctx, (
                f"{figure} is not offered, so the overage sentence can never be completed and "
                "the line is silently never drawn"
            )

    def test_the_context_defaults_are_the_no_cloud_plugin_reading(self):
        from datanika.ui.state.dashboard_state import usage_context

        ctx = usage_context(org_id=7)
        assert ctx["org_id"] == 7
        assert ctx["bytes_limit"] == 0
        assert ctx["bytes_hard_cap"] is False

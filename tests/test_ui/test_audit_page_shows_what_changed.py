"""core#694 — the audit page must show *what changed*, not the empty column.

30 call sites write `old_values` / `new_values`; until this shipped, **nothing
read them**. The page rendered `ip_address` — the one column that has never held
a value in any production row (core#670) — beside two columns that hold every
value and appeared nowhere. So the log could say *that* something happened and
never *what*, while the data to answer it had been collected all along.

**The security gate this shipped behind.** core#694's own body refuses to be
built before `SPEC_PII_SEPARATION` D11/D12, because five call sites put a third
party's email in the payload and a reader would turn dormant storage into live
exposure. Verified before writing this: `AuditService.log_action` redacts both
payloads at the chokepoint (`audit_service.py:174-175`), and the derived key set
really does redact — measured by calling it, not by reading it.

🚨 **The renderer redacts a SECOND time, at read.** That is defence in depth for
rows a future path might write around the service. ⚠️ It does **not** reduce
D12.2's guard: §2c deleted the backfill and states that guard is now the *only*
witness to a redactor regression. This makes the **page** safe, never the
**table** — and the table is what an export or an MCP tool would reach.

**The subtle rule, and the reason it is not "skip unchanged fields".** Redaction
happens at *write*, so a PII field holds the marker on both sides whether or not
it changed: the information is destroyed before this code runs. Skipping on
equality would turn *"the email changed"* into silence, and D12.3 says the log
records **that** a value changed and never what it was. Rendering it as *changed*
would be the opposite lie for a snapshot that merely included the field. So it
renders `email: (redacted) → (redacted)` — the field is in the record, and
neither side is ours to show.
"""

import ast
import json
import pathlib

import datanika.ui
from datanika.services.audit_service import REDACTED
from datanika.ui.state.audit_state import AuditLogItem, format_changes

UI = pathlib.Path(datanika.ui.__file__).parent
I18N_DIR = UI.parent / "i18n"
LOCALES = ("en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr")

R = "(redacted)"
N = "(not set)"


def _locale(name: str) -> dict:
    return json.loads((I18N_DIR / f"{name}.json").read_text(encoding="utf-8"))


# --------------------------------------------------------------- what changed


def test_an_update_names_the_field_and_both_sides():
    assert format_changes({"role": "viewer"}, {"role": "editor"}, R, N) == "role: viewer → editor"


def test_an_unchanged_non_pii_field_is_left_out():
    """A snapshot of ten fields where one moved should show the one."""
    out = format_changes({"role": "a", "seats": 1}, {"role": "b", "seats": 1}, R, N)
    assert out == "role: a → b", f"unchanged fields leaked into the summary: {out!r}"


def test_a_create_and_a_delete_render_without_an_arrow():
    """An arrow with nothing on one side reads as data loss, not as creation."""
    created = format_changes(None, {"invitation_id": 5, "role": "editor"}, R, N)
    assert created == "invitation_id: 5; role: editor"
    assert "→" not in created

    deleted = format_changes({"membership_id": 9}, None, R, N)
    assert deleted == "membership_id: 9"
    assert "→" not in deleted


def test_a_key_present_on_only_one_side_says_so():
    """`(not set)` distinguishes *absent* from *present and null*."""
    assert format_changes({"role": "a"}, {"role": "a", "seats": 3}, R, N) == "seats: (not set) → 3"


def test_nothing_to_say_is_the_empty_string_not_a_crash():
    assert format_changes(None, None, R, N) == ""
    assert format_changes({}, {}, R, N) == ""


# ------------------------------------------------------------------- the rule


def test_a_redacted_field_is_reported_even_though_both_sides_match():
    """The spec-grounded rule, and the one a later "skip equal values" would break.

    Redaction happens at write, so both sides hold the marker whether or not the
    value changed. Skipping on equality turns "the email changed" into silence —
    and D12.3 requires the log to record *that* a value changed.
    """
    out = format_changes({"email": "a@x.com"}, {"email": "b@x.com"}, R, N)
    assert out == "email: (redacted) → (redacted)", (
        "a PII field that changed vanished from the summary entirely"
    )


def test_the_redaction_marker_never_reaches_the_screen_raw():
    """`__REDACTED__` is an implementation detail; the user gets a sentence."""
    out = format_changes({"email": "a@x.com"}, {"email": "b@x.com"}, R, N)
    assert REDACTED not in out, "the raw marker was rendered to the user"
    assert R in out


# ------------------------------------------------------------------ the leak


def test_personal_values_never_reach_the_summary_even_unredacted_on_the_row():
    """Defence in depth: a row the service did not redact must still be safe.

    ⚠️ This is NOT a substitute for D12.2's write-side guard — see the module
    docstring. It closes the page, not the table.
    """
    out = format_changes(
        {"email": "victim@example.com", "full_name": "Victim Name", "role": "viewer"},
        {"email": "new@example.com", "full_name": "New Name", "role": "editor"},
        R,
        N,
    )
    for secret in ("victim@example.com", "new@example.com", "Victim Name", "New Name"):
        assert secret not in out, f"{secret!r} reached the audit summary"
    assert "role: viewer → editor" in out, "redaction ate the non-PII change too"


# ------------------------------------------------------------------- the page


def test_the_page_renders_the_change_and_no_longer_the_empty_column():
    """⚠️ Asserted on the AST, not on the text.

    The first version of this test read `"log.ip_address" not in src` and failed
    on the **comment** that explains why the column was removed. A source grep
    cannot tell a reference from a mention, and prose about a removal is
    indistinguishable from the removal not having happened.
    """
    src = (UI / "pages" / "audit_logs.py").read_text(encoding="utf-8")
    assert "log.changes" in src, "the page still cannot show what changed"
    assert '_t["audit.changes"]' in src, "no header for the change column"

    attrs = {n.attr for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Attribute)}
    assert "changes" in attrs, "the extractor sees no attributes — it proves nothing"
    assert "ip_address" not in attrs, (
        "the page still reads ip_address, which has never held a value in any row"
    )


def test_the_row_model_no_longer_carries_ip_address():
    """A field nothing renders is state that outlives its reason."""
    assert not hasattr(AuditLogItem(), "ip_address")
    assert hasattr(AuditLogItem(), "changes")


def test_the_long_summary_is_truncated_and_the_full_text_is_kept():
    """The cell stays a cell; the tooltip carries the rest."""
    src = (UI / "state" / "audit_state.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    fn = next(
        (
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "_change_fields"
        ),
        None,
    )
    assert fn is not None, "_change_fields is gone — this test proves nothing"

    page = (UI / "pages" / "audit_logs.py").read_text(encoding="utf-8")
    assert "log.truncated" in page and "log.changes_full" in page, (
        "nothing renders the untruncated text, so a long change is silently cut"
    )


# ------------------------------------------------------------------- the i18n


def test_the_three_new_keys_resolve_in_every_locale():
    en = _locale("en")
    for locale in LOCALES:
        data = _locale(locale)
        for key in ("audit.changes", "audit.redacted", "audit.not_set"):
            assert key in data, f"{locale}.json is missing {key}"
            assert data[key].strip(), f"{locale}.json has an empty {key}"
            if locale != "en":
                assert data[key] != en[key], f"{locale}.json's {key} is the English verbatim"


def test_the_ip_address_translations_went_with_the_column():
    """The deletion is deliberate, and this records why — including the reversal.

    🔑 **My first instinct was to keep them** and the suite refuted it.
    `TestCodeJsonSync::test_no_orphan_keys_in_json` has no allowlist and it is
    right: with the column gone the translation is dead, and an allowlist is how a
    gate gets turned off one line at a time.

    ⚠️ **This is not the core#872 hazard.** That one is about an orphan sweep
    dropping translations *silently*. This is argued, recorded on core#670, and
    recoverable with one `git show` — the nine strings are "IP Address" and its
    obvious translations, not prose anyone must re-derive.

    **For whoever wires `client_ip.py` (core#670): re-add these keys with the
    column.** A populated `ip_address` with no header is the same defect this
    change removed, pointing the other way.
    """
    for locale in LOCALES:
        assert "audit.ip_address" not in _locale(locale), (
            f"{locale}.json still carries audit.ip_address, which nothing renders — "
            "test_no_orphan_keys_in_json will fail on it"
        )

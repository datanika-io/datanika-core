"""No icon tag in the UI is silently substituted for a different glyph (#701).

Reflex does not raise on an unknown lucide tag. It prints

    Warning: Invalid icon tag: arrow_up_circle. ... Using 'circle_help' icon instead.

to **stdout**, and renders a question mark. That warning is the only signal
this defect has ever produced, and in production nothing is listening — which
is how `quota_callout.py` shipped a help icon beside "Upgrade", on the one
callout shown at the moment of highest purchase intent, for as long as it had
existed.

Two things this guard does deliberately:

* **It probes the validator, not a hardcoded list of valid names.** A list would
  need updating on every lucide bump and would go wrong in the silent
  direction — a name we failed to add reads as a defect, a name lucide removed
  reads as fine.
* **The extractor asserts a floor.** A sweep that finds nothing satisfies the
  real assertion in silence, which is the failure mode a source-derived guard is
  most likely to have.

⚠️ **Hyphens are not the rule, and assuming they are sends you the wrong way.**
`triangle-alert` and `triangle_alert` are *both* accepted; `help-circle` is
rejected while `circle_help` is accepted — because lucide **renamed** the icon,
not because of the separator. The rule is "the tag is a current lucide name".

⚠️ **`redirect_stdout` is load-bearing and fragile in one specific way**: if
Reflex ever switches to `warnings.warn` or a logger, this test goes green
forever with no code change. Pair it with the floor above and re-run the
negative control (below) whenever Reflex is bumped.

**Negative control:** add `rx.icon("definitely-not-an-icon")` to any UI file and
confirm `test_no_icon_tag_is_silently_substituted` goes red naming that file and
line. A guard for silent substitution that has never been watched substituting
is the same defect it exists to catch.
"""

import ast
import io
import pathlib
import re
from contextlib import redirect_stdout

import reflex as rx

import datanika.ui

UI = pathlib.Path(datanika.ui.__file__).parent


def _icon_tags() -> dict[str, list[str]]:
    """tag -> ["file:line", ...] for `rx.icon("tag")` and `icon="tag"` anywhere in the UI."""
    tags: dict[str, list[str]] = {}
    for path in sorted(UI.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            if (
                isinstance(f, ast.Attribute)
                and f.attr == "icon"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                tags.setdefault(node.args[0].value, []).append(
                    f"{path.relative_to(UI).as_posix()}:{node.lineno}"
                )
            for kw in node.keywords:
                if (
                    kw.arg == "icon"
                    and isinstance(kw.value, ast.Constant)
                    and isinstance(kw.value.value, str)
                ):
                    tags.setdefault(kw.value.value, []).append(
                        f"{path.relative_to(UI).as_posix()}:{node.lineno}"
                    )
    return tags


def test_the_extractor_is_armed():
    """A sweep that finds nothing passes the real assertion below in silence."""
    tags = _icon_tags()
    assert len(tags) >= 40, (
        f"only {len(tags)} distinct icon tags found under {UI}; the extractor "
        "has stopped matching and the assertion below is now vacuous"
    )


def test_no_icon_tag_is_silently_substituted():
    offenders = []
    for tag, sites in sorted(_icon_tags().items()):
        buf = io.StringIO()
        with redirect_stdout(buf):
            rx.icon(tag)
        out = buf.getvalue()
        if "Invalid icon tag" in out:
            m = re.search(r"Using '([a-z_]+)' icon instead", out)
            offenders.append(f"{tag!r} -> rendered as {m.group(1) if m else '?'} at {sites}")
    assert not offenders, (
        "these tags are not current lucide names; Reflex substitutes a different "
        "icon and only says so on stdout, where nothing is listening in "
        "production:\n  " + "\n  ".join(offenders)
    )


def test_the_probe_can_still_detect_a_bad_tag():
    """The control, run in-process on every CI run rather than left as a comment.

    This is what fails if Reflex stops warning on stdout — the one change that
    would make the assertion above green forever while the defect it exists to
    catch went unreported.
    """
    buf = io.StringIO()
    with redirect_stdout(buf):
        rx.icon("definitely-not-a-lucide-icon")
    assert "Invalid icon tag" in buf.getvalue(), (
        "Reflex no longer warns on stdout for an unknown icon tag, so "
        "test_no_icon_tag_is_silently_substituted can no longer fail. Find where "
        "the warning went (warnings.warn? a logger?) and re-point both tests."
    )

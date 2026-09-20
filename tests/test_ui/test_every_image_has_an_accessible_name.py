"""Every ``rx.image`` in the app carries an ``alt`` (core#1409, the ``image-alt`` class).

axe's ``image-alt`` is a **critical** rule and it fired one node on every page swept, because the
logo is rendered by shared components that every page reaches. A screen reader announces an image
with no accessible name by its file name, or skips it.

**Derived, not listed.** The check reads the source tree for every ``rx.image(`` call site rather
than pinning the seven that exist today, so an eighth added tomorrow fails here rather than in a
sweep somebody has to run (``WORKFLOW_RULES`` §5a). Pinning today's count would be a snapshot
wearing a test's clothes.

⚠️ **This closes one of the six violation classes on core#1409, not the issue.** ``button-name``,
``aria-allowed-attr`` and ``label`` are also critical, and ``color-contrast`` and
``link-in-text-block`` are serious — all of them still count toward the sweep's verdict, so the
graduation this unblocks is partial.

**Why no translation key.** The accessible name of a brand logo is the product name, which is a
proper noun — the same reading as the i18n rule's *skip technical identifiers and OAuth provider
names*. Every other user-visible string in these files does carry a key.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

UI_ROOT = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "ui"


def _image_calls() -> list[tuple[str, int, ast.Call]]:
    """Every ``rx.image(...)`` call in the UI tree, as (file, line, node)."""
    found: list[tuple[str, int, ast.Call]] = []
    for path in sorted(UI_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "image"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "rx"
            ):
                found.append((str(path.relative_to(UI_ROOT.parents[1])), node.lineno, node))
    return found


def test_the_scan_finds_the_call_sites_at_all():
    """Anti-vacuity: a scan that matches nothing makes the assertion below trivially true."""
    calls = _image_calls()
    assert len(calls) >= 7, f"only {len(calls)} rx.image call sites found — the AST match is wrong"


def test_every_image_carries_an_alt():
    bare = [
        f"{where}:{line}"
        for where, line, node in _image_calls()
        if not any(kw.arg == "alt" for kw in node.keywords)
    ]
    assert bare == [], (
        f"rx.image with no alt: {bare}. axe's image-alt is critical, and a screen reader "
        "announces such an image by its file name or skips it. Pass alt= (the product name for "
        'the logo), or alt="" with a role that marks it decorative.'
    )


@pytest.mark.parametrize("where_line_node", _image_calls(), ids=lambda c: f"{c[0]}:{c[1]}")
def test_no_alt_is_empty(where_line_node):
    """An empty ``alt`` is how an image is marked decorative, and the logo is not decorative.

    Asserted separately from the presence check because ``alt=""`` satisfies that one while
    telling a screen-reader user nothing about a control they can click.
    """
    _where, _line, node = where_line_node
    alt = next((kw for kw in node.keywords if kw.arg == "alt"), None)
    assert alt is not None
    if isinstance(alt.value, ast.Constant):
        assert alt.value.value, "alt is empty — that marks the image decorative"

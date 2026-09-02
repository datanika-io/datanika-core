"""Every ``rx.input`` in the product must have an accessible name (core#720).

**This rule has one confirmed production instance, and it was found by accident.**
Three E2E specs failed deterministically for months with
``locator.fill: Test timeout 60000ms exceeded``. The cause was not a flake and not a
selector style: ``page.getByLabel(/email/i)`` matches a ``<label>``, an ``aria-label``
or an ``aria-labelledby``, and the Reflex auth forms rendered ``rx.text("Email")`` as a
**sibling ``<p>``** before an unlabelled ``rx.input``. The inputs had no accessible name
at all — a screen reader announced nothing. Fixed for the auth forms in ``8a5c90d``
(``rx.el.label(..., html_for="login-email")``), and found only because a *test harness*
tripped over it.

That pattern is still live elsewhere. ``datanika/ui/pages/settings.py`` currently reads::

    rx.text(_t["auth.new_password"], size="2", weight="medium"),
    rx.input(name="password", type="password", ...),

which is the identical shape, on the password-change form.

**Why a ratchet and not a flat rule.** Measured on ``origin/dev d2ac870``: **8 inputs
named, 67 not**, across 13 files. The 8 are exactly the eight ``html_for`` values from
``8a5c90d``; every input outside the five auth pages is unnamed. A flat "all inputs must
be named" is therefore red on arrival in 13 files, and a gate that must be loosened on
its first day teaches everyone to loosen gates (core#720 says this in its own scope
section). So the baseline below records what is already unnamed, and the assertion is
that it does not **grow** — and that when it shrinks, it shrinks visibly in a diff.

⚠️ **What this guard cannot see, stated so nobody assumes otherwise.** The baseline is a
per-file *count*. A single change that fixes one input and adds another unnamed one *in
the same file* leaves the count equal and passes. Keying on line numbers would be
unstable and keying on ``id`` is impossible for inputs that have none — which is most of
them, that being the defect. The residual hole is one PR touching one file in both
directions at once; everything else is caught.

⚠️ **This is not a substitute for an axe sweep** (core#720's main scope). It checks one
rule, statically. It is here because it catches the one rule we have a confirmed
production instance of, on **every** form rather than on the nine pages in the scan
list — including connection config, the pipeline builder, the upload wizard and settings,
which the E2E harness never reaches — and because it needs no browser and no staging.

Proven able to fail by mutating the **real** artifact, not a fixture: stripping
``html_for`` off ``login-email`` in ``datanika/ui/pages/login.py`` reds
``test_the_auth_forms_keep_the_accessible_names_they_were_given`` (naming that id) and
``test_no_form_gains_an_input_without_an_accessible_name`` (``login.py`` 0 -> 1).
Harness: ``plans/qa/notes/probe-720/mutate_720.py``.
"""

import ast
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
UI_ROOT = REPO_ROOT / "datanika" / "ui"

#: Calls that render a text-entry control the user types into.
_INPUT_CALLS = {("rx", "input"), ("rx", "text_area"), ("rx", "el", "input")}

#: Props that carry an accessible name on their own, without a paired ``<label>``.
_ARIA_NAME_PROPS = {"aria_label", "aria_labelledby"}

#: Files with inputs that have no accessible name today, and how many each has.
#:
#: 🚨 **This table may only ever go DOWN.** When you give an input a label, lower the
#: number here in the same commit — that is the whole point, and it is what makes the
#: progress visible in a diff instead of invisible in a green tick. Adding an entry, or
#: raising one, means shipping a form no screen-reader user can complete.
#:
#: Measured on ``origin/dev d2ac870``: 67 unnamed across 13 files.
KNOWN_UNLABELLED: dict[str, int] = {
    "datanika/ui/components/captcha.py": 1,
    "datanika/ui/components/pipeline_mode_selector.py": 1,
    "datanika/ui/components/searchable_select.py": 1,
    "datanika/ui/components/secure_input.py": 2,
    # api_keys.py had 1 until core#886 consolidated the duplicated create block
    # into components/api_key_row.py and labelled it there. Row removed rather
    # than set to 0: this dict means "files carrying known debt".
    "datanika/ui/pages/dag.py": 3,
    "datanika/ui/pages/model_detail.py": 13,
    "datanika/ui/pages/pipelines.py": 4,
    "datanika/ui/pages/schedules.py": 3,
    "datanika/ui/pages/settings.py": 13,  # was 14 — same core#886 consolidation
    "datanika/ui/pages/sql_editor.py": 1,
    "datanika/ui/pages/transformations.py": 7,
    "datanika/ui/pages/uploads.py": 16,
}

#: The eight ids ``8a5c90d`` bound to a ``<label>``. These are the production fix for
#: the defect this module exists for; if one loses its label the regression is the
#: original bug, so they are asserted by name rather than only by count.
LABELLED_AUTH_INPUT_IDS = frozenset(
    {
        "forgot-email",
        "login-email",
        "login-password",
        "reset-confirm",
        "reset-password",
        "signup-email",
        "signup-full-name",
        "signup-password",
    }
)

#: Lower bound on inputs the walk must find. An analyser that silently stops walking
#: returns an empty set, which would satisfy every "no new violations" assertion in this
#: file. A skip is the same colour as a pass; so is a vacuous pass.
_MIN_INPUTS_EXPECTED = 60


def _dotted(node: ast.AST) -> tuple[str, ...]:
    """``('rx', 'input')`` for ``rx.input``; ``()`` for anything not a dotted name."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return tuple(reversed(parts))
    return ()


def _str_literal(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


class _Walk:
    """One pass over ``datanika/ui`` — the inputs, and every ``html_for`` target.

    ``html_for`` is collected across the whole tree rather than per file: a label and
    the input it names may legitimately live in different modules, and a per-file rule
    would report a false violation for that.
    """

    def __init__(self, root: pathlib.Path):
        self.root = root
        self.html_for: set[str] = set()
        self.inputs: list[tuple[str, int, str, str | None, bool]] = []
        self.files_walked = 0
        self._walk()

    def _walk(self) -> None:
        for path in sorted(self.root.rglob("*.py")):
            source = path.read_bytes().decode("utf-8")
            # A SyntaxError here must NOT be swallowed: a file that cannot be parsed is
            # a file whose inputs are invisible, which reads as "no violations".
            tree = ast.parse(source, filename=str(path))
            self.files_walked += 1
            rel = path.relative_to(REPO_ROOT).as_posix()
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                for kw in node.keywords:
                    if kw.arg == "html_for":
                        target = _str_literal(kw.value)
                        if target is not None:
                            self.html_for.add(target)
                name = _dotted(node.func)
                if name in _INPUT_CALLS:
                    kwargs = {kw.arg for kw in node.keywords if kw.arg}
                    id_literal = next(
                        (_str_literal(kw.value) for kw in node.keywords if kw.arg == "id"),
                        None,
                    )
                    self.inputs.append(
                        (
                            rel,
                            node.lineno,
                            ".".join(name),
                            id_literal,
                            bool(kwargs & _ARIA_NAME_PROPS),
                        )
                    )

    def is_named(self, id_literal: str | None, has_aria: bool) -> bool:
        """An input has an accessible name via aria-*, or via a label pointed at its id.

        ``placeholder`` is deliberately **not** accepted. It is announced inconsistently,
        disappears the moment the field has a value, and treating it as a name is what
        makes an unlabelled form look compliant.
        """
        if has_aria:
            return True
        return id_literal is not None and id_literal in self.html_for

    def unnamed_by_file(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for rel, _line, _call, id_literal, has_aria in self.inputs:
            if not self.is_named(id_literal, has_aria):
                counts[rel] = counts.get(rel, 0) + 1
        return counts


@pytest.fixture(scope="module")
def walk() -> _Walk:
    return _Walk(UI_ROOT)


def test_the_walk_actually_reaches_the_inputs(walk: _Walk):
    """Anti-vacuity. Every other test here is satisfied by finding nothing."""
    assert walk.files_walked >= 20, (
        f"Only {walk.files_walked} UI modules parsed. The walk is not reaching "
        f"{UI_ROOT}, so 'no new unlabelled inputs' below means nothing."
    )
    assert len(walk.inputs) >= _MIN_INPUTS_EXPECTED, (
        f"Found only {len(walk.inputs)} input controls under {UI_ROOT}, expected at "
        f"least {_MIN_INPUTS_EXPECTED}. Either the walk broke, or the app lost most of "
        "its forms. Both need a human — do not lower this bound to make it pass."
    )
    assert walk.html_for, (
        "No html_for= target was found anywhere in the UI. Since an input is judged "
        "named by pointing at one, an empty set would mark every input in the product "
        "as a violation — or, with a baseline this wide, hide a real regression."
    )


def test_no_form_gains_an_input_without_an_accessible_name(walk: _Walk):
    """The ratchet. Compared per file and in both directions, on purpose."""
    actual = walk.unnamed_by_file()
    regressions, improvements = [], []

    for rel in sorted(set(actual) | set(KNOWN_UNLABELLED)):
        now = actual.get(rel, 0)
        allowed = KNOWN_UNLABELLED.get(rel, 0)
        if now > allowed:
            regressions.append(f"  {rel}: {allowed} allowed -> {now} found (+{now - allowed})")
        elif now < allowed:
            improvements.append(f'    "{rel}": {now},   # was {allowed}')

    assert not regressions, (
        "An input was added with no accessible name — a screen reader announces "
        "nothing for it, and `page.getByLabel(...)` cannot find it either:\n"
        + "\n".join(regressions)
        + "\n\nGive it a name: rx.el.label(..., html_for='some-id') paired with "
        "rx.input(id='some-id'), as datanika/ui/pages/login.py does. A neighbouring "
        "rx.text() is NOT a label — that is exactly the defect (core#720)."
    )
    assert not improvements, (
        "Inputs were given accessible names — thank you. Lower KNOWN_UNLABELLED in "
        "this file to lock the improvement in, or the next regression will be "
        "measured against a stale ceiling:\n" + "\n".join(improvements)
    )


def test_the_baseline_names_only_files_that_still_exist(walk: _Walk):
    """A baseline entry for a deleted file is a permanent free pass.

    Deleting the form is one of the ways this guard could be 'satisfied' without any
    input gaining a name, so a stale entry has to fail rather than quietly hold.
    """
    missing = [rel for rel in KNOWN_UNLABELLED if not (REPO_ROOT / rel).exists()]
    assert not missing, (
        "KNOWN_UNLABELLED names files that no longer exist: "
        + ", ".join(sorted(missing))
        + ". Remove the entries — an exemption for a file nobody can see is one "
        "nobody will ever revisit."
    )


def test_the_auth_forms_keep_the_accessible_names_they_were_given(walk: _Walk):
    """Direct regression guard on 8a5c90d — the production fix this module descends from."""
    lost = sorted(LABELLED_AUTH_INPUT_IDS - walk.html_for)
    assert not lost, (
        "These auth inputs lost the <label> that 8a5c90d gave them: "
        + ", ".join(lost)
        + ". This is the original core#720 defect returning: the field renders, the "
        "text beside it still reads correctly, and both a screen reader and "
        "page.getByLabel() get nothing."
    )

    named_ids = {
        id_literal
        for _rel, _line, _call, id_literal, has_aria in walk.inputs
        if id_literal is not None and walk.is_named(id_literal, has_aria)
    }
    orphaned = sorted(LABELLED_AUTH_INPUT_IDS - named_ids)
    assert not orphaned, (
        "A <label html_for=...> still exists for these ids but no input carries them: "
        + ", ".join(orphaned)
        + ". A label pointing at nothing is not a name; check the input's id= was not "
        "renamed or made dynamic."
    )


@pytest.mark.parametrize(
    ("source", "expected", "why"),
    [
        ('rx.input(aria_label="Email")', True, "aria_label is an accessible name"),
        ('rx.input(aria_labelledby="email-hint")', True, "aria_labelledby is one too"),
        ('rx.input(id="login-email")', True, "an id a label points at"),
        ('rx.input(placeholder="Email")', False, "placeholder is NOT an accessible name"),
        ('rx.input(name="email", type="email")', False, "name= is submitted, not announced"),
        ('rx.input(id="not-pointed-at")', False, "an id no label points at names nothing"),
    ],
)
def test_the_naming_rule_itself(walk: _Walk, source: str, expected: bool, why: str):
    """Guard the guard.

    The real control for this module is the mutation harness against login.py — a
    synthetic case is written from the same model as the check and agrees with it
    including where it is wrong (WORKFLOW_RULES §13). These pin the one thing the
    mutation cannot show: that `placeholder` and `name` are rejected, which is the
    distinction the whole rule rests on.
    """
    call = ast.parse(source).body[0].value
    kwargs = {kw.arg for kw in call.keywords if kw.arg}
    id_literal = next((_str_literal(kw.value) for kw in call.keywords if kw.arg == "id"), None)
    named = walk.is_named(id_literal, bool(kwargs & _ARIA_NAME_PROPS))
    assert named is expected, f"{source} -> named={named}, expected {expected}: {why}"

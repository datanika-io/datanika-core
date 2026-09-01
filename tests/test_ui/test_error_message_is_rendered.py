"""A state that writes `error_message` must have somewhere that reads it (core#887).

`error_message` is declared on `BaseState`, so **every** substate has one and
every assignment type-checks, lints and passes tests. Nothing distinguishes a
state whose page renders it from one whose page does not: the difference lives
in a *different file*, in the page, and only as an absence. The handler decides
to refuse, writes down why, returns normally, and the user sees nothing happen.

That is the mechanism behind an entire class of "the product did nothing and
said nothing", and it outlives any individual repair, because the next handler
to be written gets no signal either.

**Why this cannot be fixed on `BaseState`.** `AuthState.session_expired` already
carries the reasoning (`auth_state.py`): every substate gets its *own* copy of
an inherited var, so a flag set on `ApiKeyState` is invisible to `page_layout`.
Inheritance buys nothing here, which is why the renderer has to be matched by
the state class actually named at the call site.

## 🚨 The indirection that a name-only audit cannot see

`error_or_quota_callout(state_cls)` (`ui/components/quota_callout.py`) renders
`state_cls.error_message` for **whatever class it is handed**. So a page can
render a state's errors while containing the string `error_message` zero times.
Five pages do exactly that.

The audit on core#887 matched receiver *names* and therefore reported ten
unrendered classes. Four of them — `ConnectionState`, `PipelineState`,
`ScheduleState`, `UploadState` — are rendered through that component and were
never broken. **Its negative control could not have caught this**: the five
classes known to be rendered are *also* rendered directly, so a check that only
ever exercises the direct path agrees with itself. A control has to include the
shape that would break the rule, expressed the way the real code expresses it.

So this walk resolves both paths — a direct `SomeState.error_message` in a page,
**and** a call passing a state class into a component that reads it off its own
parameter. `test_the_indirect_path_is_resolved` is the control for the second
one, and it is the reason the allowlist below is empty rather than six long.

## The shape of this check

`UNRENDERED_ERROR_MESSAGE_STATES` is an allowlist that **shrinks**. It is empty
today, because the two genuine offenders (`BackupState`, `NotificationState` —
both on `/settings`, where `SettingsState`'s errors *did* show) were fixed in
the same change rather than recorded. Both directions are asserted:

* a state that starts assigning `error_message` with nothing rendering it fails
  immediately — that is the whole value of the check;
* an entry that is no longer an offender **also** fails, naming itself, and
  refuses to pass until it is deleted.

A one-direction allowlist stays green over a stale claim forever.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

UI = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "ui"
STATE_DIR = UI / "state"
RENDER_DIRS = (UI / "pages", UI / "components")

VAR = "error_message"

#: States that assign `error_message` with nothing rendering it. **Empty on
#: purpose** — fix the page, do not add a name here. If you genuinely must,
#: say why in a comment beside the entry and open an issue.
UNRENDERED_ERROR_MESSAGE_STATES: frozenset[str] = frozenset()

#: `BaseState` is the declaration site, not a page's state — it reaches a screen
#: through whichever substate a page mounts, so the name rule does not apply.
#: Kept separate from the allowlist so it cannot be misread as a live offender.
NOT_A_PAGE_STATE: frozenset[str] = frozenset({"BaseState"})


def _is_clearing(value: ast.expr) -> bool:
    """`self.error_message = ""` clears a previous report; it is not one.

    Counting clears makes a state that only ever *resets* the var look like one
    that refuses silently — the opposite of the finding. Three classes
    (`DashboardState`, `ModelState`, `RunState`) assign it exactly once, and
    every one of those assignments is a clear.
    """
    return isinstance(value, ast.Constant) and value.value == ""


def _assigning_states() -> dict[str, int]:
    """{class name: count of non-clearing `self.error_message = ...` writes}."""
    found: dict[str, int] = {}
    for path in sorted(STATE_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for cls in (n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)):
            count = 0
            for node in ast.walk(cls):
                if isinstance(node, ast.Assign):
                    targets, value = list(node.targets), node.value
                elif isinstance(node, ast.AnnAssign) and node.value is not None:
                    targets, value = [node.target], node.value
                else:
                    continue
                for t in targets:
                    if (
                        isinstance(t, ast.Attribute)
                        and t.attr == VAR
                        and isinstance(t.value, ast.Name)
                        and t.value.id == "self"
                        and not _is_clearing(value)
                    ):
                        count += 1
            if count:
                found[cls.name] = found.get(cls.name, 0) + count
    return found


def _param_renderers(tree: ast.AST) -> dict[str, int]:
    """{function name: index of the parameter it reads `.error_message` off}.

    This is the `error_or_quota_callout(state_cls)` shape — a component that
    renders the var for whichever state class it is given.
    """
    out: dict[str, int] = {}
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        params = [a.arg for a in (*fn.args.posonlyargs, *fn.args.args)]
        for node in ast.walk(fn):
            if (
                isinstance(node, ast.Attribute)
                and node.attr == VAR
                and isinstance(node.value, ast.Name)
                and node.value.id in params
            ):
                out[fn.name] = params.index(node.value.id)
                break
    return out


def _rendered_names() -> tuple[set[str], set[str], dict[str, int]]:
    """Names `X` whose `X.error_message` reaches a screen.

    Returns (resolved class names, unresolved receivers, param-renderer map).
    An unresolved receiver is one this walk cannot attribute to a class; those
    are surfaced rather than dropped, because silently discarding a renderer
    manufactures offenders.
    """
    trees: list[ast.AST] = []
    for directory in RENDER_DIRS:
        for path in sorted(directory.rglob("*.py")):
            trees.append(ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))

    # Pass 1 — which helpers render the var off a parameter.
    renderers: dict[str, int] = {}
    for tree in trees:
        renderers.update(_param_renderers(tree))

    concrete: set[str] = set()
    unresolved: set[str] = set()
    for tree in trees:
        local_names = {
            a.arg
            for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            for a in (*n.args.posonlyargs, *n.args.args, *n.args.kwonlyargs)
        }
        # Pass 2a — direct `SomeState.error_message` in a page or component.
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == VAR:
                recv = node.value
                if isinstance(recv, ast.Name) and recv.id not in local_names:
                    concrete.add(recv.id)
                elif isinstance(recv, ast.Name):
                    pass  # a parameter: resolved at its call sites in 2b
                else:
                    unresolved.add(ast.unparse(recv))
        # Pass 2b — `error_or_quota_callout(SomeState)` and friends.
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fname = (
                node.func.id
                if isinstance(node.func, ast.Name)
                else node.func.attr
                if isinstance(node.func, ast.Attribute)
                else None
            )
            idx = renderers.get(fname) if fname else None
            if idx is None or idx >= len(node.args):
                continue
            arg = node.args[idx]
            if isinstance(arg, ast.Name) and arg.id not in local_names:
                concrete.add(arg.id)
            else:
                unresolved.add(f"{fname}({ast.unparse(arg)})")
    return concrete, unresolved, renderers


def audit() -> dict:
    assigning = _assigning_states()
    rendered, unresolved, renderers = _rendered_names()
    unrendered = {c for c in assigning if c not in rendered and c not in NOT_A_PAGE_STATE}
    return {
        "assigning": assigning,
        "rendered": rendered,
        "unresolved": unresolved,
        "renderers": renderers,
        "unrendered": unrendered,
    }


@pytest.fixture(scope="module")
def report() -> dict:
    return audit()


class TestTheAuditCanSeeWhatItClaimsTo:
    """Negative controls. Without these the real assertion proves nothing — a
    broken walker reports an empty finding, and empty reads as clean."""

    def test_it_finds_states_that_assign_error_message(self, report):
        assert len(report["assigning"]) >= 10, (
            "the AST walk found almost no assignments — it is broken, not the "
            f"codebase clean. found={report['assigning']}"
        )

    def test_it_finds_pages_that_render_error_message(self, report):
        assert len(report["rendered"]) >= 8, (
            f"renderer detection resolved almost nothing: {sorted(report['rendered'])}"
        )

    def test_the_directly_rendered_states_come_back_rendered(self, report):
        """The direct path — a page naming the state itself."""
        for known in ("SettingsState", "TransformationState", "ApiKeyState", "DagState"):
            assert known in report["rendered"], f"{known} renders error_message but was not seen"

    def test_the_indirect_path_is_resolved(self, report):
        """🔑 The control that the core#887 audit was missing.

        These four appear in no page as `X.error_message`; they reach a screen
        only through `error_or_quota_callout(X)`. A walk that matches receiver
        names alone calls all four broken — which is what happened. If this
        test ever goes red while the direct-path test above stays green, the
        call-site resolution has silently stopped working and every offender
        this suite reports is suspect.
        """
        assert "error_or_quota_callout" in report["renderers"], (
            "the shared error callout was not recognised as a parameter "
            f"renderer; found renderers={report['renderers']}"
        )
        for indirect in ("UploadState", "ConnectionState", "PipelineState", "ScheduleState"):
            assert indirect in report["rendered"], (
                f"{indirect} is rendered via error_or_quota_callout({indirect}) but the "
                "call-site resolution did not see it"
            )

    def test_no_unresolved_receiver_hides_a_renderer(self, report):
        assert report["unresolved"] == set(), (
            "a page/component reads `error_message` off something this audit cannot "
            f"resolve to a state class: {sorted(report['unresolved'])}. Resolve it "
            "explicitly before trusting the offender list."
        )


class TestEveryWrittenErrorMessageHasAReader:
    def test_no_state_writes_error_message_into_the_void(self, report):
        """The assertion the check exists for."""
        new = report["unrendered"] - UNRENDERED_ERROR_MESSAGE_STATES
        assert not new, (
            f"{sorted(new)} assign `{VAR}` and nothing renders it, so every refusal "
            "they write is invisible to the user. Render it on the page — "
            "`error_or_quota_callout(<TheState>)` is the one-line way — or route the "
            "refusal through a channel `page_layout` already reads. Do not add it "
            "to the allowlist."
        )

    def test_the_allowlist_has_no_stale_entries(self, report):
        """The other direction, and the half that is usually missing.

        An allowlist that only forgives stays green over a claim that stopped
        being true. This one names its own dead entries and refuses to pass.
        """
        stale = UNRENDERED_ERROR_MESSAGE_STATES - report["unrendered"]
        assert not stale, (
            f"{sorted(stale)} no longer assign `{VAR}` without a reader — the gap is "
            "closed. Delete them from UNRENDERED_ERROR_MESSAGE_STATES; leaving them "
            "in tells the next reader about a problem that does not exist."
        )


if __name__ == "__main__":  # pragma: no cover — the audit, runnable by hand
    r = audit()
    width = max(len(c) for c in r["assigning"])
    print(f"{'state class':<{width}}  sets  rendered")
    print("-" * (width + 22))
    for cls, n in sorted(r["assigning"].items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"{cls:<{width}}  {n:>4}  {'yes' if cls in r['rendered'] else '-- NOWHERE --'}")
    print("-" * (width + 22))
    print(f"{len(r['assigning'])} classes assign, {sum(r['assigning'].values())} reports")
    print(f"param renderers: {r['renderers']}")
    print(f"unrendered: {sorted(r['unrendered'])}")
    print(f"unresolved: {sorted(r['unresolved'])}")

r"""SPEC_AUDIT_TRAIL §2.2 and §2.3 — the vocabulary an audit call site is allowed to use.

Two clauses, one walker, because they are the same sweep over the same call sites:

* **§2.2** — the ``action`` must be an ``AuditAction`` member (core#1127).
* **§2.3** — the ``resource_type`` must be a value the reader can filter for (core#1128),
  asserted in *both* directions: nothing written that the filter omits, nothing offered
  that no writer writes.

Part one — §2.2
===============

``BaseState._audit`` does ``AuditAction(action)`` and **swallows the ``ValueError``**
(``base_state.py:212-246``), by design: an audit failure must never break the operation it
describes. The cost of that design is that **a misspelled action is a silently dropped
row** — no test fails, no page breaks, and the only evidence is a log line nobody watches.

That is not hypothetical. It shipped: ``SettingsState.transfer_ownership`` passed
``"transfer_ownership"``, which is not a member, so **the highest-privilege action in the
product had never written an audit row** (core#1127). Everything else about that call was
correct — one string cost the entire row.

Why a source sweep and not a runtime assertion
----------------------------------------------
The runtime path already "validates": ``AuditAction(action)`` raises on a bad value. The
problem is precisely that the raise is caught and the operation continues, so validation
at runtime produces *silence*. This guard moves the verdict to **authoring** time, where
it is a red check on the PR that introduces the next one.

Why unresolvable shapes FAIL rather than being skipped
------------------------------------------------------
⚠️ A classifier that returns "probably fine" for a shape it does not understand is
emitting a skip, and **a skip is the same colour as a pass**. This bit us on core#1139:
a guard resolving a raised class with ``getattr`` treated an *unresolvable* name as clean,
so the defect it existed to catch walked past it. So the polarity here is fail-closed —
an ``action`` argument this walker cannot reduce to a member is **reported**, and the
message says how to make it analysable. There are zero such sites today, so the polarity
costs nothing now and catches the first one.

Three shapes are accepted, and each is accepted for a stated reason:

======================  ==================================================================
``"update"``            a literal — checked against the enum's *values*
``AuditAction.UPDATE``  a member reference — checked against the enum's *names*
``AuditAction(expr)``   **validated by construction**: this call raises on a bad value
                        before ``log_action`` is reached. It is what the chokepoint in
                        ``base_state.py`` itself does, and it is sound anywhere.
======================  ==================================================================

``IfExp`` is followed, so ``"create" if is_new else "update"`` resolves to both branches.

Scope
-----
This sweeps ``datanika/``, and additionally a sibling ``datanika_cloud/`` package when one
is reachable from the checkout. ⚠️ **Core CI checks out no cloud tree** (``ci.yml``'s
``test`` job takes this repo alone; only the image jobs fetch cloud), so in CI this guard
speaks for core. That is adequate rather than lucky: measured 2026-09-06 on
``datanika-cloud`` @ ``dev``, the cloud package contains **0** calls to ``_audit`` or
``log_action`` — it emits no audit rows at all. ``test_the_sweep_is_armed`` names the
roots it actually walked, so a core-only run can never be mistaken for a both-roots run.

Re-derive with::

    grep -rnE "_audit\(|log_action\(" datanika_cloud | wc -l
"""

from __future__ import annotations

import ast
import pathlib

import pytest

from datanika.models.audit_log import AuditAction

CORE_ROOT = pathlib.Path(__file__).resolve().parents[2] / "datanika"

#: ``BaseState._audit`` (the ~34 UI call sites) and the service method it forwards to,
#: which ``auth_state`` and ``user_service`` reach directly.
AUDIT_CALLEES = ("_audit", "log_action")

#: ``_audit(session, org_id, user_id, action, resource_type, ...)`` and
#: ``log_action(session, org_id, user_id, action, resource_type, ...)`` agree on the
#: position of ``action``, which is what lets one walker read both.
ACTION_POSITION = 3

#: ``resource_type`` sits one place further along in both signatures.
RESOURCE_POSITION = 4

VALID_VALUES = frozenset(a.value for a in AuditAction)
VALID_NAMES = frozenset(a.name for a in AuditAction)


def _cloud_root() -> pathlib.Path | None:
    """A sibling ``datanika_cloud`` package, if this checkout can see one.

    Absent in CI and in a bare clone; present in the monorepo working layout. Returned
    rather than assumed so the armed test can *say* which roots were walked.
    """
    worktrees = CORE_ROOT.parents[1]
    for candidate in sorted(worktrees.glob("datanika-cloud-*/datanika_cloud")):
        if candidate.is_dir():
            return candidate
    return None


def _roots() -> list[pathlib.Path]:
    cloud = _cloud_root()
    return [CORE_ROOT] if cloud is None else [CORE_ROOT, cloud]


def _python_files(root: pathlib.Path) -> list[pathlib.Path]:
    return sorted(p for p in root.rglob("*.py") if "__pycache__" not in p.parts)


def _callee_name(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return ""


def _action_arg(node: ast.Call) -> ast.AST | None:
    for kw in node.keywords:
        if kw.arg == "action":
            return kw.value
    if len(node.args) > ACTION_POSITION:
        return node.args[ACTION_POSITION]
    return None


def _resource_arg(node: ast.Call) -> ast.AST | None:
    for kw in node.keywords:
        if kw.arg == "resource_type":
            return kw.value
    if len(node.args) > RESOURCE_POSITION:
        return node.args[RESOURCE_POSITION]
    return None


def _classify(node: ast.AST) -> tuple[bool, str]:
    """``(is_an_audit_action, description)`` for one ``action`` argument.

    ``False`` covers both "resolves to a non-member" and "does not resolve at all" —
    deliberately, because the caller must treat them the same way. The description is
    what the failure message shows, so it distinguishes them for the human.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        ok = node.value in VALID_VALUES
        return ok, f"{node.value!r}" + ("" if ok else " is not an AuditAction value")

    if (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "AuditAction"
    ):
        ok = node.attr in VALID_NAMES
        return ok, f"AuditAction.{node.attr}" + ("" if ok else " is not an AuditAction member")

    # ``AuditAction(expr)`` cannot reach ``log_action`` with a bad value: the constructor
    # raises first. This is the chokepoint's own shape, recognised by shape rather than
    # by file path, so moving ``_audit`` does not silently create an exemption.
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "AuditAction"
    ):
        return True, "AuditAction(...) — validated by construction"

    if isinstance(node, ast.IfExp):
        body_ok, body_desc = _classify(node.body)
        else_ok, else_desc = _classify(node.orelse)
        return body_ok and else_ok, f"{body_desc} / {else_desc}"

    return False, f"unresolvable: {ast.unparse(node)}"


def _scan() -> tuple[list[str], int, list[str]]:
    """``(offending, actions_seen, roots_walked)``."""
    offending: list[str] = []
    seen = 0
    walked: list[str] = []

    for root in _roots():
        walked.append(root.as_posix())
        for path in _python_files(root):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            rel = path.relative_to(root.parent).as_posix()

            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or _callee_name(node) not in AUDIT_CALLEES:
                    continue
                arg = _action_arg(node)
                if arg is None:
                    # No action argument at all: a different signature, not our call.
                    continue
                seen += 1
                ok, desc = _classify(arg)
                if not ok:
                    offending.append(f"{rel}:{node.lineno} action={desc}")
    return offending, seen, walked


def test_the_sweep_is_armed() -> None:
    """Guard the guard, and say out loud what it walked.

    Every assertion below is vacuously true if the walker finds no call sites — a wrong
    root, a renamed helper, a parse failure. Pin both ends: there really are audit call
    sites, and ``AuditAction`` really is populated (an empty enum makes every membership
    test fail rather than pass, but an enum whose values stopped being strings would make
    ``VALID_VALUES`` empty and every literal offending, which is the loud direction).
    """
    _, seen, walked = _scan()
    assert seen >= 30, (
        f"only {seen} audit action arguments found across {walked} — is the walker "
        "reaching datanika/? A renamed helper empties this file silently."
    )
    assert VALID_VALUES and VALID_NAMES, "AuditAction is empty; this whole file is vacuous"
    assert CORE_ROOT.as_posix() in walked, f"core was not walked: {walked}"


def test_every_audit_action_argument_is_an_audit_action() -> None:
    """SPEC_AUDIT_TRAIL §2.2, core#1127.

    🔑 Proved red on the real tree before the fix, which is the control that matters and
    it was free::

        tests/.../test_audit_action_call_sites.py::test_every_audit_action_argument_...
        E  datanika/ui/state/settings_state.py:453 action='transfer_ownership' is not
           an AuditAction value

    A version of this test that was green on that tree would not have been measuring the
    action argument at all.
    """
    offending, _, walked = _scan()
    assert not offending, (
        "these audit call sites pass an action AuditAction() cannot accept, so "
        "BaseState._audit's deliberate swallow drops the row and nothing goes red "
        "(SPEC_AUDIT_TRAIL §2.2).\n  "
        + "\n  ".join(offending)
        + "\n\nUse one of the six members "
        + f"({sorted(VALID_VALUES)}), following erase_user's precedent: record the fact "
        "inside the existing enum rather than adding a member, which is an expand/"
        "contract pair because audit_logs.action is Enum(..., native_enum=False) and the "
        "previously deployed container raises LookupError when it READS a value it does "
        f"not know.\n\nroots walked: {walked}"
    )


@pytest.mark.parametrize(
    ("source", "should_offend"),
    [
        ('self._audit(s, 1, 2, "transfer_ownership", "member")', True),
        ('self._audit(s, 1, 2, "update", "member")', False),
        ('self._audit(s, 1, 2, action="nope", resource_type="member")', True),
        ('self._audit(s, 1, 2, action="delete", resource_type="member")', False),
        ("svc.log_action(s, 1, 2, AuditAction.NOT_A_MEMBER, 'member')", True),
        ("svc.log_action(s, 1, 2, AuditAction.DELETE, 'member')", False),
        ("svc.log_action(s, 1, 2, AuditAction(action), resource_type)", False),
        ("self._audit(s, 1, 2, computed_action, 'member')", True),
        ("self._audit(s, 1, 2, 'create' if new else 'update', 'member')", False),
        ("self._audit(s, 1, 2, 'create' if new else 'transferred', 'member')", True),
    ],
)
def test_the_classifier_discriminates(source: str, should_offend: bool) -> None:
    """Both halves, in one test — the rejecting half AND the accepting half.

    ⚠️ Written this way because a guard that only demonstrates a red proves nothing about
    its own polarity: *"the artifact must not contain X"* is satisfied by a checker that
    rejects everything, and a checker that rejects everything is discovered the day it
    reds a correct PR, not the day it is written. The accepting rows are the half that
    says this guard will still be here in a month.

    Synthetic rather than by mutating a real file, per the standing rule: the mutation
    harnesses in this repo have twice been the thing that was broken.
    """
    tree = ast.parse(source)
    call = next(
        n for n in ast.walk(tree) if isinstance(n, ast.Call) and _callee_name(n) in AUDIT_CALLEES
    )
    arg = _action_arg(call)
    assert arg is not None, f"the walker cannot even find the action argument in {source!r}"
    ok, desc = _classify(arg)
    assert ok is not should_offend, (
        f"classifier said {desc!r} for {source!r}; expected "
        f"{'an offence' if should_offend else 'acceptance'}"
    )


# ---------------------------------------------------------------------------------------
# SPEC_AUDIT_TRAIL §2.3 — "the resource_type must be a value the reader can filter for"
# ---------------------------------------------------------------------------------------
#
# The second failure mode SPEC_AUDIT_TRAIL §1 names, and the worse of the two: **an absent
# audit log is not consulted; a lying one is believed.** Both render the same empty table.
#
# Measured on origin/dev @ aa078bb, 2026-09-06 — 13 resource_type values are written and
# the filter offered 7, and the two sets did not overlap in either direction:
#
#   written and NOT filterable (7)  import member notification_channel org password
#                                   session user
#   filterable and NEVER written(1) membership
#
# `member` carries 7 of the 13 writes — every membership change, invitation, role change,
# and `leave_org`. An admin asking "who removed this person?" picked `membership`, the one
# option on screen that looks right, and got an empty table (core#1128).


def _written_resource_types() -> tuple[set[str], list[str]]:
    """``(literal values written, arguments this walker could not reduce)``."""
    written: set[str] = set()
    unresolved: list[str] = []

    for root in _roots():
        for path in _python_files(root):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            rel = path.relative_to(root.parent).as_posix()
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or _callee_name(node) not in AUDIT_CALLEES:
                    continue
                arg = _resource_arg(node)
                if arg is None:
                    continue
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    written.add(arg.value)
                else:
                    # The chokepoint forwards its own parameter; that is the only
                    # unresolved site today and it writes nothing of its own.
                    unresolved.append(f"{rel}:{node.lineno} {ast.unparse(arg)}")
    return written, unresolved


def test_the_resource_sweep_is_armed() -> None:
    """Guard the guard. An empty written set makes every comparison below vacuous."""
    written, unresolved = _written_resource_types()
    assert len(written) >= 12, (
        f"only {len(written)} resource types found ({sorted(written)}) — is the walker "
        "reaching datanika/?"
    )
    assert "member" in written, (
        "the type carrying 7 of the writes is missing from the sweep, so the headline "
        "finding of core#1128 could not be reproduced by this file"
    )
    # Only ``BaseState._audit`` forwards a variable. If this grows, a call site has
    # started computing its resource type and the comparison below has a blind spot.
    assert len(unresolved) <= 1, f"unresolved resource_type arguments: {unresolved}"


def test_every_written_resource_type_is_filterable() -> None:
    """core#1128, direction 1: a row nobody can narrow to.

    A row written under a type absent from the filter is *in* the table and unreachable
    through the only UI that reads the table. ``password`` and ``session`` are the two an
    incident responder wants most.
    """
    from datanika.ui.pages.audit_logs import RESOURCE_FILTER_OPTIONS

    written, _ = _written_resource_types()
    offered = set(RESOURCE_FILTER_OPTIONS) - {"all"}
    missing = sorted(written - offered)
    assert not missing, (
        "these resource types are written by call sites and cannot be filtered for on "
        "/audit-logs, so the rows exist and the only instrument for reading them reports "
        f"nothing (SPEC_AUDIT_TRAIL §2.3):\n  {missing}"
    )


def test_every_filter_option_is_something_a_call_site_writes() -> None:
    """core#1128, direction 2: an option that matches nothing, ever.

    🚨 This is the half that made the defect dangerous rather than merely incomplete. An
    option that returns an empty table does not read as a broken filter — it reads as
    *"nobody did it"*, which is precisely the answer the audit log exists to give
    truthfully.
    """
    from datanika.ui.pages.audit_logs import RESOURCE_FILTER_OPTIONS

    written, _ = _written_resource_types()
    offered = set(RESOURCE_FILTER_OPTIONS) - {"all"}
    orphans = sorted(offered - written)
    assert not orphans, (
        "these filter options are offered on /audit-logs and no call site has ever "
        "written them, so selecting one returns an empty table that reads as 'nothing "
        f"happened':\n  {orphans}"
    )


def test_every_action_filter_option_is_an_audit_action() -> None:
    """core#1128 AC4 — the action filter is the same hand-list shape.

    It happened to be correct, which is not the same as being *kept* correct: it is a
    second copy of ``AuditAction``'s membership maintained by memory, and the adjacent
    core#1127 is what that costs when a copy drifts.
    """
    from datanika.ui.pages.audit_logs import ACTION_FILTER_OPTIONS

    offered = set(ACTION_FILTER_OPTIONS) - {"all"}
    assert offered == set(VALID_VALUES), (
        "the /audit-logs action filter and AuditAction disagree.\n"
        f"  offered but not a member: {sorted(offered - VALID_VALUES)}\n"
        f"  a member but not offered: {sorted(VALID_VALUES - offered)}"
    )


def test_the_page_renders_the_derived_lists_rather_than_its_own_literals() -> None:
    """The lists must reach the component by name, or the constants above are decoration.

    ⚠️ Asserted as a **presence** — *the option argument is one of these two names* —
    rather than as the absence of a list literal. A ban on literals would be satisfied by
    deleting the select altogether, and would go red on a perfectly good refactor that
    happened to inline something unrelated. The presence form fails exactly when the page
    stops reading the derived list, which is the regression this is for.
    """
    page = CORE_ROOT / "ui" / "pages" / "audit_logs.py"
    tree = ast.parse(page.read_text(encoding="utf-8"), filename=str(page))
    options_args = [
        node.args[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _callee_name(node) == "searchable_select" and node.args
    ]
    assert len(options_args) == 2, (
        f"expected the two filter selects on /audit-logs, found {len(options_args)}"
    )
    names = sorted(a.id for a in options_args if isinstance(a, ast.Name))
    assert names == ["ACTION_FILTER_OPTIONS", "RESOURCE_FILTER_OPTIONS"], (
        "the filter selects must be handed the derived module constants; found "
        f"{[ast.unparse(a) for a in options_args]}"
    )

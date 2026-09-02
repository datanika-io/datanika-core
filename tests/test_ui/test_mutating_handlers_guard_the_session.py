"""Every handler that writes to the database revalidates the session (#673).

#671 guarded page loads. #760 guarded ``BaseState._check_role``, and its module
docstring states the coverage claim that made one guard sufficient:

    "~20 mutating handlers across 9 state modules already route through it, so
     one guard covers them all"

**That claim was never tested, and it is false.** It is prose in a docstring,
which is exactly the shape this project keeps paying for — a guard whose reach
is asserted rather than measured. Of the public handlers that reach a
``session.commit()``, 25 route through ``_check_role`` and 13 do not; five of
those thirteen are unauthenticated entry points that *must not* be guarded, and
the remaining eight were writing to the database for sessions that had ended.

``session.commit()`` is the discriminator on purpose. A name heuristic
("save", "delete", "update") over the same package produced 20 candidates of
which 12 were false positives — ``existing.add(name)`` on a Python set,
``add_model()`` appending to a form list, an ``@rx.var`` named ``grants_write``.
A commit is not a guess about intent; it is the write.

⚠️ **A blanket guard would be wrong here, and that is why this is a coverage
test rather than a decorator.** Three classes need three answers:

* **role-gated mutations** — ``_check_role(min_role)``. Already correct.
* **mutations every member may perform** — ``leave_org``, ``change_password``,
  dismissing your own notification. These need the *session* checked and must
  **not** acquire a role gate: ``leave_org``'s own docstring records that it is
  "deliberately **not** gated on a minimum role", because leaving is the one
  member-management action every member has.
* **unauthenticated entry points** — ``login``, ``signup``, ``logout``,
  password reset. Guarding these breaks sign-in, and guarding ``logout``
  strands a user in a session they cannot end.

So the fix is ``BaseState._require_live_session()`` — the session half of
``_check_role``, extracted — applied to the second class only.
"""

import ast
import pathlib

import pytest

import datanika.ui.state

STATE_DIR = pathlib.Path(datanika.ui.state.__file__).parent

SESSION_GUARDS = ("_check_role", "_require_live_session")

# Handlers that write while signed OUT. Guarding any of these is a defect, so
# each carries the reason it is here rather than a bare name.
UNAUTHENTICATED: dict[tuple[str, str, str], str] = {
    (
        "auth_state.py",
        "AuthState",
        "login",
    ): "mints the session; there is nothing to revalidate yet",
    (
        "auth_state.py",
        "AuthState",
        "signup",
    ): "creates the user and the org; runs before any session exists",
    (
        "auth_state.py",
        "AuthState",
        "logout",
    ): "ENDS the session — a guard here strands a user who cannot sign out",
    (
        "password_reset_state.py",
        "PasswordResetState",
        "request_reset",
    ): "reached from /forgot-password by someone who cannot sign in",
    (
        "password_reset_state.py",
        "PasswordResetState",
        "submit_new_password",
    ): "reached from an emailed token by someone who cannot sign in",
}


def _call_names(fn: ast.AST) -> set[str]:
    names = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            f = node.func
            if isinstance(f, ast.Attribute):
                names.add(f.attr)
            elif isinstance(f, ast.Name):
                names.add(f.id)
    return names


def _committing_handlers() -> list[dict]:
    """Public handlers in the state package whose body reaches ``.commit()``."""
    found = []
    for path in sorted(STATE_DIR.glob("*.py")):
        if path.name == "__init__.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for cls in [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]:
            for fn in cls.body:
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if fn.name.startswith("_"):
                    continue
                calls = _call_names(fn)
                if "commit" not in calls:
                    continue
                found.append(
                    {
                        "key": (path.name, cls.name, fn.name),
                        "line": fn.lineno,
                        "calls": calls,
                        "is_async": isinstance(fn, ast.AsyncFunctionDef),
                    }
                )
    return found


def test_the_extractor_is_armed():
    """A sweep that finds nothing satisfies every assertion below in silence.

    This is the failure mode a source-derived guard is most likely to have: an
    AST walk that stops matching after a refactor reports a clean tree. The
    floor is set well under the current count (38) so ordinary churn does not
    trip it, and well above zero so a broken extractor does.
    """
    handlers = _committing_handlers()
    assert len(handlers) >= 30, (
        f"only {len(handlers)} committing handlers found in {STATE_DIR}; the "
        "extractor has stopped matching and every other test in this file is "
        "now vacuous"
    )


def test_every_allowlist_entry_matches_a_real_handler():
    """A stale exemption is an exemption nobody re-argued.

    If a handler is renamed, its allowlist entry goes dead — and the renamed
    handler correctly fails the coverage test. But the dead entry stays,
    quietly available to grant an exemption to some future handler that happens
    to take the old name.
    """
    live = {h["key"] for h in _committing_handlers()}
    stale = sorted(k for k in UNAUTHENTICATED if k not in live)
    assert not stale, (
        "these allowlist entries name no committing handler any more — delete "
        f"them or fix the name: {stale}"
    )


def test_every_committing_handler_guards_the_session():
    """The coverage claim #760's docstring makes, as an assertion.

    Red before the fix, naming eight handlers across six modules.
    """
    offenders = []
    for h in _committing_handlers():
        if h["key"] in UNAUTHENTICATED:
            continue
        if h["calls"] & set(SESSION_GUARDS):
            continue
        mod, cls, fn = h["key"]
        offenders.append(f"{mod}:{h['line']}  {cls}.{fn}")

    assert not offenders, (
        "these handlers write to the database without revalidating the "
        "session, so they execute for a session whose access token aged out "
        "and, transitively, for one a password change was meant to end "
        "(#673):\n  " + "\n  ".join(sorted(offenders)) + "\n\n"
        "Add `if not await self._require_live_session(): return` — NOT a role "
        "gate, unless the action genuinely requires one. If the handler runs "
        "signed out, add it to UNAUTHENTICATED with the reason."
    )


@pytest.mark.parametrize(
    "key",
    sorted(k for k in UNAUTHENTICATED),
    ids=lambda k: f"{k[1]}.{k[2]}",
)
def test_unauthenticated_entry_points_are_not_guarded(key):
    """The negative control, and the reason a blanket decorator is wrong.

    A guard applied to every committing handler would pass the coverage test
    above and break sign-in, sign-up and sign-out. This is the assertion that
    goes red if somebody "fixes" the coverage test by decorating everything.
    """
    by_key = {h["key"]: h for h in _committing_handlers()}
    handler = by_key.get(key)
    assert handler is not None, f"{key} is in the allowlist but no longer exists"
    guards = handler["calls"] & set(SESSION_GUARDS)
    assert not guards, (
        f"{key[1]}.{key[2]} calls {sorted(guards)}, but it runs for somebody who "
        f"is not signed in: {UNAUTHENTICATED[key]}"
    )


def test_the_guard_a_role_free_mutation_needs_actually_exists():
    """``_require_live_session`` must be a real method on ``BaseState``.

    Without it the only guard available is ``_check_role``, and applying that
    to ``leave_org`` contradicts its documented contract — leaving is the one
    member-management action every member has, refused only by the service's
    owner-count invariant.
    """
    from datanika.ui.state.base_state import BaseState

    assert hasattr(BaseState, "_require_live_session"), (
        "BaseState._require_live_session is missing, so a mutation that every "
        "member may perform has no way to check the session without also "
        "acquiring a role gate it must not have"
    )

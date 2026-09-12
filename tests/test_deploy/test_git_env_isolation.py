"""A test that runs `git` must not inherit the environment git gave the hook (core#1307).

What happened
-------------
`git push` ran the pre-push hook, the hook ran `tests/test_deploy` (its default scope), and the
worktree came back at a commit nobody wrote::

    commit ccbb08b   author: t <t@example.com>   subject: "seed"
    tree: scripts/promotion_gate.sh, seed.txt        <- every other tracked file DELETED

Mechanism: **git exports `GIT_DIR` to its hooks.** A fixture that builds a throwaway repo with
``subprocess.run([...], cwd=tmp_path)`` is correctly isolated by *directory* and not at all by
*environment* -- `git init` sees `GIT_DIR`, re-initialises the REAL repository, `cwd` becomes its
work tree, and the following ``git add -A`` records every tracked file as deleted because none of
them exist under ``tmp_path``. The commit lands on the branch being pushed.

⚠️ git prints the tell -- ``warning: re-init: ignored --initial-branch=main`` -- and
``capture_output=True`` sends it nowhere.

Why this guard exists rather than just the fix
----------------------------------------------
🔑 **The remedy already existed in this repository, twice, and the file that destroyed a branch did
not have it.** `test_hooks/test_pre_push_gating.py` and `test_scripts/test_mutation_probe.py`
both carry a `_clean_env()` whose docstring is the entire diagnosis, written before this happened:
*"It fails nowhere else: standalone and in a plain CI run there is no GIT_DIR, so the tests pass and
look fine."*

So the hazard was known and solved; what was missing was anything that made the next file inherit
the solution. A list of files that happen to know is what we already had.

This scan is therefore **derived** -- it finds git subprocesses by reading the call, not by
consulting a list -- and it asserts the PRESENCE of a scrub rather than the absence of a symptom.
`ENGINEERING_RULES` §58 states the general rule; this is the case with teeth.

What a matcher for this has to cover, so it is not re-earned
------------------------------------------------------------
🚨 **The AST match MUST cover the loop-variable form.** The first version of this scan matched only
a literal argv -- ``subprocess.run(["git", "init", ...], ...)`` -- and `test_promotion_gate.py`
builds its setup commands in a loop::

    for cmd in (["git", "init", "-q", "-b", "main"], ["git", "config", ...]):
        subprocess.run(cmd, cwd=tmp_path, ...)

So the scan **missed the `git init` line** -- the one that starts the damage -- and flagged the file
only via the two literal calls after it. The file was caught, and it was caught **by luck**: a file
that used the loop form throughout would have passed.

⚠️ **That is the same shape as a probe that reads a frozen series and calls it a pass.** In both
cases the anti-vacuity floor is what turns luck into a measurement, which is why
``test_the_scan_finds_the_calls_it_is_supposed_to_guard`` exists and why it asserts a number rather
than "found something".

The matcher therefore treats a bare ``Name`` argument as a candidate in any module that builds git
argv literally somewhere. Over-inclusive on purpose: the remedy is one keyword, and the failure it
prevents rewrote a branch.
"""

from __future__ import annotations

import ast
from pathlib import Path

TESTS = Path(__file__).resolve().parents[1]

#: Subcommands that WRITE. A read (`rev-parse`, `show`) against the wrong repository gives a wrong
#: answer; these change it. The guard covers writes, because that is the class that cost a branch.
_MUTATING = {"init", "add", "commit", "checkout", "reset", "config", "push", "rm", "mv"}


def _string_elements(node: ast.AST) -> list[str]:
    """Literal strings in a list/tuple argument, ignoring f-strings and names."""
    if not isinstance(node, ast.List | ast.Tuple):
        return []
    return [e.value for e in node.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)]


def _mutating_git_calls(tree: ast.AST) -> list[ast.Call]:
    """Every `subprocess.*([... "git" ... "<write subcommand>" ...])` call in a module.

    Found by reading the argument list, not by matching a helper's name: a file that wraps its
    calls in `def git(...)` is still doing the thing, and a file that names a helper `git_safe`
    is not automatically safe.
    """
    # A module that builds its argv in a loop -- `for cmd in (["git", "init", ...], ...):
    # subprocess.run(cmd, ...)` -- passes a Name, not a literal, so the literal check below
    # cannot see it. That is exactly the idiom test_promotion_gate.py uses for `git init`, and
    # the first version of this scan missed that line while catching the two after it.
    #
    # ⚠️ A file caught only by its later literal calls is caught by luck. So: if the module
    # contains ANY literal git-mutating argv, every subprocess call in it that passes a bare
    # Name is treated as a candidate too. Over-inclusive on purpose -- the remedy is one
    # keyword, and the failure it prevents rewrote a branch.
    module_builds_git_argv = any(
        "git" in _string_elements(n) and _MUTATING.intersection(_string_elements(n))
        for n in ast.walk(tree)
    )

    calls: list[ast.Call] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name not in {"run", "check_output", "check_call", "Popen"}:
            continue
        argv = _string_elements(node.args[0])
        literal_git = "git" in argv and bool(_MUTATING.intersection(argv))
        built_elsewhere = module_builds_git_argv and isinstance(node.args[0], ast.Name)
        if literal_git or built_elsewhere:
            calls.append(node)
    return calls


def _scans() -> list[tuple[Path, str, ast.AST]]:
    out = []
    for path in sorted(TESTS.rglob("test_*.py")):
        src = path.read_text(encoding="utf-8")
        out.append((path, src, ast.parse(src)))
    return out


def test_the_scan_finds_the_calls_it_is_supposed_to_guard():
    """Anti-vacuity. A scan that matches nothing passes every assertion below.

    This is the failure mode the guard is about, one level up: `test_promotion_gate.py`'s fixture
    was invisible to every existing check, so "nothing flagged it" meant nothing.
    """
    total = sum(len(_mutating_git_calls(tree)) for _, _, tree in _scans())
    assert total >= 4, (
        f"the scan found only {total} mutating git subprocess calls across tests/. It is supposed "
        "to find the repo-building fixtures; if the count collapsed, the matcher stopped matching "
        "and every assertion in this file is now vacuous."
    )


def test_every_mutating_git_subprocess_passes_an_explicit_env():
    """The fix, stated as a requirement on all future files rather than on the four that exist."""
    offenders: list[str] = []
    for path, _src, tree in _scans():
        for call in _mutating_git_calls(tree):
            if not any(kw.arg == "env" for kw in call.keywords):
                offenders.append(f"{path.relative_to(TESTS.parent)}:{call.lineno}")
    assert not offenders, (
        "these git subprocess calls inherit the caller's environment:\n  "
        + "\n  ".join(offenders)
        + "\n\ngit exports GIT_DIR to its hooks, and the pre-push hook runs tests/test_deploy. "
        "Under it, `cwd=tmp_path` isolates the directory and nothing isolates the repository: "
        "`git init` re-initialises the REAL repo and the next `git add -A` records every tracked "
        "file as deleted. Measured on 2026-09-12 (core#1307). Pass env=<GIT_* stripped>."
    )


def test_the_files_doing_this_actually_strip_git_variables():
    """`env=` alone is not the fix -- `env=os.environ` passes the keyword and changes nothing.

    Asserts the presence of the scrub itself, in any file that mutates a repository.
    """
    missing: list[str] = []
    for path, src, tree in _scans():
        if not _mutating_git_calls(tree):
            continue
        if 'startswith("GIT_")' not in src and "startswith('GIT_')" not in src:
            missing.append(str(path.relative_to(TESTS.parent)))
    assert not missing, (
        "these files build or mutate a git repository but never strip GIT_* from the environment "
        f"they hand the subprocess: {missing}. Passing `env=os.environ` satisfies the keyword and "
        "reproduces the defect exactly."
    )

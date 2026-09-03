"""core#983 — every credential the smoke suite requires must be named in the nightly.

## Why this exists

Until core#983 the nightly connector smoke received its credentials as a single
base64 bundle: one secret, decoded at runtime into ``$GITHUB_ENV``. Adding a
credential meant re-encoding the bundle and nothing else — the workflow file never
changed, so the credential surface of a public repository lived entirely outside
version control.

Per-key secrets fix the exposure problem (GitHub masks a registered secret
natively) and introduce a bookkeeping one in its place: the workflow must now name
each key, so **a credential added to a test and forgotten in the workflow is a
silent omission**. core#983 accepted that cost on the grounds that it fails loudly
— ``_require_env`` raises rather than skips, so the nightly goes red.

A red nightly is a real signal and it is the *wrong* one to rely on:

* it arrives up to twenty-four hours after the merge that caused it;
* it arrives on a scheduled run, which executes the **default branch's** copy of
  the workflow, so a fix merged to ``dev`` does not change what the schedule runs
  (WORKFLOW_RULES §7); and
* the failure it produces — ``Missing env vars: FOO`` — is indistinguishable from
  a credential that expired overnight, which is what the nightly is *for*.

This guard moves that from a red nightly to a red pull request, and separates
"we forgot to plumb it" from "the vendor revoked it".

## What it does NOT assert, deliberately

The reverse direction — *every name in the workflow is read by some test* — is
**not** asserted. It would be a real tidiness win (the workflow currently names
exactly the 19 values the suite consumes, down from the ~41 the bundle carried),
but it forbids landing the workflow half of a new connector before its test, which
is a legitimate order. Tidiness is not worth a rule that fights a correct sequence.

## Scope note, which is easy to get wrong

The ``smoke`` job runs the whole ``tests/test_connector_smoke/`` directory, which
collects the Oracle tests too — and those need ``ORACLE_*``, supplied by the
*other* job (``oracle-smoke``) via a ``$GITHUB_ENV`` write of connection
constants. So the assertion is "provided somewhere in this workflow", not
"provided by the smoke job". Narrowing it to the one job would make it red today
for a condition that is pre-existing and not a plumbing bug.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "nightly-connector-smoke.yml"
SMOKE_TESTS = REPO_ROOT / "tests" / "test_connector_smoke"

# `{ echo "ORACLE_HOST=localhost"; ... } >> "$GITHUB_ENV"` — a name defined by the
# workflow itself rather than by a secret or variable.
ECHOED_NAME = re.compile(r"""echo\s+["']?([A-Z][A-Z0-9_]*)=""")
GITHUB_ENV_WRITE = re.compile(r">>\s*\"?\$(\{)?GITHUB_ENV")


def required_env_names(directory: Path) -> set[str]:
    """Env var names the smoke suite demands, read by AST from ``require_env`` calls.

    AST rather than a regex because a multi-line ``require_env(\n "A",\n "B",\n)``
    is the common spelling here and a line-oriented regex silently returns a
    subset — which in this guard would be a *pass*, since a smaller required set
    trivially satisfies the containment below.
    """
    names: set[str] = set()
    for path in sorted(directory.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name not in {"require_env", "_require_env"}:
                continue
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    names.add(arg.value)
    return names


def provided_env_names(workflow: Path) -> set[str]:
    """Every env name the workflow makes available: ``env:`` keys at any level, plus
    names it writes into ``$GITHUB_ENV`` itself."""
    data = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    names: set[str] = set()

    def add_env(obj: object) -> None:
        if isinstance(obj, dict):
            env = obj.get("env")
            if isinstance(env, dict):
                names.update(str(k) for k in env)

    add_env(data)
    for job in (data.get("jobs") or {}).values():
        if not isinstance(job, dict):
            continue
        add_env(job)
        for step in job.get("steps") or []:
            if not isinstance(step, dict):
                continue
            add_env(step)
            run = str(step.get("run") or "")
            if GITHUB_ENV_WRITE.search(run):
                names.update(ECHOED_NAME.findall(run))
    return names


class TestNightlySmokeEnvParity:
    def test_the_derivation_is_armed(self) -> None:
        """Both halves must be non-empty and recognisable before the rule means anything.

        A containment assertion is satisfied by an empty left-hand side, so a broken
        AST walk or a workflow that stops parsing would make this file green while
        checking nothing — the exact failure mode core#983 asked this guard to avoid
        inheriting from the masking guard it sits beside.
        """
        assert WORKFLOW.is_file(), f"missing workflow: {WORKFLOW}"
        assert SMOKE_TESTS.is_dir(), f"missing smoke suite: {SMOKE_TESTS}"

        required = required_env_names(SMOKE_TESTS)
        assert len(required) >= 15, (
            f"only {len(required)} required env name(s) derived from {SMOKE_TESTS}: "
            f"{sorted(required)}. The AST walk found little or nothing, so the "
            "containment test below cannot fail."
        )
        # Two names from opposite ends of the split: one masked credential, one
        # visible constant. Both are read through `require_env`, so either
        # disappearing means the derivation stopped seeing real call sites.
        for anchor in ("STRIPE_API_KEY", "SHOPIFY_API_VERSION"):
            assert anchor in required, (
                f"{anchor} is read by the smoke suite but the derivation missed it: "
                f"{sorted(required)}"
            )

        provided = provided_env_names(WORKFLOW)
        assert len(provided) >= 20, (
            f"only {len(provided)} env name(s) parsed out of {WORKFLOW.name}: {sorted(provided)}"
        )

    def test_every_required_credential_is_named_in_the_workflow(self) -> None:
        required = required_env_names(SMOKE_TESTS)
        provided = provided_env_names(WORKFLOW)
        missing = sorted(required - provided)
        assert not missing, (
            f"{len(missing)} env var(s) are required by tests/test_connector_smoke/ and "
            f"named nowhere in {WORKFLOW.name}: {missing}.\n\n"
            "Since core#983 the nightly names each credential explicitly as "
            "`${{ secrets.X }}` or `${{ vars.Y }}` — there is no bundle to fall back on. "
            "An unnamed variable resolves to the empty string, `_require_env` fails, and "
            "the nightly goes red about twenty-four hours from now against the DEFAULT "
            "BRANCH's copy of this workflow.\n\n"
            "Decide the destination by TODAY'S EXPOSURE, not by whether it looks like a "
            "credential: masked in the logs today => `secrets.`, visible today => `vars.`. "
            "A value that names the account a credential opens (a host, a shop domain, a "
            "workspace id) is masked today and must stay a secret — see cloud#153."
        )

    def test_the_containment_check_can_actually_fail(self, tmp_path: Path) -> None:
        """Negative control against the REAL workflow, not a hand-written fixture.

        A synthetic control is written from the same mental model as the check and
        agrees with it including where it is wrong. So: take the shipped workflow,
        delete one required key from the smoke step's env block, and confirm the
        rule notices. The mutation is byte-level on a copy; nothing in the work tree
        is touched.
        """
        victim = "STRIPE_API_KEY"
        original = WORKFLOW.read_text(encoding="utf-8")
        assert f"{victim}: " in original, f"{victim} is not in the workflow to remove"

        mutated = "\n".join(
            line for line in original.splitlines() if not line.strip().startswith(f"{victim}:")
        )
        copy = tmp_path / "mutated.yml"
        copy.write_text(mutated, encoding="utf-8")

        assert victim not in provided_env_names(copy), "the mutation did not remove the key"
        missing = required_env_names(SMOKE_TESTS) - provided_env_names(copy)
        assert victim in missing, (
            "Removing a required credential from the workflow did not make it show up as "
            "missing, so this guard cannot detect the thing it exists for."
        )

    def test_a_name_supplied_by_the_workflow_itself_counts_as_provided(self) -> None:
        """The Oracle constants come from a ``$GITHUB_ENV`` write, not from a secret.

        Permissive control: without it the obvious implementation — read ``env:``
        keys only — is red today for five ORACLE_* names that are correctly plumbed,
        and the natural repair is to add them as secrets, which would register five
        non-secret constants and mask ``1521`` across the whole log.
        """
        provided = provided_env_names(WORKFLOW)
        for name in ("ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE"):
            assert name in provided, (
                f"{name} is written into $GITHUB_ENV by the oracle-smoke job but was not "
                "counted as provided; the run-block half of the derivation is broken."
            )

    @pytest.mark.parametrize(
        "source, expected",
        [
            ('env = require_env("A_ONE", "A_TWO")', {"A_ONE", "A_TWO"}),
            ('env = require_env(\n    "B_ONE",\n    "B_TWO",\n)', {"B_ONE", "B_TWO"}),
            ("env = require_env(*names)", set()),
            ('env = other_helper("C_ONE")', set()),
        ],
    )
    def test_the_ast_walk_handles_the_real_call_spellings(
        self, tmp_path: Path, source: str, expected: set[str]
    ) -> None:
        """The multi-line case is the one that matters — it is how this suite writes them.

        A line-oriented regex returns a subset for the multi-line spelling, and a
        subset makes the containment assertion *pass*. That is a guard whose own
        breakage produces the reassuring answer.
        """
        (tmp_path / "test_probe.py").write_text(source, encoding="utf-8")
        assert required_env_names(tmp_path) == expected

"""core#943 — a secret decoded into ``$GITHUB_ENV`` must be re-registered with ``::add-mask::``.

## The defect this guards

``.github/workflows/nightly-connector-smoke.yml`` decodes a base64 credential bundle and
appends every ``KEY=VALUE`` line to ``$GITHUB_ENV``.

GitHub Actions masks values it has **registered**. It registers the bundle secret
(``QA_CONNECTOR_CREDENTIALS``) and masks *that exact string*. The individual values decoded
out of it are **different strings, manufactured at runtime**, so the runner has never heard of
them and never masks them. Once such a value is in ``$GITHUB_ENV`` it joins the job
environment, and the runner prints the whole ``env:`` map in the ``##[group]Run …`` header of
every later step — so each credential is echoed in cleartext several times per run.

Measured 2026-09-02 on this repo, which is **public**: 53 still-downloadable Actions logs
carried a live vendor PAT verbatim, the oldest from 2026-06-05. The operational half of that
(rotation, log disposition) is tracked privately; this guard is the part that keeps it fixed.

**The proof it is a masking failure rather than masking being off** is that one line of one log
shows both behaviours at once — the registered bundle renders as ``***`` while a value decoded
from that same bundle renders in full.

## Why the predicate is narrow, and why that is the whole design

A guard that said *"never write to ``$GITHUB_ENV``"* would be wrong, and a guard that said
*"mask everything written to ``$GITHUB_ENV``"* would be worse than wrong — it would demand
masking of values like ``localhost``, ``1521`` or ``1``, and ``::add-mask::1`` redacts **every
digit 1 in the log**, which destroys the log while reporting success.

So the predicate is: a step that **both** takes a ``${{ secrets.* }}`` into its ``env:`` **and**
writes to ``$GITHUB_ENV``. That is the shape where a registered secret becomes an unregistered
one. ``oracle-connector-smoke.yml`` writes hardcoded throwaway localhost credentials to
``$GITHUB_ENV`` and takes no secret, so it is correctly *not* flagged — and
``test_the_predicate_does_not_flag_a_secretless_env_write`` pins that, because a predicate that
flags everything is not a predicate.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

SECRET_REF = re.compile(r"\$\{\{\s*secrets\.", re.IGNORECASE)
GITHUB_ENV_WRITE = re.compile(r">>\s*\"?\$(\{)?GITHUB_ENV")
ADD_MASK = "::add-mask::"


def _steps(workflow: Path):
    """Yield ``(job_id, step_index, step_name, env_text, run_text)`` for every step."""
    data = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return
    for job_id, job in (data.get("jobs") or {}).items():
        if not isinstance(job, dict):
            continue
        for i, step in enumerate(job.get("steps") or []):
            if not isinstance(step, dict):
                continue
            run = step.get("run") or ""
            env = step.get("env") or {}
            if isinstance(env, dict):
                env_text = "\n".join(f"{k}={v}" for k, v in env.items())
            else:
                env_text = str(env)
            yield job_id, i, step.get("name") or f"step[{i}]", env_text, str(run)


def _materializers():
    """Steps that take a repository secret AND write to ``$GITHUB_ENV``.

    This is the exact shape in which a *registered* secret becomes an *unregistered* one.
    """
    out = []
    for wf in sorted(WORKFLOW_DIR.glob("*.yml")) + sorted(WORKFLOW_DIR.glob("*.yaml")):
        for job_id, idx, name, env_text, run in _steps(wf):
            if SECRET_REF.search(env_text) and GITHUB_ENV_WRITE.search(run):
                out.append((wf.name, job_id, idx, name, run))
    return out


class TestSecretsWrittenToGithubEnvAreMasked:
    def test_the_scan_finds_the_known_materializer_steps(self) -> None:
        """Arming check — without this the whole class passes vacuously.

        If the workflow is renamed, restructured, or the YAML stops parsing, every
        assertion below becomes an assertion about an empty list. A guard that cannot
        tell 'nothing is wrong' from 'I looked at nothing' is not a guard.
        """
        assert WORKFLOW_DIR.is_dir(), f"missing workflow dir: {WORKFLOW_DIR}"
        found = _materializers()
        assert len(found) >= 2, (
            "Expected at least the two credential-materialization steps in "
            f"nightly-connector-smoke.yml, found {len(found)}: "
            f"{[(f, n) for f, _, _, n, _ in found]}. Either the workflow changed shape "
            "or the scan is broken — do not read this as 'nothing to mask'."
        )
        files = {f for f, _, _, _, _ in found}
        assert "nightly-connector-smoke.yml" in files, (
            f"the nightly connector smoke is not among the scanned materializers: {files}"
        )

    def test_every_secret_materialized_into_github_env_is_masked(self) -> None:
        offenders = [
            (wf, name) for wf, _job, _i, name, run in _materializers() if ADD_MASK not in run
        ]
        assert not offenders, (
            f"{len(offenders)} step(s) decode a repository secret into $GITHUB_ENV without "
            f"calling {ADD_MASK!r} on the value first: {offenders}.\n\n"
            "GitHub masks the registered secret, not the values decoded out of it. Anything "
            "written to $GITHUB_ENV is then echoed in the `env:` block of every later step's "
            "log group — in cleartext, on a public repository. This is core#943."
        )

    def test_masking_happens_before_the_github_env_write(self) -> None:
        """Order matters: masking after the write still leaks the env echo of earlier steps.

        Registering the value late is not equivalent — the runner only redacts output
        produced *after* the command runs.
        """
        for wf, _job, _i, name, run in _materializers():
            if ADD_MASK not in run:
                continue  # covered by the test above
            mask_at = run.index(ADD_MASK)
            write = GITHUB_ENV_WRITE.search(run)
            assert write is not None
            assert mask_at < write.start(), (
                f"{wf} / {name!r}: {ADD_MASK} appears after the $GITHUB_ENV write. "
                "The value must be registered before it can be redacted."
            )

    def test_the_predicate_does_not_flag_a_secretless_env_write(self) -> None:
        """The permissive control — the one most likely to be missing.

        ``oracle-connector-smoke.yml`` writes hardcoded throwaway localhost credentials
        (``ORACLE_HOST=localhost``, ``ORACLE_PORT=1521``) into ``$GITHUB_ENV`` for an
        ephemeral Oracle XE container. Those must NOT be flagged: masking ``localhost`` or
        ``1521`` would redact those tokens throughout the log for no benefit, and a guard
        that demands it would be routed around rather than satisfied.

        If this test ever fails, the predicate has widened into 'anything touching
        $GITHUB_ENV' and the guard has stopped discriminating.
        """
        oracle = WORKFLOW_DIR / "oracle-connector-smoke.yml"
        if not oracle.is_file():
            pytest.skip("oracle-connector-smoke.yml not present")

        writes_env = [
            name for _job, _i, name, _env, run in _steps(oracle) if GITHUB_ENV_WRITE.search(run)
        ]
        assert writes_env, (
            "oracle-connector-smoke.yml no longer writes to $GITHUB_ENV, so this control "
            "no longer controls anything. Re-point it at another secretless writer."
        )
        flagged = [name for wf, _j, _i, name, _r in _materializers() if wf == oracle.name]
        assert not flagged, (
            f"The predicate flagged secretless $GITHUB_ENV writes in {oracle.name}: {flagged}. "
            "It has widened from 'a secret becomes unregistered' to 'anything writes env', "
            "which would demand ::add-mask::localhost."
        )


class TestTheGuardCanFail:
    """Negative controls. A check that has never failed has never been shown able to."""

    def test_the_pre_fix_run_block_is_rejected(self) -> None:
        """The exact pre-core#943 shape must fail the masking predicate."""
        pre_fix = (
            "printf '%s' \"$QA_CONNECTOR_CREDENTIALS\" | base64 -d > /tmp/creds.env\n"
            "while IFS= read -r line; do\n"
            "  case \"$line\" in ''|\\#*) continue ;; esac\n"
            '  line=$(echo "$line" | sed -E \'s/^([A-Z_][A-Z0-9_]*)="(.*)"$/\\1=\\2/\')\n'
            '  echo "$line" >> "$GITHUB_ENV"\n'
            "done < /tmp/creds.env\n"
        )
        assert GITHUB_ENV_WRITE.search(pre_fix), (
            "The matcher does not even recognise the pre-fix $GITHUB_ENV write, so the "
            "guard would have reported the original defect as absent. Fix the regex, not "
            "this assertion."
        )
        assert ADD_MASK not in pre_fix, "pre-fix sample unexpectedly contains a mask call"

    def test_the_matcher_recognises_both_github_env_spellings(self) -> None:
        """`>> "$GITHUB_ENV"` and `>> $GITHUB_ENV` and `${GITHUB_ENV}` all count.

        A matcher that only knows one spelling reports a leaking workflow as clean —
        which is the failure mode this whole file exists to prevent, one level up.
        """
        for sample in (
            'echo "K=v" >> "$GITHUB_ENV"',
            "echo K=v >> $GITHUB_ENV",
            'echo "K=v" >> "${GITHUB_ENV}"',
            '} >> "$GITHUB_ENV"',
        ):
            assert GITHUB_ENV_WRITE.search(sample), f"matcher missed a real write: {sample!r}"

    def test_the_secret_reference_matcher_is_not_trivially_true(self) -> None:
        """It must distinguish a secret reference from an ordinary env value."""
        assert SECRET_REF.search("QA_CONNECTOR_CREDENTIALS=${{ secrets.QA_CONNECTOR_CREDENTIALS }}")
        assert not SECRET_REF.search("DATANIKA_CONNECTOR_SMOKE=1")
        assert not SECRET_REF.search("ORACLE_HOST=localhost")
        assert not SECRET_REF.search("TOKEN=${{ github.token }}")

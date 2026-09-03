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
SECRET_DEREF = "\\$\\{?%s\\b"


def _env_map(obj) -> dict:
    env = obj.get("env") or {}
    return env if isinstance(env, dict) else {}


def _secret_bound_names(env: dict) -> set:
    """Variable names bound to a ``${{ secrets.* }}`` expression."""
    return {str(k) for k, v in env.items() if SECRET_REF.search(str(v))}


def _steps(workflow: Path):
    """Yield ``(job_id, step_index, step_name, env_text, run_text)`` for every step."""
    data = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return
    wf_secrets = _secret_bound_names(_env_map(data))
    for job_id, job in (data.get("jobs") or {}).items():
        if not isinstance(job, dict):
            continue
        inherited = wf_secrets | _secret_bound_names(_env_map(job))
        for i, step in enumerate(job.get("steps") or []):
            if not isinstance(step, dict):
                continue
            run = step.get("run") or ""
            env = step.get("env") or {}
            if isinstance(env, dict):
                env_text = "\n".join(f"{k}={v}" for k, v in env.items())
            else:
                env_text = str(env)
            yield job_id, i, step.get("name") or f"step[{i}]", env_text, str(run), inherited


def _materializers(workflow_dir: Path | None = None):
    """Steps that write to ``$GITHUB_ENV`` with a secret IN SCOPE.

    In scope means either the secret sits in the step's own ``env:``, or the
    ``run:`` dereferences a variable bound to a secret at **job** or **workflow**
    level.

    The second half is the core#943 follow-up. The original predicate read only
    ``step["env"]``, so hoisting the secret one level up -- the ordinary refactor
    when two steps need the same value -- left the predicate's scope while the
    leak was byte-identical. Measured before this widened: a job-level and a
    workflow-level probe each passed 7/7 against an unmasked ``$GITHUB_ENV``
    write, while the step-level probe was correctly caught.

    It is name-based rather than 'any secret anywhere in the job' deliberately: a
    job may hold a secret for one step while an unrelated step writes a
    non-secret to ``$GITHUB_ENV``. Flagging that would push the guard back toward
    'anything touching env', which the permissive control below exists to stop.

    ``workflow_dir`` is injectable so the probes can scan a synthetic tree; it
    defaults to this repository's real workflows.
    """
    directory = WORKFLOW_DIR if workflow_dir is None else workflow_dir
    out = []
    for wf in sorted(directory.glob("*.yml")) + sorted(directory.glob("*.yaml")):
        for job_id, idx, name, env_text, run, inherited in _steps(wf):
            if not GITHUB_ENV_WRITE.search(run):
                continue
            direct = bool(SECRET_REF.search(env_text))
            via_inherited = any(re.search(SECRET_DEREF % re.escape(n), run) for n in inherited)
            if direct or via_inherited:
                out.append((wf.name, job_id, idx, name, run))
    return out


class TestSecretsWrittenToGithubEnvAreMasked:
    """core#983 moved the arming check, and the move is the interesting part.

    This class used to arm itself by finding the two credential-materialization
    steps in ``nightly-connector-smoke.yml``: *"if I can see the known offenders,
    I can see offenders."* core#983 deleted those steps — the bundle was replaced
    by per-key secrets that GitHub masks natively — so the repository now has
    **zero** materializers, which is the goal and also exactly what a broken scan
    reports.

    🚨 The obvious repair is to relax the count to ``>= 0``. That would delete the
    guard's ability to detect its own blindness while leaving every assertion
    green, which is this project's signature defect. Instead the arming check is
    **split into the two properties it was conflating**:

    1. :meth:`test_the_scan_can_still_detect_a_materializer` — detection works,
       proven against the pre-fix shape on a synthetic tree. Independent of
       whether the real tree happens to contain an offender today.
    2. :meth:`test_the_real_workflow_tree_is_genuinely_scanned` — the real tree
       is being read: files found, YAML parsed, steps extracted, and the
       ``$GITHUB_ENV`` matcher still firing on a real file. So an empty offender
       list means *scanned and clean*, never *looked at nothing*.

    Neither half is sufficient alone, which is why 1 could not simply be dropped
    in favour of 2 (the scan could read every file and still never match) nor 2 in
    favour of 1 (the predicate could work perfectly on a tmp_path it was handed
    while pointed at a directory that no longer exists).
    """

    def test_the_scan_can_still_detect_a_materializer(self, tmp_path: Path) -> None:
        """Arming half 1 — the predicate can still see the thing it hunts.

        Uses the *pre-fix* shape this guard was written against (core#943): a
        secret decoded out of a bundle and appended to ``$GITHUB_ENV`` with no
        ``::add-mask::``. If this stops being flagged, the predicate is broken and
        every 'no offenders' result below is meaningless.
        """
        found = _scan(tmp_path, _JOB_LEVEL_UNMASKED)
        assert [n for _wf, _j, _i, n, _r in found] == ["Materialize"], (
            "The predicate no longer detects the pre-fix materializer shape. Until "
            "this passes, 'zero offenders in the real tree' is not evidence of "
            f"anything. Got: {found}"
        )
        offenders = [n for _wf, _j, _i, n, run in found if ADD_MASK not in run]
        assert offenders == ["Materialize"], (
            "The predicate found the step but did not classify the unmasked write as "
            "an offender, so the rule below cannot fire either."
        )

    def test_the_real_workflow_tree_is_genuinely_scanned(self) -> None:
        """Arming half 2 — 'no offenders' must mean scanned, not skipped.

        Deliberately asserts on *positive artifacts of the scan* rather than on
        the absence of offenders: files discovered, steps extracted, and at least
        one real ``$GITHUB_ENV`` write still matched. A moved directory, a
        renamed workflow or YAML that stopped parsing all drive these to zero,
        and each of them would otherwise read as a clean repository.
        """
        assert WORKFLOW_DIR.is_dir(), f"missing workflow dir: {WORKFLOW_DIR}"
        workflows = sorted(WORKFLOW_DIR.glob("*.yml")) + sorted(WORKFLOW_DIR.glob("*.yaml"))
        assert len(workflows) >= 8, (
            f"only {len(workflows)} workflow file(s) under {WORKFLOW_DIR} — the scan is "
            "pointed somewhere thin or the tree moved."
        )

        steps = [s for wf in workflows for s in _steps(wf)]
        assert len(steps) >= 40, (
            f"only {len(steps)} step(s) parsed across {len(workflows)} workflows. YAML that "
            "fails to parse yields an empty generator, which is indistinguishable from a "
            "repository with nothing to check."
        )

        env_writers = {
            wf.name
            for wf in workflows
            for _j, _i, _n, _e, run, _inh in _steps(wf)
            if GITHUB_ENV_WRITE.search(run)
        }
        assert env_writers, (
            "No step anywhere in the real tree writes to $GITHUB_ENV. Either the matcher "
            f"({GITHUB_ENV_WRITE.pattern!r}) has stopped working or every writer was "
            "removed — the first is a broken guard, the second is worth knowing."
        )
        assert "nightly-connector-smoke.yml" in env_writers, (
            "nightly-connector-smoke.yml no longer contributes any $GITHUB_ENV write. Its "
            "oracle-smoke job writes ORACLE_* connection constants, and that write is what "
            "proves this file is still being parsed by this scan. Found writers in: "
            f"{sorted(env_writers)}"
        )

    def test_every_secret_materialized_into_github_env_is_masked(self) -> None:
        """The rule itself.

        ⚠️ Since core#983 the expected offender count in this repository is **zero
        out of zero materializers** — the bundle-decoding pattern is gone entirely
        and native masking replaced it. That is a vacuous pass by construction, and
        it is only meaningful because the two arming tests above hold. Do not read
        a green here as evidence on its own; read it together with them.
        """
        offenders = [
            (wf, name) for wf, _job, _i, name, run in _materializers() if ADD_MASK not in run
        ]
        assert not offenders, (
            f"{len(offenders)} step(s) decode a repository secret into $GITHUB_ENV without "
            f"calling {ADD_MASK!r} on the value first: {offenders}.\n\n"
            "GitHub masks the registered secret, not the values decoded out of it. Anything "
            "written to $GITHUB_ENV is then echoed in the `env:` block of every later step's "
            "log group — in cleartext, on a public repository. This is core#943.\n\n"
            "Prefer per-key secrets over a bundle (core#983): a value registered as its own "
            "secret is masked natively and no code of ours has the opportunity to get it wrong."
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
            name
            for _job, _i, name, _env, run, _inh in _steps(oracle)
            if GITHUB_ENV_WRITE.search(run)
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


_JOB_LEVEL_UNMASKED = """
name: probe
on: {workflow_dispatch: {}}
jobs:
  leak:
    runs-on: ubuntu-latest
    env: {BUNDLE: "${{ secrets.SOME_BUNDLE }}"}
    steps:
      - name: Materialize
        run: |
          printf '%s' "$BUNDLE" | base64 -d > /tmp/c.env
          while IFS= read -r l; do echo "$l" >> "$GITHUB_ENV"; done < /tmp/c.env
"""

_WORKFLOW_LEVEL_UNMASKED = """
name: probe
on: {workflow_dispatch: {}}
env: {BUNDLE: "${{ secrets.SOME_BUNDLE }}"}
jobs:
  leak:
    runs-on: ubuntu-latest
    steps:
      - name: Materialize
        run: |
          printf '%s' "$BUNDLE" | base64 -d > /tmp/c.env
          while IFS= read -r l; do echo "$l" >> "$GITHUB_ENV"; done < /tmp/c.env
"""

_JOB_SECRET_UNRELATED_WRITE = """
name: probe
on: {workflow_dispatch: {}}
jobs:
  fine:
    runs-on: ubuntu-latest
    env: {BUNDLE: "${{ secrets.SOME_BUNDLE }}"}
    steps:
      - name: Write a constant
        run: echo "ORACLE_PORT=1521" >> "$GITHUB_ENV"
      - name: Use the secret without touching the env file
        run: printf '%s' "$BUNDLE" | md5sum
"""

_JOB_LEVEL_MASKED = """
name: probe
on: {workflow_dispatch: {}}
jobs:
  ok:
    runs-on: ubuntu-latest
    env: {BUNDLE: "${{ secrets.SOME_BUNDLE }}"}
    steps:
      - name: Materialize
        run: |
          printf '%s' "$BUNDLE" | base64 -d > /tmp/c.env
          while IFS= read -r l; do
            v=${l#*=}
            echo "::add-mask::$v"
            echo "$l" >> "$GITHUB_ENV"
          done < /tmp/c.env
"""


def _scan(tmp_path: Path, yaml_text: str):
    """Run the real predicate over a synthetic one-workflow tree."""
    d = tmp_path / ".github" / "workflows"
    d.mkdir(parents=True, exist_ok=True)
    (d / "probe.yml").write_text(yaml_text, encoding="utf-8")
    return _materializers(d)


class TestSecretsInheritedFromJobOrWorkflowLevel:
    """core#943 follow-up: the leak is the same wherever the secret is declared.

    These four shapes were run against the pre-fix predicate first. The two
    unmasked hoisted shapes passed -- i.e. the guard covered its own instance and
    not its kind, which is the defect class this repository keeps rediscovering.
    """

    def test_a_job_level_secret_written_unmasked_is_caught(self, tmp_path: Path) -> None:
        found = _scan(tmp_path, _JOB_LEVEL_UNMASKED)
        assert [n for _wf, _j, _i, n, _r in found] == ["Materialize"], (
            "A secret declared in the JOB's env: and decoded into $GITHUB_ENV without "
            "masking was not flagged. This is byte-identical in effect to the step-level "
            "shape core#943 fixed; only the declaration site moved."
        )

    def test_a_workflow_level_secret_written_unmasked_is_caught(self, tmp_path: Path) -> None:
        found = _scan(tmp_path, _WORKFLOW_LEVEL_UNMASKED)
        assert [n for _wf, _j, _i, n, _r in found] == ["Materialize"], (
            "A secret declared in the WORKFLOW's top-level env: and decoded into "
            "$GITHUB_ENV without masking was not flagged."
        )

    def test_a_job_secret_unrelated_to_the_env_write_is_not_flagged(self, tmp_path: Path) -> None:
        """Permissive control for the widening — the half most likely to be skipped.

        A job may legitimately hold a secret for one step while a different step
        writes a non-secret constant to $GITHUB_ENV. Flagging that would recreate
        'mask everything', which is worse than the bug.
        """
        assert _scan(tmp_path, _JOB_SECRET_UNRELATED_WRITE) == [], (
            "The widened predicate flagged a step that writes a hardcoded constant to "
            "$GITHUB_ENV merely because its job holds an unrelated secret. It has "
            "widened from 'a secret becomes unregistered' to 'any env write in a job "
            "that has a secret'."
        )

    def test_a_correctly_masked_job_level_secret_is_flagged_but_satisfies_the_rule(
        self, tmp_path: Path
    ) -> None:
        """It is still a materializer — it just masks first, so the rule is met."""
        found = _scan(tmp_path, _JOB_LEVEL_MASKED)
        assert [n for _wf, _j, _i, n, _r in found] == ["Materialize"]
        offenders = [n for _wf, _j, _i, n, run in found if ADD_MASK not in run]
        assert not offenders, "a correctly masked hoisted secret must not be an offender"


NIGHTLY = WORKFLOW_DIR / "nightly-connector-smoke.yml"

# core#983's classification, pinned. The rule that produced it is **today's
# exposure**, not "does it look like a credential":
#   masked in the logs today  => secrets.  (no new exposure)
#   visible in the logs today => vars.     (no new masking)
#
# The right-hand column is the reason the value cannot move, so that a future
# edit has to argue with a measurement rather than with a preference.
MUST_STAY_SECRET = {
    "ASANA_ACCESS_TOKEN": "PAT; publicly logged for ~89 days before core#943",
    "DATABRICKS_HOST": "names the workspace the token opens (cloud#153)",
    "DATABRICKS_TOKEN": "dapi… PAT",
    "FRESHDESK_API_KEY": "API key",
    "FRESHDESK_DOMAIN": "names the account the key opens (cloud#153)",
    "HUBSPOT_ACCESS_TOKEN": "PAT",
    "KAFKA_BOOTSTRAP_SERVERS": "names the cluster the SASL password opens",
    "KAFKA_SASL_PASSWORD": "password",
    "PIPEDRIVE_API_TOKEN": "PAT",
    "SHOPIFY_ACCESS_TOKEN": "PAT",
    "SHOPIFY_SHOP_DOMAIN": "names the store the token opens (cloud#153)",
    "STRIPE_API_KEY": "rk_test_… key",
}
MUST_STAY_VARIABLE = {
    "BIGQUERY_CREDENTIALS_FILE": "a PATH, and a literal already in this workflow's source",
    "GA4_CREDENTIALS_FILE": "same path as above; masking it redacts the write's own log line",
    "BIGQUERY_PROJECT_ID": "8 chars — the project name, which appears in nearly every path",
    "GA4_PROPERTY_ID": "9 digits",
    "KAFKA_SASL_USERNAME": "visible today",
    "KAFKA_TOPIC": "visible today",
    "SHOPIFY_API_VERSION": "7 chars, of the form 2026-04",
}


def _secret_expr(name: str) -> str:
    """``${{ secrets.NAME }}``, built by concatenation.

    Not an f-string and not ``%``: the literal contains the braces GitHub's
    expression syntax uses, so an f-string needs them quadrupled and ruff rejects
    the percent form (UP031). Concatenation is the spelling that stays readable.
    """
    return "${{ secrets." + name + " }}"


def _vars_expr(name: str) -> str:
    return "${{ vars." + name + " }}"


def _nightly_smoke_env() -> dict:
    data = yaml.safe_load(NIGHTLY.read_text(encoding="utf-8"))
    for step in data["jobs"]["smoke"]["steps"]:
        if isinstance(step, dict) and step.get("name") == "Run connector smoke tests":
            return dict(step.get("env") or {})
    raise AssertionError("the 'Run connector smoke tests' step is gone — re-derive this test")


class TestPerKeyRegistrationDoesNotOverMask:
    """Replaces the executable half of ``test_workflow_secret_masking_executes.py``.

    🚨 **core#983 deleted that file, and this class exists so the deletion is not a
    silent loss of coverage.** That module executed the real ``::add-mask::`` loop
    under bash and asserted both directions: that credentials were registered, and
    — the half its own docstring called load-bearing — that ``datanika`` (8 chars),
    ``SASL_SSL`` and ``2024-01`` were **not**, because ``::add-mask::X`` redacts
    every occurrence of ``X`` and masking an ordinary word destroys the log while
    looking like the strongest possible security posture.

    Per-key registration deletes that loop, so those six tests had no subject: a
    repo-wide scan finds **zero** materializers. But the *hazard* did not go away —
    it moved from a runtime decision to a registration decision. Registering
    ``SHOPIFY_API_VERSION`` as a secret rather than a variable masks ``2026-04``
    across every log in the repository, and it is the exact change a well-meaning
    "make the credentials more secure" edit would make.

    So the assertion moves with it: this is a **pin on the classification**, not a
    re-implementation of the loop. It is deliberately a restatement of the
    workflow — that is what makes a change to it require an argument.
    """

    def test_the_env_block_is_parsed_and_populated(self) -> None:
        """Arming check: a containment assertion over an empty dict passes."""
        env = _nightly_smoke_env()
        assert len(env) >= 20, f"only {len(env)} env keys on the smoke step: {sorted(env)}"
        assert env.get("DATANIKA_CONNECTOR_SMOKE") == "1", (
            "the smoke gate is not set on this step — every probe would skip at "
            "collection time and pytest would still exit 0"
        )

    def test_credentials_are_registered_as_secrets(self) -> None:
        env = _nightly_smoke_env()
        wrong = {k: env.get(k) for k in MUST_STAY_SECRET if env.get(k) != _secret_expr(k)}
        assert not wrong, (
            f"{len(wrong)} credential(s) are not bound to a secret: {wrong}.\n\n"
            "Each is masked in today's logs. Moving one to `vars.` publishes it on a "
            "PUBLIC repository's Actions logs. Reasons per key: "
            f"{ {k: MUST_STAY_SECRET[k] for k in wrong} }"
        )

    def test_configuration_constants_are_not_registered_as_secrets(self) -> None:
        """The load-bearing direction, inherited from the deleted executes-guard."""
        env = _nightly_smoke_env()
        wrong = {k: env.get(k) for k in MUST_STAY_VARIABLE if env.get(k) != _vars_expr(k)}
        assert not wrong, (
            f"{len(wrong)} configuration value(s) are not bound to a variable: {wrong}.\n\n"
            "Registering one of these as a secret makes GitHub redact every occurrence of "
            "its value across the whole log — `2026-04`, a 9-digit id, an 8-character "
            "project name — which destroys the log while every check still reports "
            "success. Masking is not free and this is the direction that has no alarm. "
            f"Reasons per key: { {k: MUST_STAY_VARIABLE[k] for k in wrong} }"
        )

    def test_the_classification_pin_can_fail(self, tmp_path: Path) -> None:
        """Negative control on the REAL workflow: promote one constant to a secret."""
        victim = "SHOPIFY_API_VERSION"
        text = NIGHTLY.read_text(encoding="utf-8")
        mutated = text.replace(
            f"{victim}: {_vars_expr(victim)}", f"{victim}: {_secret_expr(victim)}"
        )
        assert mutated != text, f"the mutation did not apply — {victim} is not bound to vars."
        copy = tmp_path / "mutated.yml"
        copy.write_text(mutated, encoding="utf-8")

        data = yaml.safe_load(copy.read_text(encoding="utf-8"))
        step = next(
            s
            for s in data["jobs"]["smoke"]["steps"]
            if isinstance(s, dict) and s.get("name") == "Run connector smoke tests"
        )
        env = dict(step.get("env") or {})
        wrong = [k for k in MUST_STAY_VARIABLE if env.get(k) != _vars_expr(k)]
        assert wrong == [victim], (
            "Promoting a configuration constant to a secret was not detected, so this "
            f"pin cannot catch the over-masking regression it exists for. Got: {wrong}"
        )

"""core#943 — run the REAL materialization step and see what it masks.

``test_workflow_secret_masking.py`` asserts the workflow *text* calls ``::add-mask::``.
That is a necessary check and a weak one: it passes for any block that merely contains the
string. This module extracts the actual ``run:`` script out of the workflow YAML, executes it
under a real bash against a synthetic credential bundle, and reads back **both** outputs — the
``::add-mask::`` commands it emitted and the file it wrote as ``$GITHUB_ENV``.

That is the difference between "the fix is spelled correctly" and "the fix does the thing".
Same reasoning as the existing ``_run_sed`` harness in
``test_nightly_smoke_reports_failure.py``, which executes the quote-stripping expression
rather than pattern-matching it.

## The failure mode this is really guarding

Masking is not free. ``::add-mask::X`` redacts **every** occurrence of ``X`` in the log, so
masking a short or ordinary value silently destroys the log:

* the project name is 8 characters and appears in nearly every path in the build output;
* ``SASL_SSL``, ``1521``, ``2024-01`` are configuration, not credentials.

A guard that only checked "did we mask enough?" would be satisfied by a rule that masks
everything — which passes every leak test and produces an unreadable, unusable log while
looking like the strongest possible security posture. **The over-masking assertions below are
the load-bearing half of this file.**
"""

from __future__ import annotations

import base64
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "nightly-connector-smoke.yml"

# Realistic shapes. The two 8-character entries are the point: they are exactly the
# length at which a naive floor would start redacting ordinary words.
PROJECT_NAME = "datanika"  # 8 chars, appears in every path in a real build log
SYNTHETIC_BUNDLE = "\n".join(
    [
        "# a comment line, must be skipped",
        "",
        'DATABRICKS_TOKEN="dapi0123456789abcdef0123456789abcd"',  # 36, credential
        'STRIPE_API_KEY="rk_test_' + ("x" * 80) + '"',  # long, credential
        'KAFKA_SASL_PASSWORD="pw0123456789abcdef0123456789ab"',  # 30, credential
        f'BIGQUERY_PROJECT_ID="{PROJECT_NAME}"',  # 8, NOT a credential
        'KAFKA_SECURITY_PROTOCOL="SASL_SSL"',  # 8, NOT a credential
        'KAFKA_SASL_MECHANISM="SCRAM-SHA-256"',  # 13, NOT a credential
        'GA4_PROPERTY_ID="123456789"',  # 9, digit-bearing name (core#827)
        'SHOPIFY_API_VERSION="2024-01"',  # 7, NOT a credential
    ]
)

MASK_LINE = re.compile(r"^::add-mask::(.*)$", re.MULTILINE)


def _materialize_step_script() -> str:
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    for job in (data.get("jobs") or {}).values():
        for step in job.get("steps") or []:
            if isinstance(step, dict) and step.get("name") == "Materialize connector credentials":
                run = step.get("run")
                assert run, "the materialize step has no run: block"
                return str(run)
    raise AssertionError("'Materialize connector credentials' step not found in the workflow")


@pytest.fixture(scope="module")
def executed():
    """Run the real step; return (mask_values, github_env_text)."""
    bash = shutil.which("bash")
    if bash is None:  # pragma: no cover - CI always has bash
        pytest.skip("bash not available")

    script = _materialize_step_script()
    assert "$GITHUB_ENV" in script, "step no longer writes to $GITHUB_ENV — re-derive this test"

    workdir = tempfile.mkdtemp(prefix="qa943_")
    creds_path = os.path.join(workdir, "creds.env").replace("\\", "/")
    env_path = os.path.join(workdir, "github_env").replace("\\", "/")

    # ⚠️ One substitution, and only one: the step's hardcoded /tmp/creds.env becomes a
    # per-run path so two concurrent test sessions cannot clobber each other. The
    # masking logic under test is executed byte-identically.
    assert "/tmp/creds.env" in script, "the temp path changed; update this substitution"
    script = script.replace("/tmp/creds.env", creds_path)

    Path(env_path).write_bytes(b"")
    env = dict(os.environ)
    env["QA_CONNECTOR_CREDENTIALS"] = base64.b64encode(SYNTHETIC_BUNDLE.encode()).decode()
    env.pop("QA_BIGQUERY_SA_JSON", None)
    env["GITHUB_ENV"] = env_path

    script_file = os.path.join(workdir, "step.sh")
    # Binary write: a Python text round-trip would introduce CRLF and bash would read
    # `$'...\r'` into every value (WORKFLOW_RULES §3).
    Path(script_file).write_bytes(script.encode("utf-8"))

    proc = subprocess.run(
        [bash, "-e", script_file], cwd=workdir, env=env, capture_output=True, text=True
    )
    assert proc.returncode == 0, (
        f"the real step failed to execute: rc={proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    masked = MASK_LINE.findall(proc.stdout)
    written = Path(env_path).read_text(encoding="utf-8")

    # Arming check: if nothing reached $GITHUB_ENV the step did not run and every
    # assertion below would be vacuous.
    assert "DATABRICKS_TOKEN=" in written, (
        f"the step wrote nothing recognisable to $GITHUB_ENV; stdout:\n{proc.stdout}"
    )
    return masked, written


class TestCredentialsAreMasked:
    def test_the_long_credentials_are_registered_for_masking(self, executed) -> None:
        masked, _ = executed
        assert "dapi0123456789abcdef0123456789abcd" in masked, (
            f"the Databricks-shaped PAT was not masked. Masked values: {masked!r}"
        )
        assert any(v.startswith("rk_test_") for v in masked), "the Stripe-shaped key was not masked"
        assert "pw0123456789abcdef0123456789ab" in masked, "the SASL password was not masked"

    def test_every_value_of_credential_length_is_masked(self, executed) -> None:
        """The invariant, derived from the output rather than from a hand-written list."""
        masked, written = executed
        unmasked = [
            ln.split("=", 1)[1]
            for ln in written.splitlines()
            if "=" in ln and len(ln.split("=", 1)[1]) >= 16 and ln.split("=", 1)[1] not in masked
        ]
        assert not unmasked, (
            f"{len(unmasked)} value(s) of 16+ chars reached $GITHUB_ENV unmasked "
            "(lengths shown, values withheld): "
            f"{[len(v) for v in unmasked]}"
        )


class TestOrdinaryValuesAreNotMasked:
    """The over-masking half. A rule that masks everything passes every test above."""

    def test_the_project_name_is_not_masked(self, executed) -> None:
        masked, _ = executed
        assert PROJECT_NAME not in masked, (
            f"'{PROJECT_NAME}' was registered for masking. ::add-mask:: redacts every "
            "occurrence in the log, and the project name appears in nearly every path — "
            "this would replace most of the build output with ***, while every check "
            "still reported success."
        )

    def test_configuration_constants_are_not_masked(self, executed) -> None:
        masked, _ = executed
        for value in ("SASL_SSL", "SCRAM-SHA-256", "2024-01", "123456789"):
            assert value not in masked, (
                f"{value!r} was masked. It is configuration, not a credential; redacting it "
                "makes the log harder to read and buys nothing."
            )


class TestCore827BehaviourSurvives:
    """The masking edit sits inside the loop that core#270 / core#827 fixed."""

    def test_quotes_are_still_stripped_including_digit_bearing_names(self, executed) -> None:
        _, written = executed
        assert "GA4_PROPERTY_ID=123456789" in written, (
            "the digit-bearing name reached $GITHUB_ENV with quotes attached — core#827 "
            f"has regressed. Written:\n{written}"
        )
        assert '"' not in written, f"a literal quote survived into $GITHUB_ENV:\n{written}"

    def test_comments_and_blank_lines_are_still_skipped(self, executed) -> None:
        _, written = executed
        assert "# a comment" not in written, "a comment line reached $GITHUB_ENV"
        assert all(ln.strip() for ln in written.splitlines()), "a blank line reached $GITHUB_ENV"

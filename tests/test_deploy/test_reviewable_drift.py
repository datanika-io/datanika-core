"""A reviewable box override the repo does not declare is named, with its value.

datanika-io/datanika-cloud#141. The production env file is preserved by the deploy, so nothing
diffs it. The check reads the settings object through the container
interpreter (core#646). Secret-pattern values are never printed.

Two results are both required. A synthetic staging override exits 1 and the
output names the setting and the resolved value, which is the probe's value
rather than the env-file token. An unchanged file exits 0 and prints
``reviewable-drift 0``. Production invokes the same script with
``--report-only``, so flags already in the preserved file do not fail the
deploy.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "check-reviewable-drift.sh"
PROBE = ROOT / "scripts" / "read_reviewable_settings.py"
STAGING_DECL = ROOT / "deploy" / "reviewable-settings.staging.env"
PROD_DECL = ROOT / "deploy" / "reviewable-settings.prod.env"
DEPLOY = ROOT / ".github" / "workflows" / "deploy-pointer.yml"

PROD_DECLARED = {
    "DATANIKA_EDITION": "cloud",
    "DATANIKA_ALLOW_LOCAL_FILE_PATHS": "false",
    "DATANIKA_BYTES_QUOTA_ENFORCE": "true",
    "DATANIKA_OVERAGE_CHARGE_ENABLE": "true",
    "DATANIKA_E2E_ADMIN_ENABLE": "false",
    "PADDLE_ENVIRONMENT": "production",
}
STAGING_DECLARED = {
    "DATANIKA_EDITION": "cloud",
    "DATANIKA_ALLOW_LOCAL_FILE_PATHS": "true",
    "PADDLE_ENVIRONMENT": "sandbox",
}
MARKER = "zz-marker"
OVERRIDE = "DATANIKA_E2E_ADMIN_ENABLE"


def _bash() -> str:
    exe = shutil.which("bash")
    if exe is None:
        pytest.fail("bash not found; this suite must not silently stop testing the script")
    return exe


def _posix(path: Path) -> str:
    exe = shutil.which("cygpath")
    if exe:
        return subprocess.run(
            [exe, "-u", str(path)], capture_output=True, text=True, check=True
        ).stdout.strip()
    text = path.as_posix()
    if len(text) > 1 and text[1] == ":":
        return f"/{text[0].lower()}{text[2:]}"
    return text


def _write_exec(path: Path, body: str) -> None:
    path.write_bytes(body.encode("utf-8"))
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _parse_decl(text: str) -> dict[str, str]:
    found: dict[str, str] = {}
    for line in text.splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        found[name] = value
    return found


_FAKE = r"""#!/usr/bin/env bash
ST="__STATE__"
printf '%s\n' "$*" >> "$ST/calls.log"
case "$1" in
  exec)
    for arg in "$@"; do
      case "$arg" in
        [A-Z]*)
          if grep -q "^${arg}=" "$ST/readings"; then
            grep "^${arg}=" "$ST/readings"
          else
            printf 'UNREAD %s\n' "$arg"
          fi
          ;;
      esac
    done
    ;;
  *)
    echo "fake docker: unhandled $*" >&2
    exit 127
    ;;
esac
"""


def _run(
    tmp: Path,
    *,
    env_name: str,
    env_body: str,
    readings: str,
    report_only: bool,
) -> subprocess.CompletedProcess[str]:
    state = tmp / "state"
    bindir = tmp / "bin"
    state.mkdir()
    bindir.mkdir()
    (state / "readings").write_bytes(readings.encode("utf-8"))
    (state / "calls.log").write_bytes(b"")
    _write_exec(bindir / "docker", _FAKE.replace("__STATE__", _posix(state)))
    env_file = tmp / "box.env"
    env_file.write_bytes(env_body.encode("utf-8"))
    args = f"--env {env_name}"
    if report_only:
        args += " --report-only"
    command = (
        f'export PATH="{_posix(bindir)}:$PATH"; '
        f'export DATANIKA_DRIFT_ENV_FILE="{_posix(env_file)}"; '
        f'export DATANIKA_DRIFT_DECLARATION="{_posix(STAGING_DECL)}"; '
        f'export DATANIKA_DRIFT_CONTAINER="synthetic-staging"; '
        f'exec bash "{_posix(SCRIPT)}" {args}'
    )
    return subprocess.run(
        [_bash(), "-lc", command],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def _staging_env(*, extra: str = "") -> str:
    lines = [f"{name}={value}" for name, value in STAGING_DECLARED.items()]
    lines.append(f"SECRET_KEY={MARKER}")
    lines.append(f"DATABASE_URL=postgresql://{MARKER}@localhost/db")
    if extra:
        lines.append(extra)
    return "\n".join(lines) + "\n"


def _readings(*, extra: str = "") -> str:
    lines = [f"{name}={value}" for name, value in STAGING_DECLARED.items()]
    if extra:
        lines.append(extra)
    return "\n".join(lines) + "\n"


def test_declarations_are_the_reviewable_halves() -> None:
    assert _parse_decl(PROD_DECL.read_text(encoding="utf-8")) == PROD_DECLARED
    assert _parse_decl(STAGING_DECL.read_text(encoding="utf-8")) == STAGING_DECLARED
    assert OVERRIDE not in STAGING_DECLARED
    joined = PROD_DECL.read_text(encoding="utf-8") + STAGING_DECL.read_text(encoding="utf-8")
    assert "SECRET_KEY=" not in joined
    assert "pointer-app.env.docker" not in joined


def test_synthetic_staging_override_fails_and_names_the_resolved_value(tmp_path: Path) -> None:
    """The env file says false. The settings object resolved true. Report true."""
    proc = _run(
        tmp_path,
        env_name="staging",
        env_body=_staging_env(extra=f"{OVERRIDE}=false"),
        readings=_readings(extra=f"{OVERRIDE}=true"),
        report_only=False,
    )
    assert proc.returncode == 1, proc.stderr
    assert f"DRIFT {OVERRIDE}=true" in proc.stdout
    assert f"DRIFT {OVERRIDE}=false" not in proc.stdout
    assert MARKER not in proc.stdout
    assert MARKER not in proc.stderr
    calls = (tmp_path / "state" / "calls.log").read_text(encoding="utf-8")
    assert "exec" in calls
    assert "inspect" not in calls
    assert "SECRET_KEY" not in calls
    assert MARKER not in calls
    assert OVERRIDE in calls


def test_clean_negative_control_reports_zero(tmp_path: Path) -> None:
    proc = _run(
        tmp_path,
        env_name="staging",
        env_body=_staging_env(),
        readings=_readings(),
        report_only=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "reviewable-drift 0" in proc.stdout
    assert "DRIFT " not in proc.stdout
    assert MARKER not in proc.stdout
    assert MARKER not in proc.stderr


def test_prod_report_only_names_drift_and_does_not_fail(tmp_path: Path) -> None:
    proc = _run(
        tmp_path,
        env_name="prod",
        env_body=_staging_env(extra=f"{OVERRIDE}=false"),
        readings=_readings(extra=f"{OVERRIDE}=true"),
        report_only=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert f"DRIFT {OVERRIDE}=true" in proc.stdout
    assert MARKER not in proc.stdout


def _code_lines(text: str) -> str:
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def test_the_script_asks_the_settings_object() -> None:
    script = _code_lines(SCRIPT.read_text(encoding="utf-8"))
    probe = _code_lines(PROBE.read_text(encoding="utf-8"))
    assert "docker inspect" not in script
    assert "os.environ" not in script
    assert "read_reviewable_settings.py" in script
    assert "docker exec -i" in script
    assert "from datanika.config import settings" in probe
    assert "os.environ" not in probe
    assert "datanika_cloud.billing.config" in probe


def test_the_production_deploy_is_report_only() -> None:
    text = DEPLOY.read_text(encoding="utf-8")
    assert text.count("check-reviewable-drift.sh") == 1
    assert "check-reviewable-drift.sh --env prod --report-only" in text


def test_probe_prints_resolved_reviewable_values_and_hides_secrets(tmp_path: Path) -> None:
    env = {
        "PATH": os.environ.get("PATH", ""),
        "SYSTEMROOT": os.environ.get("SYSTEMROOT", ""),
        "PATHEXT": os.environ.get("PATHEXT", ""),
        "DATANIKA_EDITION": "cloud",
        "DATANIKA_ALLOW_LOCAL_FILE_PATHS": "false",
        "SECRET_KEY": MARKER,
        "DATABASE_URL": f"postgresql://{MARKER}@localhost/db",
    }
    proc = subprocess.run(
        [
            sys.executable,
            str(PROBE),
            "DATANIKA_EDITION",
            "DATANIKA_ALLOW_LOCAL_FILE_PATHS",
            "SECRET_KEY",
            "DATABASE_URL",
            "NOT_A_REAL_SETTING",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env=env,
        cwd=tmp_path,
    )
    assert proc.returncode == 0, proc.stderr
    assert "DATANIKA_EDITION=cloud" in proc.stdout
    assert "DATANIKA_ALLOW_LOCAL_FILE_PATHS=false" in proc.stdout
    assert "UNREAD NOT_A_REAL_SETTING" in proc.stdout
    assert "SECRET_KEY=" not in proc.stdout
    assert "DATABASE_URL=" not in proc.stdout
    assert MARKER not in proc.stdout

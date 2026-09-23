"""Grafana's SQLite goes to WAL once, while stopped, and the conversion is fail-safe (core#1476).

`GF_DATABASE_WAL=true` is set and does nothing on Grafana 13.1.0 (modernc.org/sqlite): a fresh
database created with it is `journal_mode=delete`. `journal_mode=WAL` is persistent in the file
header, though, and Grafana keeps a converted file in WAL — probed on the exact production image.
So `scripts/grafana-sqlite-wal.sh` converts it, and `deploy-pointer.yml` runs it while Grafana is
stopped.

What these tests pin is the script's CONTROL FLOW, driven through a fake `docker` that records
every call: it must refuse while Grafana runs, do nothing when the file is already WAL, back up
before converting, verify before handing the files back, and never report success on a file that
does not read back as WAL. And the deploy must treat its failure as a warning — the worst case is
the journal mode production already runs, never a Grafana that does not come back.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "grafana-sqlite-wal.sh"
DEPLOY = ROOT / ".github" / "workflows" / "deploy-pointer.yml"

FAKE_DOCKER = r"""#!/usr/bin/env bash
# Fake docker for test_grafana_sqlite_wal.py. Records calls; answers from FAKE_* variables.
echo "$*" >> "$FAKE_LOG"
if [ "$1" = inspect ]; then
  [ "${FAKE_NO_CONTAINER:-0}" = 1 ] && exit 1
  case "$3" in
    *State.Running*) echo "${FAKE_RUNNING:-false}" ;;
    *Mounts*) echo "${FAKE_VOLUME-datanika_grafana_data}" ;;
  esac
  exit 0
fi
if [ "$1" = run ]; then
  code="${!#}"
  case "$code" in
    *"journal_mode=WAL"*) echo convert >> "$FAKE_ORDER"; echo "${FAKE_CONVERT:-wal ok}"; exit 0 ;;
    *shutil.copy2*) echo backup >> "$FAKE_ORDER"; exit "${FAKE_BACKUP_RC:-0}" ;;
    *os.chown*) echo chown >> "$FAKE_ORDER"; echo "chowned 1 file(s) to 472:0"; exit 0 ;;
    *"PRAGMA journal_mode'"*)
      n=$(grep -c '^read' "$FAKE_ORDER" 2>/dev/null || true)
      echo read >> "$FAKE_ORDER"
      if [ "${n:-0}" = 0 ]; then echo "${FAKE_MODE_BEFORE:-delete}"
      else echo "${FAKE_MODE_AFTER:-wal}"; fi
      exit 0 ;;
  esac
  echo "fake docker: unrecognised run: $code" >&2; exit 99
fi
exit 0
"""

BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(BASH is None, reason="needs bash to run the script")


def _run(tmp_path: Path, **fake: str) -> tuple[int, str, list[str], list[str]]:
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    docker = bindir / "docker"
    docker.write_text(FAKE_DOCKER, encoding="utf-8", newline="\n")
    docker.chmod(0o755)
    log, order = tmp_path / "calls.log", tmp_path / "order.log"
    env = {
        **os.environ,
        "PATH": f"{bindir}{os.pathsep}{os.environ.get('PATH', '')}",
        "FAKE_LOG": str(log),
        "FAKE_ORDER": str(order),
        **fake,
    }
    r = subprocess.run(
        [BASH, str(SCRIPT)], env=env, capture_output=True, text=True, timeout=60, check=False
    )
    calls = log.read_text(encoding="utf-8").splitlines() if log.exists() else []
    steps = order.read_text(encoding="utf-8").splitlines() if order.exists() else []
    return r.returncode, r.stdout + r.stderr, calls, steps


def test_it_refuses_while_grafana_is_running(tmp_path: Path) -> None:
    rc, out, calls, steps = _run(tmp_path, FAKE_RUNNING="true")
    assert rc == 3, out
    assert steps == [], "it touched the database while Grafana held it"
    assert not any(c.startswith("run ") for c in calls)


def test_it_refuses_when_there_is_no_container(tmp_path: Path) -> None:
    rc, out, _calls, steps = _run(tmp_path, FAKE_NO_CONTAINER="1")
    assert rc == 3, out
    assert steps == []


def test_it_refuses_when_there_is_no_data_volume(tmp_path: Path) -> None:
    rc, out, _calls, steps = _run(tmp_path, FAKE_VOLUME="")
    assert rc == 3, out
    assert steps == []


def test_an_already_wal_database_is_left_alone(tmp_path: Path) -> None:
    """Idempotent: every deploy runs this, and only the first may change anything."""
    rc, out, _calls, steps = _run(tmp_path, FAKE_MODE_BEFORE="wal")
    assert rc == 0, out
    assert steps == ["read"], steps


def test_a_rollback_journal_database_is_backed_up_then_converted_then_verified(
    tmp_path: Path,
) -> None:
    rc, out, calls, steps = _run(tmp_path, FAKE_MODE_BEFORE="delete", FAKE_MODE_AFTER="wal")
    assert rc == 0, out
    assert steps == ["read", "backup", "convert", "chown", "read"], steps
    runs = [c for c in calls if c.startswith("run ")]
    assert runs and all("--network none" in c and "--user 0:0" in c for c in runs), runs
    assert all("datanika_grafana_data:/g" in c for c in runs), (
        "it wrote to a volume it did not resolve"
    )


def test_a_conversion_that_did_not_take_fails_and_hands_nothing_back(tmp_path: Path) -> None:
    rc, out, _calls, steps = _run(tmp_path, FAKE_CONVERT="delete ok")
    assert rc == 4, out
    assert "chown" not in steps


def test_a_failed_backup_stops_before_any_conversion(tmp_path: Path) -> None:
    rc, out, _calls, steps = _run(tmp_path, FAKE_BACKUP_RC="1")
    assert rc == 4, out
    assert "convert" not in steps, "it converted without a backup"


def test_a_file_that_does_not_read_back_as_wal_is_not_reported_as_converted(
    tmp_path: Path,
) -> None:
    """The mode is a property of the FILE; the connection that set it is not the witness."""
    rc, out, _calls, _steps = _run(tmp_path, FAKE_MODE_AFTER="delete")
    assert rc == 4, out
    assert "is now in WAL mode" not in out


def test_the_deploy_converts_while_stopped_and_treats_failure_as_a_warning() -> None:
    text = DEPLOY.read_text(encoding="utf-8")
    stop = text.index("docker compose stop grafana")
    conv = text.index("bash scripts/grafana-sqlite-wal.sh ||")
    recreate = text.index("docker compose up -d --force-recreate prometheus grafana blackbox")
    assert stop < conv < recreate, "the conversion must sit between stopping and recreating Grafana"
    line = text[conv : text.index("\n", conv)]
    assert '|| echo "::warning::' in line, "a failed conversion must not fail the deploy"


def test_the_deploy_reports_the_mode_off_the_running_database() -> None:
    text = DEPLOY.read_text(encoding="utf-8")
    assert "Report Grafana's SQLite journal mode (core#1476)" in text
    assert "test -e /var/lib/grafana/grafana.db-wal" in text


def test_the_fake_can_tell_the_cases_apart(tmp_path: Path) -> None:
    """Control: if the fake answered every read the same way, the tests above would be vacuous."""
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    already = _run(tmp_path / "a", FAKE_MODE_BEFORE="wal")
    rollback = _run(tmp_path / "b", FAKE_MODE_BEFORE="delete")
    assert already[3] == ["read"] and rollback[3][:2] == ["read", "backup"], (already, rollback)

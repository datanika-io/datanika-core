"""Run `deploy/server/export-prod-settings.sh` against a fake `docker` (core#1421).

`test_prod_settings_exporter.py` reads the script; this one *executes* it. The defect it
pins is a timing one, and text cannot see timing:

    A blue/green swap stops the colour it retires. A container that is running when the
    exporter's loop filters `docker ps`, and stopping by the time `docker exec` reaches
    it, fails the read. An empty read is `state=error`, `error` never equals `require`,
    and the critical `Production Setting Violation` rule paged for a container that
    served nothing -- 2026-09-17 06:17:40Z, every live container compliant.

The fix re-checks liveness when a read errors. Its constraint is the half that matters
more: **`absent` stays a violation, and so does `error` in a container that is still
running.** Treating a missing or unreadable setting as compliant is `noDataState: OK` in a
different costume. So every test here that proves a stopping container is *discarded* has
a sibling that proves a running one is still *graded*.

Real docker is deliberately not used: the fake decides which containers are running and
what each read returns, which is the only way to make a container stop *between* two
calls on purpose. The two Windows traps from `test_docker_prune_behaviour.py` apply
unchanged -- a drive letter in `PATH` silently disables the stub, and `write_text` writes
CRLF -- and `test_the_stub_is_what_the_script_calls` is the control for the first.
"""

from __future__ import annotations

import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "deploy" / "server" / "export-prod-settings.sh"

ALL = ("datanika-app", "datanika-app-b", "datanika-celery", "datanika-beat")
GRADED = "datanika_allow_local_file_paths"


def _bash() -> str:
    exe = shutil.which("bash")
    if exe is None:  # pragma: no cover - no bash means no box either
        pytest.fail("bash not found; this suite must not silently stop testing the script")
    return exe


def _posix_path(path: Path) -> str:
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
    path.write_bytes(body.encode("utf-8"))  # never write_text: CRLF
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


# The fake `docker`. State lives in files so it survives across the script's many calls:
#   running          one container name per line -- what `docker ps` lists
#   mode_<name>      what `docker exec <name>` does:
#                      false | true | absent  -> prints False / True / ABSENT
#                      fail                   -> exits 1, the container stays running
#                      stop                   -> exits 1 and the container is gone at once
#                      stopping:<n>           -> exits 1; listed for <n> more `ps` calls
#   countdown_<name> remaining `ps` calls before a `stopping` container disappears
_FAKE_DOCKER = r"""#!/usr/bin/env bash
ST="__STATE__"
echo "docker $1 ${2:-}" >> "$ST/calls.log"
case "$1" in
  ps)
    cat "$ST/running"
    for f in "$ST"/countdown_*; do
      [ -e "$f" ] || continue
      name=${f##*/countdown_}
      n=$(( $(cat "$f") - 1 ))
      if [ "$n" -le 0 ]; then
        grep -vx "$name" "$ST/running" > "$ST/running.new"; mv "$ST/running.new" "$ST/running"
        rm -f "$f"
      else
        printf '%s' "$n" > "$f"
      fi
    done
    ;;
  exec)
    name=$2
    mode=$(cat "$ST/mode_$name" 2>/dev/null || echo false)
    case "$mode" in
      false)  echo False ;;
      true)   echo True ;;
      absent) echo ABSENT ;;
      fail)   echo "Error response from daemon: exec failed" >&2; exit 1 ;;
      stop)
        grep -vx "$name" "$ST/running" > "$ST/running.new"; mv "$ST/running.new" "$ST/running"
        echo "Error response from daemon: container $name is not running" >&2; exit 1 ;;
      stopping:*)
        [ -e "$ST/countdown_$name" ] || printf '%s' "${mode#stopping:}" > "$ST/countdown_$name"
        echo "Error response from daemon: container $name is stopping" >&2; exit 1 ;;
    esac
    ;;
  *) echo "fake docker: unhandled $*" >&2; exit 127 ;;
esac
"""


class Box:
    """A fake production host: which containers run, and what reading each one returns."""

    def __init__(
        self,
        tmp: Path,
        modes: dict[str, str],
        *,
        running: tuple[str, ...] = ALL,
        tries: int = 3,
    ) -> None:
        self.bin = tmp / "bin"
        self.state = tmp / "state"
        self.textfile = tmp / "textfile"
        for d in (self.bin, self.state, self.textfile):
            d.mkdir(parents=True, exist_ok=True)
        self.tries = tries
        (self.state / "running").write_bytes(("\n".join(running) + "\n").encode())
        (self.state / "calls.log").write_bytes(b"")
        for name, mode in modes.items():
            (self.state / f"mode_{name}").write_bytes(mode.encode())
        _write_exec(self.bin / "docker", _FAKE_DOCKER.replace("__STATE__", _posix_path(self.state)))

    def run(self) -> str:
        proc = subprocess.run(
            [
                _bash(),
                "-lc",
                f'export PATH="{_posix_path(self.bin)}:$PATH"; '
                f'export DATANIKA_PROD_SETTINGS_TEXTFILE_DIR="{_posix_path(self.textfile)}"; '
                f"export DATANIKA_PROD_SETTINGS_RECHECK_TRIES={self.tries}; "
                "export DATANIKA_PROD_SETTINGS_RECHECK_INTERVAL=0; "
                f'exec bash "{SCRIPT.as_posix()}"',
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert proc.returncode == 0, f"exporter exited {proc.returncode}: {proc.stderr}"
        out = self.textfile / "datanika_prod_settings.prom"
        assert out.is_file(), f"no textfile written; stderr: {proc.stderr}"
        return out.read_text(encoding="utf-8")

    def calls(self) -> list[str]:
        return (self.state / "calls.log").read_text(encoding="utf-8").split()


def _violation(prom: str, container: str) -> str | None:
    m = re.search(
        rf'^datanika_prod_setting_violation\{{container="{re.escape(container)}",'
        rf'setting="{GRADED}",require="false"\}} (\S+)$',
        prom,
        re.M,
    )
    return m.group(1) if m else None


def _state(prom: str, container: str) -> str | None:
    m = re.search(
        rf'^datanika_prod_setting\{{container="{re.escape(container)}",'
        rf'setting="{GRADED}",state="([a-z]+)"\}}',
        prom,
        re.M,
    )
    return m.group(1) if m else None


def _mentions(prom: str, container: str) -> int:
    return len(re.findall(rf'container="{re.escape(container)}"', prom))


def test_the_stub_is_what_the_script_calls(tmp_path: Path) -> None:
    """Control: without it every test below could pass against real docker and prove nothing."""
    box = Box(tmp_path, {})
    prom = box.run()
    calls = box.calls()
    assert "ps" in calls and "exec" in calls, f"the fake docker was never called: {calls}"
    for name in ALL:
        assert _violation(prom, name) == "0", f"{name} should read compliant:\n{prom}"
    assert re.search(r"^datanika_prod_settings_scrape_success 1$", prom, re.M)


def test_a_container_that_stops_during_its_read_emits_nothing(tmp_path: Path) -> None:
    """The core#1421 page: the retiring colour is not graded once it has stopped."""
    prom = Box(tmp_path, {"datanika-app": "stop"}).run()
    assert _mentions(prom, "datanika-app") == 0, (
        f"a container that stopped mid-read was still graded -- the swap page:\n{prom}"
    )
    for name in ("datanika-app-b", "datanika-celery", "datanika-beat"):
        assert _violation(prom, name) == "0", f"{name} lost its reading:\n{prom}"
    assert re.search(r"^datanika_prod_settings_scrape_success 1$", prom, re.M)


def test_an_exec_error_in_a_running_container_is_still_a_violation(tmp_path: Path) -> None:
    """The constraint: an unreadable RUNNING container is not a compliant one."""
    prom = Box(tmp_path, {"datanika-app-b": "fail"}).run()
    assert _state(prom, "datanika-app-b") == "error", prom
    assert _violation(prom, "datanika-app-b") == "1", (
        f"an exec error in a container that is still running must be a violation:\n{prom}"
    )


def test_absent_in_a_running_container_is_a_violation_and_is_not_rechecked(
    tmp_path: Path,
) -> None:
    """`absent` is a property of the code, not of a stop: graded at once, never re-checked."""
    box = Box(tmp_path, {"datanika-app-b": "absent"})
    prom = box.run()
    assert _state(prom, "datanika-app-b") == "absent", prom
    assert _violation(prom, "datanika-app-b") == "1", (
        f"an absent setting must stay a violation (the banner's rule):\n{prom}"
    )
    # One `ps` per container at the loop filter and none more: nothing errored, so
    # nothing was re-checked.
    assert box.calls().count("ps") == len(ALL), box.calls()


def test_a_true_value_is_a_violation(tmp_path: Path) -> None:
    """The reading the exporter exists for still fires: the fake can produce a real one."""
    prom = Box(tmp_path, {"datanika-celery": "true"}).run()
    assert _state(prom, "datanika-celery") == "true", prom
    assert _violation(prom, "datanika-celery") == "1", prom


def test_the_wait_is_bounded_in_both_directions(tmp_path: Path) -> None:
    """Gone within the bound: discarded. Still listed when the bound runs out: graded."""
    gone = Box(tmp_path / "gone", {"datanika-app": "stopping:2"}, tries=4).run()
    assert _mentions(gone, "datanika-app") == 0, (
        f"a container that finished stopping inside the wait was graded:\n{gone}"
    )

    stuck = Box(tmp_path / "stuck", {"datanika-app": "stopping:50"}, tries=2).run()
    assert _violation(stuck, "datanika-app") == "1", (
        f"a container still running when the wait ran out must be graded:\n{stuck}"
    )


def test_a_container_not_running_at_the_filter_is_not_read(tmp_path: Path) -> None:
    """Unchanged behaviour: the retired colour that is already `Exited` is never exec'd."""
    box = Box(tmp_path, {}, running=("datanika-app-b", "datanika-celery", "datanika-beat"))
    prom = box.run()
    assert _mentions(prom, "datanika-app") == 0, prom
    assert box.calls().count("exec") == 3 * 3, box.calls()  # 3 running x 3 manifest rows


def test_the_production_defaults_outlast_a_compose_stop() -> None:
    """The overrides exist for this file only; the box runs the defaults.

    `docker compose stop` waits 10 s before SIGKILL, and the swap's retired colour reads
    `Exited (137)`, so it takes all of it. A default bound shorter than that would put
    the page back on exactly the swaps that stop slowly.
    """
    text = SCRIPT.read_text(encoding="utf-8")
    assert re.search(
        r'TEXTFILE_DIR="\$\{DATANIKA_PROD_SETTINGS_TEXTFILE_DIR:-/opt/datanika/node_textfile\}"',
        text,
    ), "the textfile default moved; node-exporter reads only /opt/datanika/node_textfile"
    tries = re.search(r'RECHECK_TRIES="\$\{DATANIKA_PROD_SETTINGS_RECHECK_TRIES:-(\d+)\}"', text)
    interval = re.search(
        r'RECHECK_INTERVAL="\$\{DATANIKA_PROD_SETTINGS_RECHECK_INTERVAL:-(\d+)\}"', text
    )
    assert tries and interval, "the re-check bound is no longer declared with a default"
    assert int(tries.group(1)) * int(interval.group(1)) >= 15, (
        "the default re-check bound no longer outlasts compose's 10 s stop timeout"
    )

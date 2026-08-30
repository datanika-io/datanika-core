"""Run `scripts/prune-docker-cache.sh` against a stubbed docker and make its guards fire.

`test_docker_prune.py` reads the script; this one *executes* it. The distinction matters
here more than usual: the load-bearing line is an assertion that the prune did not remove
a container-referenced image, and an assertion nobody has watched fail is decoration.
That assertion protects the blue/green rollback — an `Exited (137)` container plus a
dangling image — and the whole of core#666 is about how easy it is to delete by accident.

Real docker is deliberately NOT used. Pruning the dev machine's build cache to test a
prune would evict layers that belong to whoever is sitting at it.

Two Windows traps this harness is built around, both of which make a shell test pass
while testing nothing (WORKFLOW_RULES §3, learned on core#603):

1. **A drive letter in `PATH` disables everything after it.** bash splits `PATH` on `:`,
   so `D:/Temp/bin` becomes the two nonexistent directories `D` and `/Temp/bin`, and the
   suite silently runs against the machine's real `docker`. `_posix_path` uses `cygpath`,
   and `test_the_stub_is_actually_intercepting` asserts the interception itself — a
   harness that stops intercepting fails *permissively*, which is invisible.
2. **`Path.write_text` writes CRLF.** `$(cat f)` then yields `healthy\\r` and a correct
   value compares false. Everything here goes through `write_bytes`.
"""

from __future__ import annotations

import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "prune-docker-cache.sh"

# Two containers, as on the box: the serving colour and the exited rollback. The second
# one's image is dangling, which is exactly what makes it easy to delete.
SERVING_IMG = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
ROLLBACK_IMG = "sha256:2222222222222222222222222222222222222222222222222222222222222222"


def _bash() -> str:
    exe = shutil.which("bash")
    if exe is None:  # pragma: no cover - no bash means no deploy either
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


class Box:
    """A fake host: a programmable `docker`, a programmable `df`, and a call log."""

    def __init__(self, tmp: Path, *, eats_rollback: bool = False, free_gb: int = 74) -> None:
        self.dir = tmp
        self.bin = tmp / "bin"
        self.bin.mkdir(parents=True, exist_ok=True)
        self.log = tmp / "calls.log"
        self.log.write_bytes(b"")

        # `image inspect` fails for the rollback image once `eats_rollback` is on — the
        # exact observable a `container prune` + image prune would leave behind.
        eaten = ROLLBACK_IMG if eats_rollback else "__none__"
        _write_exec(
            self.bin / "docker",
            f'''#!/usr/bin/env bash
echo "docker $*" >> "{_posix_path(self.log)}"
case "$1 $2" in
  "ps -aq")        printf 'c_serving\\nc_rollback\\n' ;;
  "inspect -f")    # `docker inspect -f '{{{{.Image}}}}' <ids...>` over stdin-listed ids
                   for c in "${{@:4}}"; do
                     case "$c" in
                       c_serving)  echo "{SERVING_IMG}" ;;
                       c_rollback) echo "{ROLLBACK_IMG}" ;;
                     esac
                   done ;;
  "images -aq")    printf 'i1\\ni2\\ni3\\n' ;;
  "builder du")    printf 'ID\\tRECLAIMABLE\\nTotal:\\t53.03GB\\n' ;;
  "builder prune") echo "Total reclaimed space: 33GB" ;;
  "image prune")   echo "Total reclaimed space: 0B" ;;
  "image inspect") [ "$2" = "inspect" ] && shift
                   for a in "$@"; do
                     [ "$a" = "{eaten}" ] && exit 1
                   done
                   echo "ok" ;;
  *)               echo "docker: unhandled $*" >&2; exit 127 ;;
esac
exit 0
''',
        )
        # `df -B1 --output=used|avail /`
        used_gb = 158 - free_gb
        _write_exec(
            self.bin / "df",
            f'''#!/usr/bin/env bash
echo "df $*" >> "{_posix_path(self.log)}"
case "$*" in
  *avail*) printf 'Avail\\n%d\\n' $(( {free_gb} * 1073741824 )) ;;
  *)       printf 'Used\\n%d\\n' $(( {used_gb} * 1073741824 )) ;;
esac
''',
        )

    def run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                _bash(),
                "-lc",
                f'export PATH="{_posix_path(self.bin)}:$PATH"; '
                f'exec bash "{SCRIPT.as_posix()}" {" ".join(args)}',
            ],
            capture_output=True,
            text=True,
        )

    def calls(self) -> str:
        return self.log.read_text(encoding="utf-8", errors="replace")


def test_the_stub_is_actually_intercepting(tmp_path: Path) -> None:
    """Without this, every test below can pass against the real docker and prove nothing.

    A harness that stops intercepting fails permissively. This is the control case that
    makes the rest of the file mean something (core#603).
    """
    box = Box(tmp_path)
    box.run("20GB", "5")
    calls = box.calls()
    assert "docker ps -aq" in calls, (
        "the docker stub was never invoked — PATH interception is broken and every "
        f"assertion in this file is vacuous. calls seen:\n{calls}"
    )
    assert "df " in calls, "the df stub was never invoked"


def test_happy_path_prunes_and_exits_clean(tmp_path: Path) -> None:
    box = Box(tmp_path)
    r = box.run("20GB", "5")
    assert r.returncode == 0, f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    calls = box.calls()
    assert "builder prune" in calls, "the capped builder prune did not run"
    assert "--keep-storage 20GB" in calls, f"cap not passed through; calls:\n{calls}"
    assert "image prune" in calls
    assert "rollback images intact: 2/2" in r.stdout


def test_it_fails_when_the_prune_eats_a_container_held_image(tmp_path: Path) -> None:
    """The assertion this script exists for.

    If a future edit adds `container prune`, or turns the image prune into `-a`, the
    rollback image stops being referenced and disappears. This must stop the deploy
    *before* the blue/green swap, not surface at 2 a.m. when the rollback is needed.
    """
    box = Box(tmp_path, eats_rollback=True)
    r = box.run("20GB", "5")
    assert r.returncode == 1, (
        "the script exited 0 after a container-referenced image vanished — the rollback "
        f"guard does not fire.\nstdout:\n{r.stdout}"
    )
    assert "::error::" in r.stdout
    assert ROLLBACK_IMG in r.stdout, "the error must name the image that went missing"


def test_it_fails_when_free_disk_is_below_the_floor(tmp_path: Path) -> None:
    """Failing before a build that cannot finish beats failing halfway through one."""
    box = Box(tmp_path, free_gb=2)
    r = box.run("20GB", "5")
    assert r.returncode == 1, f"expected the disk floor to fire.\nstdout:\n{r.stdout}"
    assert "floor" in r.stdout


def test_it_warns_but_proceeds_on_low_but_workable_disk(tmp_path: Path) -> None:
    """No alert rule watches disk on this box, so the deploy is the only thing looking."""
    box = Box(tmp_path, free_gb=12)
    r = box.run("20GB", "5")
    assert r.returncode == 0, f"12 GiB free should warn, not fail.\nstdout:\n{r.stdout}"
    assert "::warning::" in r.stdout


def test_it_never_prunes_containers(tmp_path: Path) -> None:
    """The box is shared: an exited container is deliberate state, not garbage."""
    box = Box(tmp_path)
    box.run("20GB", "5")
    assert "container prune" not in box.calls()
    assert "system prune" not in box.calls()

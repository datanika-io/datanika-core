"""`deploy-bluegreen.sh` must roll back on every failure path (core#603).

**The defect this exists to prevent.** The script armed `trap 'rollback' ERR` and then
signalled every failure as ``[ "$CODE" = 200 ] || { log "FATAL: ..."; exit 1; }``. bash
runs an ERR trap for neither half of that — not for an explicit ``exit``, and not for a
command failing on the left of ``||``. The rollback was therefore armed and unreachable
on every path that could reach it. Nothing detected this for five weeks because the only
evidence anyone had was *reading the script*, and read that way it looks correct.

On 2026-08-29 the post-swap ``/mcp`` assertion failed against an image that could not
import ``datanika_mcp`` (core#602). Production was left on the **new** colour with ``/mcp``
down, Celery still on the old image, and monitoring config transferred but not loaded —
a half-deployed state announced by nothing. It was undone by hand.

**So this file forces the failures rather than inspecting the source.** It runs the
shipped script byte-identically (via ``BLUEGREEN_TEST_ROOT``, which only prefixes host
paths) against a fake tree, with ``docker``/``curl``/``apachectl``/``sleep`` replaced by
recording shims whose answers each test chooses. Every scenario asserts on the state left
behind — where the Apache include points, which containers were stopped, whether Apache
was reloaded — not on the log text, because the log is what lied last time.

It runs anywhere bash does, Git Bash included, so the dev machine's pre-push hook exercises
it too. A missing bash is a **failure**, not a skip — a guard that can quietly stop testing
anything is the same shape of defect as the trap it guards.
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
SCRIPT = ROOT / "scripts" / "deploy-bluegreen.sh"

# prod colours, from the script's own table
BLUE_BE, BLUE_FE, BLUE_CTR = "8000", "3000", "datanika-app"
GREEN_BE, GREEN_FE, GREEN_CTR = "8010", "3010", "datanika-app-b"

def _bash() -> str:
    exe = shutil.which("bash")
    if exe is None:  # pragma: no cover - a box with no bash cannot run the deploy either
        pytest.fail("bash not found; this suite must not silently stop testing the deploy script")
    return exe


def _write_exec(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


class Harness:
    """A fake prod box: an Apache include, a compose dir, and programmable shims."""

    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.fake = tmp / "fake"
        self.bin = tmp / "bin"
        self.root = tmp / "root"
        self.fake.mkdir()
        self.bin.mkdir()

        self.include = self.root / "etc/apache2/conf-enabled/datanika-prod-active.conf"
        self.include.parent.mkdir(parents=True)
        # Blue is live, so the script's target is green — same as prod on 2026-08-29.
        self.include.write_text(
            "# Active prod backend/frontend ports — rewritten by deploy-bluegreen.sh.\n"
            "# Parsed before sites-enabled/*, where the vhost consumes ${DATANIKA_BE} / ${DATANIKA_FE}.\n"
            f"Define DATANIKA_BE {BLUE_BE}\n"
            f"Define DATANIKA_FE {BLUE_FE}\n",
            encoding="utf-8",
        )

        compose_dir = self.root / "opt/datanika/datanika"
        compose_dir.mkdir(parents=True)
        (compose_dir / ".env.docker").write_text("POSTGRES_PASSWORD=fake\n", encoding="utf-8")

        # Shim defaults: a healthy target, a running container, every URL 200, and a
        # configtest that passes. Each test degrades exactly one of these.
        self.set_health("healthy")
        self.set_running("true")
        self.set_configtest(0)
        self.set_http({})

        calls = self.fake / "calls.log"
        calls.write_text("", encoding="utf-8")

        _write_exec(
            self.bin / "docker",
            f'''#!/usr/bin/env bash
echo "docker $*" >> "{calls}"
case "$1 $2" in
  "inspect --format") cat "{self.fake}/health" ;;
  "inspect -f")       cat "{self.fake}/running" ;;
esac
exit 0
''',
        )
        # Picks the first arg that looks like a URL, then the first http-plan line whose
        # pattern occurs in it. Unlisted URLs answer 200 so a test states only what it
        # is degrading.
        _write_exec(
            self.bin / "curl",
            f'''#!/usr/bin/env bash
url=""
for a in "$@"; do case "$a" in http*) url="$a"; break ;; esac; done
echo "curl $url" >> "{calls}"
code=200
while IFS='|' read -r pat rc; do
  [ -z "$pat" ] && continue
  case "$url" in *"$pat"*) code="$rc"; break ;; esac
done < "{self.fake}/http"
printf '%s' "$code"
exit 0
''',
        )
        _write_exec(
            self.bin / "apachectl",
            f'''#!/usr/bin/env bash
echo "apachectl $*" >> "{calls}"
[ "$1" = configtest ] && exit "$(cat "{self.fake}/configtest")"
exit 0
''',
        )
        # The health loop would otherwise sleep 5s per attempt; nothing under test depends
        # on wall-clock time.
        _write_exec(self.bin / "sleep", "#!/usr/bin/env bash\nexit 0\n")

    # --- shim programming -------------------------------------------------------------
    def set_health(self, status: str) -> None:
        (self.fake / "health").write_text(status + "\n", encoding="utf-8")

    def set_running(self, value: str) -> None:
        (self.fake / "running").write_text(value + "\n", encoding="utf-8")

    def set_configtest(self, rc: int) -> None:
        (self.fake / "configtest").write_text(str(rc) + "\n", encoding="utf-8")

    def set_http(self, plan: dict[str, int]) -> None:
        (self.fake / "http").write_text(
            "".join(f"{pat}|{code}\n" for pat, code in plan.items()), encoding="utf-8"
        )

    # --- run + inspect ----------------------------------------------------------------
    def run(self) -> subprocess.CompletedProcess[str]:
        env = dict(os.environ)
        env["BLUEGREEN_TEST_ROOT"] = self.root.as_posix()
        # PATH is prepended *inside* bash rather than through the environment: on Windows
        # an inherited `;`-separated Windows PATH is rewritten on the way in, and the shim
        # directory is exactly the entry that must survive intact.
        return subprocess.run(
            [
                _bash(),
                "-c",
                f'export PATH="{self.bin.as_posix()}:$PATH"; exec bash "{SCRIPT.as_posix()}" --env prod',
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=180,
        )

    @property
    def calls(self) -> list[str]:
        return (self.fake / "calls.log").read_text(encoding="utf-8").splitlines()

    @property
    def live_be(self) -> str:
        """The backend port the Apache include currently names."""
        for line in self.include.read_text(encoding="utf-8").splitlines():
            if line.startswith("Define DATANIKA_BE "):
                return line.split()[2]
        return ""

    def stopped(self, service: str) -> bool:
        return any(f"stop {service}" in c for c in self.calls)

    def graceful_count(self) -> int:
        return sum(1 for c in self.calls if c == "apachectl graceful")


@pytest.fixture
def box(tmp_path: Path) -> Harness:
    return Harness(tmp_path)


def test_happy_path_swaps_to_green_and_retires_blue(box: Harness) -> None:
    """Control case. Without this, every assertion below could pass on a script that
    simply refuses to deploy."""
    result = box.run()

    assert result.returncode == 0, result.stdout + result.stderr
    assert box.live_be == GREEN_BE
    assert box.stopped("app"), "the old colour must be retired on success"
    assert not box.stopped("app_b"), "the new colour must keep running"
    assert "ROLLBACK" not in result.stdout


def test_post_swap_mcp_failure_rolls_back(box: Harness) -> None:
    """The 2026-08-29 incident, reproduced.

    `/mcp` answers 200 through Cloudflare instead of 401 — the SPA fall-through shape an
    image without MCP produces. Before core#603 this exited 1 with Apache still pointing
    at the new colour. It must now end with production back on blue.
    """
    # Let the pre-repoint backend check pass so the failure lands *after* the swap, which
    # is the only position from which the missing rollback could do damage.
    box.set_http({"127.0.0.1:8010/mcp": 401, "app.datanika.io/mcp": 200})

    result = box.run()

    assert result.returncode != 0
    assert box.live_be == BLUE_BE, "Apache must be pointing back at the live colour"
    assert box.stopped("app_b"), "the half-deployed colour must be stopped"
    assert not box.stopped("app"), "the colour that was serving must never be stopped"
    assert box.graceful_count() == 2, "swap reload + rollback reload"
    assert "ROLLBACK COMPLETE" in result.stdout


def test_post_swap_proxy_failure_rolls_back(box: Harness) -> None:
    box.set_http({"127.0.0.1/readyz": 502})

    result = box.run()

    assert result.returncode != 0
    assert box.live_be == BLUE_BE
    assert box.stopped("app_b")
    assert not box.stopped("app")


def test_public_oauth_discovery_failure_rolls_back(box: Harness) -> None:
    """OAuth discovery is served by the vhost, not the backend, so it can fail on its own
    after a swap that is otherwise fine."""
    box.set_http({"app.datanika.io/.well-known/oauth-authorization-server": 404})

    result = box.run()

    assert result.returncode != 0
    assert box.live_be == BLUE_BE
    assert box.stopped("app_b")


def test_configtest_failure_restores_the_include(box: Harness) -> None:
    """The include is rewritten *before* configtest runs, so a bare exit here left a
    broken Apache config on disk for whoever next reloaded — for any reason, including a
    cert renewal on a shared box that also serves a co-tenant and the founder's VPN."""
    box.set_configtest(1)

    result = box.run()

    assert result.returncode != 0
    assert box.live_be == BLUE_BE, "a config that fails configtest must not be left on disk"
    assert box.stopped("app_b")
    assert not box.stopped("app")


def test_unhealthy_target_is_stopped_and_apache_untouched(box: Harness) -> None:
    """A pre-swap failure must not reload Apache at all — there is nothing to undo, and a
    needless graceful on this box is not free."""
    box.set_health("unhealthy")

    result = box.run()

    assert result.returncode != 0
    assert box.live_be == BLUE_BE
    assert box.stopped("app_b"), "the target must not be left running on its ports"
    assert box.graceful_count() == 0, "Apache was never repointed, so it must not be reloaded"


def test_direct_healthz_failure_rolls_back(box: Harness) -> None:
    box.set_http({"127.0.0.1:8010/healthz": 500})

    result = box.run()

    assert result.returncode != 0
    assert box.live_be == BLUE_BE
    assert box.stopped("app_b")
    assert box.graceful_count() == 0


def test_target_backend_missing_mcp_fails_before_apache_is_touched(box: Harness) -> None:
    """core#602 caught one step earlier.

    The image builds, the container is healthy, `/healthz` is 200 — and `datanika_mcp`
    still failed to import, so `/mcp` is not mounted. Asserting that on the target's own
    port means the deploy fails with production untouched, rather than after it has been
    repointed at the broken colour.
    """
    box.set_http({"127.0.0.1:8010/mcp": 404})

    result = box.run()

    assert result.returncode != 0
    assert box.live_be == BLUE_BE
    assert box.graceful_count() == 0, "production must not have been repointed at all"
    assert box.stopped("app_b")
    assert not box.stopped("app")


def test_sigterm_mid_swap_rolls_back(box: Harness) -> None:
    """A cancelled CD job closes the SSH channel, which signals this script mid-swap.

    Signals do not fire an ERR trap either, so before core#603 a cancellation abandoned
    the swap in whatever state it had reached.
    """
    # Fire the signal from inside the post-swap proxy check: Apache has been repointed by
    # then, so there is real state to unwind.
    killer = box.bin / "kill-me"
    _write_exec(killer, "#!/usr/bin/env bash\nkill -TERM $PPID\nexit 0\n")
    curl = box.bin / "curl"
    body = curl.read_text(encoding="utf-8").replace(
        'printf \'%s\' "$code"',
        'case "$url" in *127.0.0.1/readyz*) kill -TERM "$PPID" ;; esac\nprintf \'%s\' "$code"',
    )
    _write_exec(curl, body)

    result = box.run()

    assert result.returncode != 0
    assert box.live_be == BLUE_BE, "a cancelled deploy must not leave prod on a half-swapped colour"
    assert box.stopped("app_b")
    assert not box.stopped("app")


def test_no_bare_exit_1_survives_in_the_script() -> None:
    """The regression guard for the *class*, not the instance.

    `fatal()` is the only abort that reaches the EXIT trap by construction. A future
    `... || { log "FATAL"; exit 1; }` would reintroduce exactly core#603, and would pass
    every scenario above that does not happen to exercise its line.
    """
    lines = SCRIPT.read_text(encoding="utf-8").splitlines()
    offenders = [
        line.strip()
        for line in lines
        if "exit 1;" in line or line.rstrip().endswith("exit 1")
    ]
    assert len(offenders) == 1, f"expected only fatal() to exit 1, found: {offenders}"
    assert offenders[0].startswith("fatal()"), offenders[0]

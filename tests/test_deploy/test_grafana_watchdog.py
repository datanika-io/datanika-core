"""A watchdog outside Grafana reports a Grafana that cannot evaluate rules (core#1477).

The eight `Watchdog: … missing` alert rules cannot report a broken Grafana, because they ARE
Grafana alert rules. On 2026-09-20 Grafana could not evaluate anything for ~25 minutes, sent 176
false alerts, and the founder noticed before any agent did.

These tests drive `deploy/server/grafana-watchdog.sh` against a **fake Grafana** and a **fake
Telegram**, both real local HTTP servers, so what is pinned is behaviour and not the text of the
script. The load-bearing sequence is the debounce and the edge trigger:

    healthy      -> 0 messages
    1 bad read   -> 0 messages      (a single transient 500 must never page)
    2 bad reads  -> 1 message
    10 bad reads -> still 1 message (the 2026-09-20 storm was 18-25/minute)
    recovery     -> 1 more message

🔑 Every "it stays quiet" assertion here is worthless on its own — a script that sends nothing
ever satisfies all of them. So each quiet assertion is paired, in the same test, with the read
that must break the silence. A control that cannot fail in the direction of the conclusion is
not a control.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "deploy" / "server" / "grafana-watchdog.sh"
CRON = ROOT / "deploy" / "server" / "datanika.cron"
INSTALLER = ROOT / "scripts" / "install-server-scripts.sh"

BASH = shutil.which("bash")
CURL = shutil.which("curl")
pytestmark = pytest.mark.skipif(
    BASH is None or CURL is None, reason="needs bash and curl to drive the script"
)


# ---------------------------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------------------------
class _Fake:
    """A local HTTP server whose next response the test sets, and which records every hit."""

    def __init__(self) -> None:
        self.hits: list[tuple[str, str]] = []
        self.status = 200
        self.body = b"{}"
        outer = self

        class H(BaseHTTPRequestHandler):
            def _serve(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                payload = self.rfile.read(length).decode("utf-8", "replace") if length else ""
                outer.hits.append((self.path, payload))
                self.send_response(outer.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(outer.body)))
                self.end_headers()
                self.wfile.write(outer.body)

            # Names fixed by BaseHTTPRequestHandler's dispatch; not ours to rename.
            do_GET = _serve  # noqa: N815
            do_POST = _serve  # noqa: N815

            def log_message(self, *_a: object) -> None:  # keep pytest output readable
                return

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def _rules_body(n: int, errors: int = 0) -> bytes:
    """Grafana's prometheus-compatible rules payload with `n` rules, `errors` of them broken."""
    rules = [{"name": f"rule-{i}", "health": "error" if i < errors else "ok"} for i in range(n)]
    groups = [{"name": "datanika", "rules": rules}] if n else []
    return json.dumps({"status": "success", "data": {"groups": groups}}).encode()


@pytest.fixture
def rig(tmp_path: Path):
    grafana, telegram = _Fake(), _Fake()
    env_file = tmp_path / ".env.docker"
    env_file.write_text(
        "GRAFANA_ADMIN_USER=admin\n"
        "GRAFANA_ADMIN_PASSWORD=secret\n"
        "TELEGRAM_BOT_TOKEN=123:ABC\n"
        "TELEGRAM_CHAT_ID=-100999\n",
        encoding="utf-8",
        newline="\n",
    )
    # 44 provisioned rules on disk, in the shape the real provisioning YAML uses.
    prov = tmp_path / "alerts.yml"
    prov.write_text(
        "groups:\n" + "".join(f"      - uid: rule{i}\n" for i in range(44)),
        encoding="utf-8",
        newline="\n",
    )

    state = tmp_path / "state" / "wd.state"
    env = {
        **os.environ,
        "ENV_FILE": str(env_file),
        "GRAFANA_URL": grafana.url,
        "PROVISIONING_FILE": str(prov),
        "STATE_FILE": str(state),
        "TEXTFILE_DIR": str(tmp_path / "textfile"),
        "TELEGRAM_API": telegram.url,
        "PYTHON": sys.executable,
        "HOSTLABEL": "test-box",
    }

    class Rig:
        def __init__(self) -> None:
            self.grafana, self.telegram, self.env = grafana, telegram, env
            self.state, self.tmp = state, tmp_path

        def healthy(self, n: int = 44, errors: int = 0) -> None:
            self.grafana.status, self.grafana.body = 200, _rules_body(n, errors)

        def broken(self, status: int = 500, body: bytes = b"oops") -> None:
            self.grafana.status, self.grafana.body = status, body

        def run(self, **extra: str) -> subprocess.CompletedProcess:
            return subprocess.run(
                [BASH, str(SCRIPT)],
                env={**self.env, **extra},
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )

        @property
        def sent(self) -> list[str]:
            # The script posts with `--data-urlencode`, so the recorded payload is
            # percent-encoded form data. Asserting on the raw body would pass for a message
            # whose text had been mangled, and fail for a correct one — decode it.
            out = []
            for path, payload in self.telegram.hits:
                if "sendMessage" not in path:
                    continue
                fields = dict(urllib.parse.parse_qsl(payload, keep_blank_values=True))
                out.append(fields.get("text", ""))
            return out

    try:
        r = Rig()
        r.healthy()
        yield r
    finally:
        grafana.stop()
        telegram.stop()


# ---------------------------------------------------------------------------------------------
# Anti-vacuity: the rig can actually reach the script, and the script can actually send.
# ---------------------------------------------------------------------------------------------
def test_the_rig_reaches_grafana_and_the_script_runs(rig) -> None:
    r = rig.run()
    assert r.returncode == 0, r.stdout + r.stderr
    assert "http=200" in r.stdout, r.stdout + r.stderr
    assert "rules=44/44" in r.stdout, r.stdout + r.stderr
    assert rig.grafana.hits, "the script never called the fake Grafana — the rig is not wired"


# ---------------------------------------------------------------------------------------------
# The sequence the issue asks for.
# ---------------------------------------------------------------------------------------------
def test_healthy_sends_nothing_but_a_broken_pair_of_reads_does(rig) -> None:
    """The quiet half and the loud half, in one test, so the quiet half means something."""
    for _ in range(5):
        rig.run()
    assert rig.sent == [], "a healthy Grafana produced a message"

    rig.broken()
    rig.run()
    assert rig.sent == [], "a SINGLE failing read paged — the 2026-09-22 transient 500 would have"

    rig.run()
    assert len(rig.sent) == 1, f"two failing reads should send exactly one message, got {rig.sent}"


def test_ten_consecutive_failures_still_send_exactly_one(rig) -> None:
    rig.broken()
    for _ in range(10):
        rig.run()
    assert len(rig.sent) == 1, (
        f"edge-triggering failed: {len(rig.sent)} messages for one outage. The 2026-09-20 storm "
        "was 18-25 per minute; a watchdog that does the same is not an improvement."
    )


def test_recovery_sends_exactly_one_more(rig) -> None:
    rig.broken()
    rig.run()
    rig.run()
    assert len(rig.sent) == 1
    rig.healthy()
    rig.run()
    assert len(rig.sent) == 2, "recovery was not reported"
    assert "🟢" in rig.sent[1] or "again" in rig.sent[1]
    for _ in range(4):
        rig.run()
    assert len(rig.sent) == 2, "it kept talking after recovery"


# ---------------------------------------------------------------------------------------------
# The failure modes that make a watchdog lie.
# ---------------------------------------------------------------------------------------------
def test_zero_rule_groups_with_http_200_is_the_2026_09_20_shape(rig) -> None:
    """Grafana answering 200 with `{"groups":[]}` is the exact reading nobody was taking."""
    rig.grafana.status, rig.grafana.body = 200, json.dumps({"groups": []}).encode()
    rig.run()
    r = rig.run()
    assert len(rig.sent) == 1, r.stdout + r.stderr
    assert "ZERO rule groups" in rig.sent[0], rig.sent[0]
    assert "44 are provisioned" in rig.sent[0], rig.sent[0]


def test_a_partial_rule_set_is_caught_against_the_file_on_disk(rig) -> None:
    rig.healthy(n=30)
    rig.run()
    rig.run()
    assert len(rig.sent) == 1
    assert "30 rules, 44 are provisioned" in rig.sent[0], rig.sent[0]


def test_rules_in_error_health_are_caught(rig) -> None:
    rig.healthy(n=44, errors=3)
    rig.run()
    rig.run()
    assert len(rig.sent) == 1
    assert "3 of 44 rules are in Error health" in rig.sent[0], rig.sent[0]


def test_the_reported_group_count_is_the_rule_groups_and_not_a_shell_builtin(rig) -> None:
    """🚨 Regression: the group count was held in a variable named `GROUPS`.

    `GROUPS` is a bash BUILT-IN array of the caller's group ids. Bash silently discards
    assignments to it, so the script reported the dev machine's gid — `197121` — however many
    rule groups Grafana returned. Never 0, so the zero-groups clause could not fire.

    It survived every other test in this file, because `rules != provisioned` caught the same
    outage a different way. **A redundant clause hid a dead one**, and only isolating the
    clause exposed it. That is what the next test does; this one pins the symptom directly.
    """
    rig.healthy(n=44)
    r = rig.run()
    assert "groups=1 " in r.stdout, (
        f"expected exactly 1 rule group, got {r.stdout!r} — a large implausible number here "
        "means the variable collided with a shell built-in again"
    )
    rig.grafana.status, rig.grafana.body = 200, json.dumps({"groups": []}).encode()
    r = rig.run()
    assert "groups=0 " in r.stdout, r.stdout
    assert "healthy=0" in r.stdout, "an empty rules API was graded healthy"


def test_an_unreadable_provisioning_file_does_not_manufacture_a_failure(rig) -> None:
    """An unreadable file is not evidence about Grafana. The other clauses still hold.

    🔑 This is also the only test that isolates the zero-groups clause from the count-equality
    clause, and it is what caught the `GROUPS` shell-builtin collision above. Keep the second
    half: without it this passes for a script that reports nothing at all.
    """
    rig.healthy(n=44)
    r = rig.run(PROVISIONING_FILE=str(rig.tmp / "does-not-exist.yml"))
    assert "healthy=1" in r.stdout, r.stdout + r.stderr
    rig.run(PROVISIONING_FILE=str(rig.tmp / "does-not-exist.yml"))
    assert rig.sent == [], "a missing provisioning file was treated as a Grafana fault"

    # ...and the clause that survives it still fires. Without this the test above passes
    # for a script that simply never reports anything.
    rig.grafana.status, rig.grafana.body = 200, json.dumps({"groups": []}).encode()
    rig.run(PROVISIONING_FILE=str(rig.tmp / "does-not-exist.yml"))
    rig.run(PROVISIONING_FILE=str(rig.tmp / "does-not-exist.yml"))
    assert len(rig.sent) == 1, "with no expected count, an empty rules API still must page"


def test_a_failed_telegram_send_is_retried_rather_than_swallowed(rig) -> None:
    """🔑 The single most important behaviour here.

    If a failed POST marked the transition as reported, one unreachable-Telegram moment would
    silently eat the only message an edge-triggered watchdog ever sends — a watchdog that
    reports nothing and looks healthy, which is the defect class this whole file is about.
    """
    rig.broken()
    rig.telegram.status = 500  # Telegram refuses
    rig.run()
    r = rig.run()
    assert "send FAILED" in (r.stdout + r.stderr), r.stdout + r.stderr
    attempted = len(rig.telegram.hits)
    assert attempted >= 1, "it did not even try"

    rig.telegram.status = 200  # Telegram comes back
    rig.run()
    assert len(rig.sent) >= 1, "the transition was swallowed by the earlier failed send"


def test_dry_run_proves_detection_without_sending(rig) -> None:
    rig.broken()
    rig.run(DRY_RUN="1")
    r = rig.run(DRY_RUN="1")
    assert "DRY RUN" in r.stdout, r.stdout + r.stderr
    assert "would send" in r.stdout
    assert rig.telegram.hits == [], "DRY_RUN posted to Telegram"


def test_it_never_prints_the_bot_token(rig) -> None:
    rig.broken()
    rig.run(DRY_RUN="1")
    r = rig.run(DRY_RUN="1")
    assert "123:ABC" not in (r.stdout + r.stderr), "the bot token reached the log"


def test_it_exits_zero_even_when_grafana_is_broken(rig) -> None:
    """cron mails root on a non-zero exit. That would be a second, unthrottled channel for
    the very condition this script just throttled."""
    rig.broken()
    rig.run()
    r = rig.run()
    assert r.returncode == 0, r.stdout + r.stderr
    assert len(rig.sent) == 1


def test_the_metric_distinguishes_a_failed_call_from_an_empty_list(rig) -> None:
    """`-1` and `0` mean opposite things and both are falsy. Keep them apart."""
    rig.broken(status=500)
    rig.run()
    text = (rig.tmp / "textfile" / "datanika_grafana_watchdog.prom").read_text(encoding="utf-8")
    assert "datanika_grafana_rules_listed -1" in text, text

    rig.grafana.status, rig.grafana.body = 200, json.dumps({"groups": []}).encode()
    rig.run()
    text = (rig.tmp / "textfile" / "datanika_grafana_watchdog.prom").read_text(encoding="utf-8")
    assert "datanika_grafana_rules_listed 0" in text, text


def test_the_metric_is_documented_as_not_being_the_reporting_path(rig) -> None:
    """An alert rule on this metric would be a Grafana rule, i.e. the trap one level down.
    The HELP text is where a future reader meets it, so the warning lives there."""
    rig.run()
    text = (rig.tmp / "textfile" / "datanika_grafana_watchdog.prom").read_text(encoding="utf-8")
    assert "NOT the reporting path" in text, text


# ---------------------------------------------------------------------------------------------
# Installation. A watchdog nobody schedules is this bug one level up.
# ---------------------------------------------------------------------------------------------
def test_the_installer_installs_the_script_and_the_cron_file() -> None:
    text = INSTALLER.read_text(encoding="utf-8")
    assert "grafana-watchdog.sh" in text, (
        "install-server-scripts.sh does not install the watchdog — it would ship to "
        "/opt/datanika/datanika/deploy/server/, a path nothing reads (core#747)"
    )
    assert "datanika.cron" in text, "the cron file is not installed, so the job never runs"
    assert "INSTALL_CRON=(" in text


def test_the_cron_destination_drops_the_dot() -> None:
    """🚨 Cron silently ignores any file in /etc/cron.d whose name contains a dot.

    `/etc/cron.d/datanika.cron` would be present, correct, readable and never executed. The
    installer must strip the suffix, and must refuse a dotted destination outright.
    """
    text = INSTALLER.read_text(encoding="utf-8")
    assert "${name%.cron}" in text, "the installer does not strip the .cron suffix"
    assert "contains a dot" in text, "the installer does not refuse a dotted cron destination"


def test_every_cron_line_carries_a_user_field() -> None:
    """A /etc/cron.d line without the 6th field is skipped with a parse error in the log —
    scheduled, present, never executed."""
    lines = [
        ln
        for ln in CRON.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.lstrip().startswith("#") and not re.match(r"^[A-Z_]+=", ln)
    ]
    assert lines, "no schedule lines found in datanika.cron — this test checked nothing"
    for ln in lines:
        fields = ln.split()
        assert len(fields) >= 7, f"cron line has no user field: {ln!r}"
        assert fields[5] == "root", f"expected the user field to be root: {ln!r}"
        assert fields[6].startswith("/"), f"command must be an absolute path: {ln!r}"


def test_the_cron_file_schedules_the_watchdog_by_its_installed_path() -> None:
    text = CRON.read_text(encoding="utf-8")
    assert "/opt/datanika/scripts/grafana-watchdog.sh" in text, (
        "the cron file does not name the path install-server-scripts.sh writes to"
    )


def _run_installer(tmp_path: Path, crontab_lines: str | None, mutate=None):
    """Drive the REAL installer against temp directories and a fake `crontab`."""
    src = tmp_path / "src"
    shutil.copytree(ROOT / "deploy" / "server", src)
    if mutate:
        mutate(src)
    dest, crond, bindir = tmp_path / "scripts", tmp_path / "cron.d", tmp_path / "bin"
    for d in (dest, crond, bindir):
        d.mkdir()

    fake_crontab = bindir / "fake-crontab"
    body = "#!/usr/bin/env bash\n"
    body += "exit 1\n" if crontab_lines is None else f"cat <<'EOF'\n{crontab_lines}\nEOF\n"
    fake_crontab.write_text(body, encoding="utf-8", newline="\n")
    fake_crontab.chmod(0o755)

    r = subprocess.run(
        [BASH, str(INSTALLER)],
        env={
            **os.environ,
            "SRC_DIR": str(src),
            "DEST_DIR": str(dest),
            "CRON_DIR": str(crond),
            "CRONTAB": str(fake_crontab),
        },
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    return r, crond, dest


def test_the_installer_lands_the_cron_file_without_its_dot(tmp_path: Path) -> None:
    r, crond, dest = _run_installer(tmp_path, crontab_lines="")
    assert r.returncode == 0, r.stdout + r.stderr
    assert (crond / "datanika").is_file(), f"cron file not installed: {sorted(crond.iterdir())}"
    assert not (crond / "datanika.cron").exists(), (
        "installed as datanika.cron — cron ignores dotted names in /etc/cron.d, silently"
    )
    assert (dest / "grafana-watchdog.sh").is_file(), "the watchdog script was not installed"
    assert (crond / "datanika").read_text(encoding="utf-8") == (
        (ROOT / "deploy" / "server" / "datanika.cron").read_text(encoding="utf-8")
    )


def test_the_duplicate_schedule_guard_refuses_and_writes_nothing(tmp_path: Path) -> None:
    """🚨 Seen failing, not assumed. This is the guard that makes the five-entry migration
    safe to do in two steps, and it had never fired when it was written."""
    r, crond, _dest = _run_installer(
        tmp_path,
        crontab_lines=(
            REAL_CRONTAB + "\n*/2 * * * * /opt/datanika/scripts/grafana-watchdog.sh >/dev/null 2>&1"
        ),
    )
    assert r.returncode != 0, (
        "the installer accepted a cron file naming a command root's crontab already "
        "schedules — that command would run twice:\n" + r.stdout + r.stderr
    )
    out = r.stdout + r.stderr
    assert "grafana-watchdog.sh" in out, out
    assert "TWICE" in out, out
    assert not (crond / "datanika").exists(), (
        "it refused AND wrote the file anyway — the refusal must happen before the write"
    )


# The production box's root crontab, verbatim (read 2026-09-23). Redirections and all —
# 🚨 the `2>&1` is the whole point. An earlier version of the duplicate guard extracted every
# `/`-prefixed token from the cron file, which yields `2` (from `*/2`) and `null` (from
# `/dev/null`), and `2` is a substring of `2>&1`. That guard would have refused EVERY DEPLOY.
# It passed a hand-written fake crontab that happened to contain no digit 2.
REAL_CRONTAB = (
    "0 3 * * * /opt/datanika/scripts/backup-offsite.sh >> /var/log/datanika-backup.log 2>&1\n"
    "0 5 1 * * /opt/datanika/scripts/restore-drill.sh >> /var/log/datanika-restore-drill.log 2>&1\n"
    "*/5 * * * * /opt/datanika/scripts/export-prod-settings.sh >/dev/null 2>&1\n"
    "30 5 1 * * /opt/datanika/scripts/rebuild-parity-drill.sh"
    " >> /var/log/datanika-rebuild-parity.log 2>&1\n"
    "*/10 * * * * /opt/datanika/scripts/export-watchdog-freshness.sh >/dev/null 2>&1"
)


def test_the_guard_permits_the_real_production_crontab(tmp_path: Path) -> None:
    """🔑 A guard that refuses everything is not discriminating — and the obvious repair for
    "it refuses everything" is to loosen it until it permits the case it exists to catch.

    So this drives it with the actual crontab from the box rather than an invented one. It is
    the test that caught the `2`-matches-`2>&1` defect, and the reason it caught it is that
    the fake crontab it replaced was tidier than the real thing.
    """
    r, crond, _dest = _run_installer(tmp_path, crontab_lines=REAL_CRONTAB)
    assert r.returncode == 0, (
        "the guard refused the production crontab, which shares no command with the cron "
        "file. Every deploy would fail:\n" + r.stdout + r.stderr
    )
    assert (crond / "datanika").is_file()


def test_a_cron_line_missing_its_user_field_is_refused(tmp_path: Path) -> None:
    """Without the 6th field cron logs a parse error and skips the line: present, readable,
    never executed — the shape the whole cron-in-git change exists to end."""

    def drop_user(src: Path) -> None:
        p = src / "datanika.cron"
        p.write_text(
            p.read_text(encoding="utf-8").replace(
                "*/2 * * * * root /opt/datanika/scripts/grafana-watchdog.sh",
                "*/2 * * * * /opt/datanika/scripts/grafana-watchdog.sh",
            ),
            encoding="utf-8",
            newline="\n",
        )

    r, crond, _dest = _run_installer(tmp_path, crontab_lines="", mutate=drop_user)
    assert r.returncode != 0, r.stdout + r.stderr
    assert "user" in (r.stdout + r.stderr), r.stdout + r.stderr
    assert not (crond / "datanika").exists()


def test_the_cron_file_does_not_duplicate_the_hand_made_crontab_entries() -> None:
    """🚨 The five existing jobs live in root's crontab. Scheduling any of them here as well
    runs it twice — two concurrent pg_dump + off-site rsync at 03:00.

    The installer refuses this at deploy time; this catches it at PR time, which is cheaper.
    """
    body = "\n".join(
        ln
        for ln in CRON.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.lstrip().startswith("#")
    )
    for name in (
        "backup-offsite.sh",
        "restore-drill.sh",
        "export-prod-settings.sh",
        "rebuild-parity-drill.sh",
        "export-watchdog-freshness.sh",
    ):
        assert name not in body, (
            f"{name} is scheduled in datanika.cron AND in root's crontab on the box, so it "
            "would run twice. Remove it from the crontab first — see the migration note in "
            "deploy/server/datanika.cron."
        )

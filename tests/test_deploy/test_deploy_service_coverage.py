"""Every service in `docker-compose.yml` must be reachable by some deploy step (core#616).

Config that CD does not apply is config that is not deployed
------------------------------------------------------------
`deploy-pointer.yml` transfers the whole tree to the box and then names, service by
service, what to bring up. Anything it does not name keeps running with whatever config
it was started with — indefinitely. There is no error, no drift warning, and
`docker compose config` on the box reports the *new* file, so even looking at the box
tells you the change arrived.

Three services were in that state on 2026-08-30: `postgres-exporter`, `cadvisor` and
`node-exporter`, all "Up 6 weeks", none named in any deploy step.

This is not theoretical. core#616 is an alert rule that alerts on a metric the exporter
never emitted because a collector flag was never passed. The one-line fix is a `command:`
on `postgres-exporter` in `docker-compose.yml` — and shipping *only* that would have
changed nothing on the box, while the commit, a green deploy, and even the rule text read
back from Grafana's API all said the rule was fixed. A rule firing on nothing is
indistinguishable from a healthy system, so nobody would have found out.

Derived, not restated
---------------------
The list of covered services is parsed out of the deploy workflow and the blue/green
script rather than written down here, for the reason `test_deployment_manifest_parity.py`
gives: a restated list drifts, and drift is the bug being hunted. Add a service to
`docker-compose.yml` and this test fails until a deploy step names it.

Not in scope: whether the service *should* be recreated eagerly. Plain `up -d` is a
config-hash recreate — a no-op unless the definition changed — and that is the right
default. `--force-recreate` is for the three monitoring services whose single-FILE bind
mounts pin the inode.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yml"
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy-pointer.yml"
BLUEGREEN = ROOT / "scripts" / "deploy-bluegreen.sh"

# `docker compose ... up -d [flags] svc svc svc` — capture the tail, then drop flags.
_UP = re.compile(r"compose\b[^\n]*?\bup\b\s+(-d\b[^\n'\"]*)")
_FLAG = re.compile(r"^-")
# A shell variable as the service argument means the script picks it at runtime.
_SHELL_VAR = re.compile(r"^[\"']?\$")


def _strip_comments(text: str) -> str:
    """Drop comment lines before looking for commands.

    Not optional: both files explain themselves at length, and those explanations quote
    the very commands being searched for. The first run of this test failed on a service
    called `won`, parsed out of the prose

        # It also fixes a subtler bug: ... `compose up -d` won't recreate, and ...

    A parser aimed at a file that documents itself will read the documentation.

    Full-line comments only, plus a ` # ` mid-line comment marker. A `#` inside a quoted
    string would be mangled; neither file has one, and the alternative is a shell parser.
    """
    out = []
    for line in text.split("\n"):
        if line.lstrip().startswith("#"):
            continue
        out.append(line.split(" # ", 1)[0])
    return "\n".join(out)


def _services_named_in(text: str) -> set[str]:
    """Service names appearing as arguments to a `docker compose up -d`."""
    found: set[str] = set()
    for match in _UP.finditer(_strip_comments(text)):
        for token in match.group(1).split():
            if _FLAG.match(token) or _SHELL_VAR.match(token):
                continue
            if token in ("&&", "||", "|", ";", "\\"):
                break
            # `... up -d postgres redis app celery beat celery-exporter; then` — staging's
            # primary `up -d` is the condition of an `if`, so the command ends at a `;`
            # ATTACHED to the last service name rather than standing alone. Without this,
            # the set picked up `celery-exporter;`, `beat;` and `then`. Harmless for the
            # orphan direction (junk in `named` cannot hide a missing service) and wrong
            # for the reverse one, which asks whether every deployed name is a real
            # service. Prod's lines carry no `;`, so nothing there changes.
            name = token.strip("\"'")
            if name.endswith(";"):
                found.add(name[:-1])
                break
            found.add(name)
    found.discard("-d")
    return found


def _bluegreen_services() -> set[str]:
    """The colour pair, read off the script's own colour table.

    `deploy-bluegreen.sh` runs `up -d "$T_SVC"`, so the service name is a variable and
    cannot be read from the `up` line. It is assigned from lines shaped like
    `GREEN="app_b datanika-app-b 8010 3010"`, whose first field is the compose service.
    """
    text = _strip_comments(BLUEGREEN.read_text(encoding="utf-8"))
    return {m.group(1) for m in re.finditer(r'^\s*(?:BLUE|GREEN)="(\S+)\s', text, re.M)}


@pytest.fixture(scope="module")
def compose_services() -> set[str]:
    data = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    return set(data["services"])


@pytest.fixture(scope="module")
def deployed_services() -> set[str]:
    return _services_named_in(DEPLOY_WORKFLOW.read_text(encoding="utf-8")) | _bluegreen_services()


def test_the_parsers_actually_found_something(compose_services, deployed_services):
    """A parser that silently matches nothing turns this file green forever.

    Both regexes run against real files whose shape can change (a reformatted workflow,
    a rewritten colour table). If either stops matching, every assertion below passes
    vacuously — the exact silent-green failure the rest of this suite exists to prevent.
    """
    assert len(compose_services) >= 8, compose_services
    assert len(deployed_services) >= 8, deployed_services
    assert "app_b" in deployed_services, (
        "the blue/green colour table parser matched nothing; `_bluegreen_services` is "
        f"reading {BLUEGREEN.name} and got {sorted(deployed_services)}"
    )
    assert "postgres" in deployed_services, (
        f"the workflow `up -d` parser matched nothing useful; got {sorted(deployed_services)}"
    )


def test_deploy_covers_every_compose_service(compose_services, deployed_services):
    orphans = sorted(compose_services - deployed_services)
    assert not orphans, (
        "these services are defined in docker-compose.yml and named by NO deploy step:\n"
        + "".join(f"    {s}\n" for s in orphans)
        + "\nCD transfers the tree and then brings up services by name. A service it "
        "never names keeps its old running config forever — silently, and with the new "
        "file sitting on disk next to it, so the box looks correct.\n\n"
        "core#616: `postgres-exporter` sat like this for 6 weeks. Its alert rule "
        "(`pg-slow-queries`) alerted on a metric a missing collector flag meant it never "
        "exported, and a compose-only fix would have left the rule firing on nothing "
        "while every other signal said it shipped.\n\n"
        "Fix by adding the service to the `up -d` list in "
        f"{DEPLOY_WORKFLOW.name} (plain `up -d`: it is a no-op unless the service's own "
        "compose definition changed), or — if it genuinely must not be CD-managed — say "
        "so here with the reason."
    )


def test_deploy_names_no_service_compose_does_not_define(compose_services, deployed_services):
    """The other direction: a typo'd or renamed service in the deploy is a failed deploy.

    `docker compose up -d typo` exits non-zero, so this would surface as a broken deploy
    rather than silently — but it would surface *in production*, mid-deploy, which is the
    expensive place to learn it.
    """
    unknown = sorted(deployed_services - compose_services)
    assert not unknown, (
        "these service names appear in a deploy step but are not defined in "
        "docker-compose.yml:\n" + "".join(f"    {s}\n" for s in unknown)
    )


# ══════════════════════════════════════════════════════════════════════════════════════
# The same question, asked of STAGING (core#762)
# ══════════════════════════════════════════════════════════════════════════════════════
#
# Everything above guards exactly one file, because `COMPOSE` is a constant pointing at
# the prod manifest. A service already slipped through that gap: core#704 added
# `celery-exporter` to BOTH compose files, the prod deploy names it, and the tests above
# went green — while `deploy/staging/docker-compose.yml` described a container that
# nothing started. Naming services explicitly means only those start, so staging's
# exporter simply never existed.
#
# The shape is the reusable part: the prod half was caught by an automated guard within
# minutes and the staging half was not caught at all — not because anyone reasoned
# differently about it, but because the guard's `COMPOSE` constant pointed at one path.
# A guard's coverage is part of the guard.

STAGING_COMPOSE = ROOT / "deploy" / "staging" / "docker-compose.yml"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"

# Services deliberately present in the staging manifest and deliberately not started.
# ⚠️ An entry here is a claim that needs a reason, not a way to silence the test. The
# blanket alternative — exempting every service behind a `profiles:` key — would be much
# worse: `postgres-exporter`, `cadvisor`, `node-exporter` and `celery-exporter` are all
# profile-gated on PROD and requiring them is precisely the core#616 fix. Exempting
# profiles wholesale would undo it.
STAGING_NOT_DEPLOYED = {
    "app_b": (
        "core#596 decided against blue/green on staging, so the green colour is scaffolding "
        "the deploy must never start. It is kept in the manifest because the recreate step "
        "relies on it existing to justify NOT passing --remove-orphans: with the bluegreen "
        "profile inactive, compose would classify app_b as an orphan and delete it."
    ),
}


@pytest.fixture(scope="module")
def staging_compose_services() -> set[str]:
    data = yaml.safe_load(STAGING_COMPOSE.read_text(encoding="utf-8"))
    return set(data["services"])


@pytest.fixture(scope="module")
def staging_deployed_services() -> set[str]:
    """Parsed from ci.yml, which carries both the primary `up -d` and its recovery retry.

    Deliberately the whole workflow rather than a line range: the recovery path is a
    second `up -d` with `--force-recreate`, and core#762 landed with the exporter missing
    from exactly one of the two.
    """
    return _services_named_in(CI_WORKFLOW.read_text(encoding="utf-8"))


def test_the_staging_parsers_actually_found_something(
    staging_compose_services, staging_deployed_services
):
    """Same guard-the-guard as above: a parser that matches nothing is green forever."""
    assert len(staging_compose_services) >= 6, staging_compose_services
    assert len(staging_deployed_services) >= 5, staging_deployed_services
    assert "postgres" in staging_deployed_services, sorted(staging_deployed_services)
    assert set(STAGING_NOT_DEPLOYED) <= staging_compose_services, (
        "an entry in STAGING_NOT_DEPLOYED names a service the staging manifest no longer "
        "defines — delete the entry rather than carrying a stale excuse"
    )


def test_staging_deploy_covers_every_staging_compose_service(
    staging_compose_services, staging_deployed_services
):
    orphans = sorted(
        staging_compose_services - staging_deployed_services - set(STAGING_NOT_DEPLOYED)
    )
    assert not orphans, (
        "these services are defined in deploy/staging/docker-compose.yml and named by NO "
        "staging deploy step:\n" + "".join(f"    {s}\n" for s in orphans) + "\n"
        "They will never start, and nothing will fail — the file on disk simply describes "
        "a container that does not exist (core#762). Add them to BOTH `up -d` lines in "
        "ci.yml (the primary and the --force-recreate recovery), or record the reason in "
        "STAGING_NOT_DEPLOYED."
    )

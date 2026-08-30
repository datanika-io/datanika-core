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
            found.add(token.strip("\"'"))
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

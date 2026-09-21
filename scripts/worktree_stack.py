#!/usr/bin/env python3
"""Isolation for a local stack run from an agent worktree (core#1197).

On 2026-09-15 two agents' local stacks collided in three ways, and every one was SILENT (measured
by QA, recorded on core#1197):

1. One shared image tag. Every worktree built `:worktree`, so one agent's build re-pointed the
   tag under another agent's running stack -- and on a containerd image store a moved tag cannot
   be put back.
2. Fixed container names and default host ports. A stack revived by the Docker daemon answered
   the next agent's health check on `localhost:3100`.
3. Reflex's `api_url` defaults to `http://localhost:8000`. A second stack on offset ports still
   sends its browser websocket, and its uploads, to whichever backend holds host port 8000: a
   walk that writes into another agent's database while every page renders correctly.

This module makes isolation the default instead of a convention, and refuses a configuration
that is not isolated. It never parses compose YAML. It reads compose's own rendering
(`docker compose config --format json`), so the merge rules being checked are compose's.

    identity <core-dir> [--port-offset N]       agent, tag, project and offset as KEY=VALUE
    overlay --agent A --offset N < base.json    a compose overlay on stdout
    check   --agent A --offset N < full.json    exit 0 if isolated; exit 1 naming every problem

`scripts/worktree-stack.sh` is the entry point. The logic lives here so that it can be tested.
"""

from __future__ import annotations

import argparse
import errno
import json
import re
import socket
import sys
from pathlib import Path

IMAGE_REPO = "ghcr.io/datanika-io/datanika-core"
PROJECT_PREFIX = "wt-"
BAND = 10_000
# One host-port band per department, so two departments' stacks cannot overlap even when both
# run every service. QA's band is the offset its hand-built `qawalk` stack already used.
OFFSETS = {
    "qa": 10_000,
    "growth": 20_000,
    "infra": 30_000,
    "engineering": 40_000,
    "product": 50_000,
}
# The highest default host port is 9810, and 9810 + 50000 still fits below 65536.
MAX_OFFSET = 50_000
FRONTEND_PORT = 3000
BACKEND_PORT = 8000


class RefusalError(Exception):
    """An identity or a configuration that must not be used. The message says why."""


def identity(core_dir: Path, port_offset: int | None = None) -> dict[str, str]:
    """Derive this worktree's tag, compose project and host-port band from its directory name."""
    name = core_dir.name
    if name == "datanika":
        agent = "main"
    elif name.startswith("datanika-core-") and len(name) > len("datanika-core-"):
        agent = name[len("datanika-core-") :]
    else:
        raise RefusalError(
            f"'{name}' is not 'datanika' or 'datanika-core-<agent>', so no identity can be derived"
        )
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", agent):
        raise RefusalError(f"'{agent}' cannot be used in a compose project name")
    offset = port_offset if port_offset is not None else OFFSETS.get(agent)
    if offset is None:
        raise RefusalError(
            f"no host-port band is assigned to '{agent}': pass --port-offset N, a multiple of "
            f"{BAND} that no other worktree uses"
        )
    if offset % BAND or not BAND <= offset <= MAX_OFFSET:
        raise RefusalError(
            f"port offset {offset} must be a multiple of {BAND} from {BAND} to {MAX_OFFSET}"
        )
    return {
        "agent": agent,
        "tag": f"{IMAGE_REPO}:worktree-{agent}",
        "project": f"{PROJECT_PREFIX}{agent}",
        "offset": str(offset),
    }


def _published(service: dict) -> list[dict]:
    return [p for p in service.get("ports") or [] if isinstance(p, dict) and p.get("published")]


def overlay(base: dict, agent: str, offset: int, probe=None) -> str:
    """A compose overlay: every container renamed, every published port moved into the band.

    ⚠️ ``probe`` is **opt-in and means something different here than in ``unbindable``**:
    ``None`` leaves every port at ``natural = published + offset``, which is the pure form
    every existing caller and test relies on. Pass a probe (the CLI passes ``probe_bind``)
    to additionally relocate ports this host refuses — see ``allocate_ports``.
    """
    project = f"{PROJECT_PREFIX}{agent}"
    services = base.get("services") or {}
    if not services:
        raise RefusalError("the rendered base configuration has no services")
    # None -> identity, so the pure form is byte-for-byte what it always was.
    resolved = allocate_ports(base, offset, probe=probe) if probe is not None else {}
    lines = [
        f"# scripts/worktree_stack.py overlay for '{agent}' (core#1197).",
        "# Streamed to compose on stdin and never written to disk.",
        f"name: {project}",
        "services:",
    ]
    for name in sorted(services):
        lines.append(f"  {name}:")
        lines.append(f"    container_name: {project}-{name}")
        ports = _published(services[name])
        if not ports:
            continue
        lines.append("    ports: !override")
        host_port_for: dict[int, int] = {}
        for port in ports:
            published = int(port["published"]) + offset
            if published > 65_535:
                raise RefusalError(
                    f"{name}: host port {port['published']} + {offset} exceeds 65535"
                )
            published = resolved.get(published, published)
            protocol = port.get("protocol") or "tcp"
            suffix = "" if protocol == "tcp" else f"/{protocol}"
            lines.append(f'      - "127.0.0.1:{published}:{port["target"]}{suffix}"')
            host_port_for[int(port["target"])] = published
        if FRONTEND_PORT in host_port_for and BACKEND_PORT in host_port_for:
            # A Reflex server: the browser must be sent to THIS stack's backend.
            backend = host_port_for[BACKEND_PORT]
            frontend = host_port_for[FRONTEND_PORT]
            lines.append("    environment:")
            lines.append(f'      REFLEX_API_URL: "http://localhost:{backend}"')
            lines.append(f'      REFLEX_DEPLOY_URL: "http://localhost:{frontend}"')
    return "\n".join(lines) + "\n"


def check(full: dict, agent: str, offset: int) -> tuple[list[str], list[str]]:
    """Return (report lines, problems) for a merged rendering. No problems means isolated.

    The report never prints an environment value other than REFLEX_API_URL: a rendered
    configuration carries the local credentials.
    """
    project = f"{PROJECT_PREFIX}{agent}"
    want_image = f"{IMAGE_REPO}:worktree-{agent}"
    band = f"{offset}-{offset + BAND - 1}"
    report: list[str] = []
    problems: list[str] = []
    if full.get("name") != project:
        problems.append(
            f"the project is {full.get('name')!r}, not {project!r}: its volumes and network "
            "would belong to whichever stack owns that name"
        )
    services = full.get("services") or {}
    if "app" not in services:
        problems.append("no 'app' service was rendered, so there is nothing to call isolated")
    for name in sorted(services):
        service = services[name]
        container = service.get("container_name")
        ports = _published(service)
        shown = ", ".join(
            f"{p.get('host_ip') or '*'}:{p['published']}->{p['target']}" for p in ports
        )
        report.append(f"  {name:<18} {str(container):<30} {shown or '-'}")
        if not str(container or "").startswith(f"{project}-"):
            problems.append(f"{name}: container name {container!r} is not under '{project}-'")
        host_port_for: dict[int, int] = {}
        for port in ports:
            published = int(port["published"])
            host_port_for[int(port["target"])] = published
            if not offset <= published < offset + BAND:
                problems.append(f"{name}: host port {published} is outside {agent}'s band {band}")
            if port.get("host_ip") != "127.0.0.1":
                where = port.get("host_ip") or "every interface"
                problems.append(f"{name}: host port {published} is bound to {where}, not 127.0.0.1")
        if FRONTEND_PORT in host_port_for and BACKEND_PORT in host_port_for:
            want = f"http://localhost:{host_port_for[BACKEND_PORT]}"
            got = (service.get("environment") or {}).get("REFLEX_API_URL")
            report.append(f"  {'':<18} REFLEX_API_URL={got}")
            if got != want:
                problems.append(
                    f"{name}: REFLEX_API_URL is {got!r}, not {want!r}; the browser would reach "
                    "whichever backend holds that port"
                )
        image = str(service.get("image") or "")
        if image.startswith(f"{IMAGE_REPO}:") and image != want_image:
            problems.append(f"{name}: image {image!r} is not this worktree's build {want_image!r}")
    return report, problems


# A bind can fail for two reasons that docker reports with one sentence, and they need
# opposite responses (core#1481).
#
# Already held — very often by THIS stack, already up. Refusing on it would make a live
# stack impossible to re-`up`, which is worse than the defect. ⚠️ The POSIX constant is not
# enough on Windows: the value observed there is 10048 (WSAEADDRINUSE) while
# `errno.EADDRINUSE` is 100, so comparing against the constant alone silently misses it.
IN_USE_ERRNOS = frozenset({errno.EADDRINUSE, 10048, 98})
# The OS refuses the port outright. On Windows these are WinNAT's reserved ranges
# (`netsh interface ipv4 show excludedportrange protocol=tcp`), and they MOVE BETWEEN
# REBOOTS — so this appears and disappears with nothing in the repository changing.
RESERVED_ERRNOS = frozenset({errno.EACCES, 13, 10013})


def probe_bind(host_ip: str, port: int) -> int | None:
    """Return the errno if this host port cannot be bound, or ``None`` if it can.

    Binds and closes immediately; never listens, so it cannot answer a request or be
    mistaken for the service. ``host_ip`` is whatever the rendering publishes on.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((host_ip or "127.0.0.1", port))
        return None
    except OSError as failure:
        return failure.errno
    finally:
        sock.close()


def unbindable(full: dict, probe=None) -> list[str]:
    """Host ports the OS will REFUSE, as distinct from ports merely in use.

    Returns one problem per refused port — **not** stopping at the first, because docker
    reports only the port it trips on first and that is what made this take two passes to
    diagnose.

    ``probe`` is injected so the reserved-port arm can be tested on any machine. A test
    that bound real sockets would never execute that arm on a runner where every port is
    free, leaving the guard green everywhere and unable to fail on the one host it exists
    for.
    """
    probe = probe or probe_bind
    problems: list[str] = []
    for name in sorted(full.get("services") or {}):
        for port in _published((full["services"] or {})[name]):
            published = int(port["published"])
            host_ip = port.get("host_ip") or "127.0.0.1"
            failure = probe(host_ip, published)
            if failure is None or failure in IN_USE_ERRNOS:
                continue
            if failure in RESERVED_ERRNOS:
                problems.append(
                    f"{name}: the OS refuses host port {published} (errno {failure}); it is "
                    f"inside a reserved range, so `up` would start some services and die on "
                    f"this one. List the ranges with "
                    f"`netsh interface ipv4 show excludedportrange protocol=tcp` — they are "
                    f"WinNAT's and move between reboots. Port {published}"
                )
            else:
                problems.append(
                    f"{name}: host port {published} could not be bound and the reason is not "
                    f"recognised (errno {failure}). Refusing rather than guessing — see "
                    f"`netsh interface ipv4 show excludedportrange protocol=tcp`. "
                    f"Port {published}"
                )
    return problems


def allocate_ports(base: dict, offset: int, probe=None) -> dict[int, int]:
    """Map each natural host port to one this host will actually let us bind (core#1481).

    Returns ``{natural: resolved}``. A port the OS refuses outright is moved **inside the
    department's band**, so isolation is preserved: leaving the band is the cross-agent
    collision this module exists to prevent, and ``--port-offset`` cannot help because it
    selects another department's whole band.

    🔑 **The two halves are deliberately not symmetric.**

    * **reserved (``EACCES``)** — the OS will never allow it. Relocate.
    * **in use (``EADDRINUSE``)** — very often *this stack, already up*. **Keep it.**
      Relocating here would hand a running stack a new set of ports on every ``up``, which
      breaks the re-``up`` the pre-flight was careful to preserve.

    ⚠️ **Nothing here encodes a range.** WinNAT's reserved ranges move between reboots, so
    the host is asked every run. A hardcoded range would pass against today's values and
    fail the moment they move — which is the failure this function exists to survive.
    """
    probe = probe or probe_bind
    services = base.get("services") or {}
    natural: list[int] = []
    for name in sorted(services):
        for port in _published(services[name]):
            want = int(port["published"]) + offset
            if want > 65_535:
                raise RefusalError(
                    f"{name}: host port {port['published']} + {offset} exceeds 65535"
                )
            natural.append(want)

    answers: dict[int, int | None] = {}

    def refused(port: int) -> int | None:
        """Ask once per port. A second answer could differ, which would make the mapping
        non-deterministic inside a single run."""
        if port not in answers:
            answers[port] = probe("127.0.0.1", port)
        return answers[port]

    low = offset
    high = offset + BAND - 1
    # Every natural port is spoken for by whoever owns it, even if that service has not
    # started yet: a candidate that is free *now* because its owner is not up is not free.
    spoken_for = set(natural)
    resolved: dict[int, int] = {}

    for want in natural:
        failure = refused(want)
        if failure is None or failure in IN_USE_ERRNOS:
            resolved[want] = want
            continue
        replacement = None
        # Deterministic scan: upward from the natural port, wrapping inside the band.
        for candidate in [*range(want + 1, high + 1), *range(low, want)]:
            if candidate in spoken_for:
                continue
            if refused(candidate) is None:
                replacement = candidate
                break
        if replacement is None:
            raise RefusalError(
                f"no bindable host port left in the band {low}-{high} for {want}: every "
                f"candidate is reserved by the OS or already taken. List the reserved "
                f"ranges with `netsh interface ipv4 show excludedportrange protocol=tcp`."
            )
        resolved[want] = replacement
        spoken_for.add(replacement)
    return resolved


def _stdin_json() -> dict:
    data = sys.stdin.buffer.read()
    if not data.strip():
        raise RefusalError(
            "nothing arrived on stdin; expected `docker compose config --format json`"
        )
    return json.loads(data.decode("utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Isolation for a worktree's stack (core#1197)")
    commands = parser.add_subparsers(dest="command", required=True)
    who = commands.add_parser("identity")
    who.add_argument("core_dir")
    who.add_argument("--port-offset", type=int)
    for command in ("overlay", "check"):
        each = commands.add_parser(command)
        each.add_argument("--agent", required=True)
        each.add_argument("--offset", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "identity":
            found = identity(Path(args.core_dir).resolve(), args.port_offset)
            for key, value in found.items():
                print(f"{key}={value}")
            return 0
        document = _stdin_json()
        if args.command == "overlay":
            # core#1481. The CLI is the real-host path, so it asks the host: a port inside
            # a reserved range is relocated within the band rather than published and left
            # to fail halfway through `up`. The pure form (no probe) is still what the unit
            # tests use.
            sys.stdout.write(overlay(document, args.agent, args.offset, probe=probe_bind))
            return 0
        report, problems = check(document, args.agent, args.offset)
        # core#1481. A port the OS refuses outright is not an isolation defect, but it has
        # the same consequence and must be caught in the same place: BEFORE `up` starts
        # anything. A pre-flight nothing calls is the same bug one level up, and
        # `TestTheCheckActuallyRunsIt` exists because that is invisible from the function.
        problems = problems + unbindable(document)
    except RefusalError as refusal:
        print(f"REFUSED: {refusal}", file=sys.stderr)
        return 1
    services = document.get("services") or {}
    print(f"check: project {document.get('name')!r}, {len(services)} service(s)")
    print("\n".join(report))
    if problems:
        print(f"NOT ISOLATED -- {len(problems)} problem(s):", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    print("isolated: every service has its own container name, host-port band and backend URL")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""The published self-hosting quickstart must actually work (core#736).

The four-line quickstart on https://datanika.io/docs/self-hosting is the first thing a
self-hoster runs, and its last line failed:

    git clone …; cd datanika-core; cp .env.example .env; docker compose up -d
    -> env file …/.env.docker not found

Nothing executed the documented sequence, so every check stayed green while the entry
point to the open-source core had never worked.

Two channels, and `env_file:` only feeds one of them
----------------------------------------------------
Compose resolves a manifest through two independent mechanisms:

* ``${VAR}`` **interpolation** in the YAML, resolved from the shell environment or from
  the project ``.env`` file — *never* from ``env_file:``;
* ``env_file:``, which populates the **container's** environment and is invisible to
  interpolation.

That distinction is the whole bug, and it is why the fix is not the obvious one-word
change. Measured from a clean directory, with none of the variables exported:

===========================================  ==========================================
 what the user does                           result
===========================================  ==========================================
 ``cp .env.example .env`` (as published)       fails: ``.env.docker not found``
 ``cp .env.example .env.docker``               **still fails**: ``required variable
                                               GRAFANA_ADMIN_PASSWORD is missing``
 both files                                    exit 0
===========================================  ==========================================

The middle row is the trap. It is the remedy core#736 originally proposed and recorded as
verified, and it is wrong — it renders correctly only in a shell that has already sourced
the file, which is exactly what the production deploy does
(``set -a && . ./.env.docker && set +a``) and exactly what a new self-hoster has not done.
An instrument that repairs the state on its way to reading it cannot audit that state.

So this module asserts **both** channels independently. `test_documented_env_file_is_accepted`
covers the container-environment half; `test_strictly_required_variables_are_in_env_example`
covers the interpolation half. Either alone goes green on a broken quickstart.

Derived, not restated
---------------------
Nothing here hardcodes a service list or a variable list. The required variables are
scanned out of the manifest text and the application services are derived from the
manifest's own structure, so adding a service or a ``${NEW:?}`` makes this fail until the
quickstart is made to cover it. A restated list drifts, and drift is the bug being hunted
(same reasoning as `test_deploy_service_coverage.py`).
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"

# The file the published quickstart tells a self-hoster to create. This is the one string
# in the module that is a *contract with the docs* rather than something derivable from
# the repo: datanika-landing/src/pages/docs/self-hosting.astro says `cp .env.example .env`.
DOCUMENTED_ENV_FILE = ".env"

# `${VAR:?msg}` / `${VAR}` — interpolation. `$$VAR` is an escaped literal for the
# container's own shell (postgres' healthcheck uses it) and must NOT be collected, hence
# the lookbehind. `${VAR:-default}` and `${VAR-default}` supply their own value and so are
# not required of the user.
_INTERPOLATION = re.compile(r"(?<!\$)\$\{([A-Za-z_][A-Za-z0-9_]*)(:?[-?])?")


def _compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


def _strip_comment_lines(text: str) -> str:
    """Drop whole comment lines before scanning for interpolation.

    The manifest explains itself at length, and those explanations quote the very syntax
    being searched for — the header note added by core#736 contains a literal ``${VAR}``
    as prose. Without this, that comment is collected as a required variable named `VAR`
    and the test fails against a perfectly good file. (`test_deploy_service_coverage.py`
    hit the identical trap and reported a service called `won`.)

    Whole lines only, never `#`-to-end-of-line: a real value contains one. The
    ``REFLEX_REDIS_URL`` default carries ``(#646)`` *inside* its `:?` message, and
    truncating there would silently drop the variable this test exists to notice.
    """
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def _env_file_paths(service: dict) -> list[str]:
    """Normalise `env_file:` to a list of paths.

    Compose accepts a bare string, a list of strings, and (since v2.24) a list of
    `{path:, required:}` mappings. All three shapes appear in the wild.
    """
    raw = service.get("env_file")
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    out = []
    for entry in raw:
        out.append(entry["path"] if isinstance(entry, dict) else entry)
    return out


def _default_profile_services(compose: dict) -> dict[str, dict]:
    """Services a plain `docker compose up -d` starts: those with no `profiles:` key."""
    return {name: svc for name, svc in compose["services"].items() if not svc.get("profiles")}


def _application_services(compose: dict) -> set[str]:
    """Derive the application set from the manifest's own structure.

    Two rules, both structural:
      * a service that `build:`s from our Dockerfile is ours (app, app_b, celery, beat);
      * a service one of those `depends_on` is a datastore the app cannot start without
        (postgres, redis).

    Anything else — an exporter, a scraper, a dashboard — is an operator's choice and
    belongs behind a profile. Deriving it this way means a newly added sidecar fails this
    test rather than silently joining the default `up`.
    """
    services = compose["services"]
    ours = {name for name, svc in services.items() if "build" in svc}
    datastores: set[str] = set()
    for name in ours:
        datastores.update(services[name].get("depends_on", {}) or {})
    return ours | datastores


def test_documented_env_file_is_accepted() -> None:
    """Every `env_file:` must accept the file the published quickstart creates.

    This is the container-environment half. A missing `env_file` is fatal to the whole
    `up`, not just to the service declaring it, so one stale entry takes the entire
    quickstart down.
    """
    compose = _compose()
    offenders = {
        name: paths
        for name, svc in compose["services"].items()
        if (paths := _env_file_paths(svc)) and DOCUMENTED_ENV_FILE not in paths
    }
    assert not offenders, (
        f"These services do not accept {DOCUMENTED_ENV_FILE!r}, which is what the "
        f"published quickstart tells a self-hoster to create: {offenders}. "
        "Either accept it (an optional `- path:`/`required: false` entry alongside the "
        "production filename) or change the docs — but the two must agree."
    )


def test_env_file_entries_are_optional_so_either_filename_works() -> None:
    """Production keeps `.env.docker`; a self-hoster creates `.env`. Neither may be fatal.

    The deploy preserves the box's `.env.docker` and never ships one, so it is absent from
    a fresh clone. Listing both filenames only helps if a missing one is tolerated.
    """
    compose = _compose()
    bad: dict[str, list] = {}
    for name, svc in compose["services"].items():
        raw = svc.get("env_file")
        if not raw or isinstance(raw, str):
            if raw:
                bad[name] = raw
            continue
        for entry in raw:
            if not isinstance(entry, dict) or entry.get("required") is not False:
                bad.setdefault(name, []).append(entry)
    assert not bad, (
        "Every `env_file:` entry must be `{path: …, required: false}` so that whichever "
        f"filename is absent does not abort the run: {bad}"
    )


def test_strictly_required_variables_are_in_env_example() -> None:
    """Every interpolated variable without a default must ship a value in `.env.example`.

    This is the interpolation half, and it is the assertion that catches the class of bug
    core#736 turned out to be. `env_file:` cannot satisfy these; only the shell or the
    project `.env` can. A `${NEW_THING:?}` added to the manifest without a matching line
    in `.env.example` breaks the documented quickstart and nothing else would notice.
    """
    text = _strip_comment_lines(COMPOSE.read_text(encoding="utf-8"))
    required = {
        name
        for name, modifier in _INTERPOLATION.findall(text)
        # `:-` / `-` supply a default; `:?` / `?` / bare do not.
        if modifier not in (":-", "-")
    }
    provided = {
        line.split("=", 1)[0].strip()
        for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    }
    missing = sorted(required - provided)
    assert not missing, (
        f"{ENV_EXAMPLE.name} has no value for {missing}, so the documented "
        "`cp .env.example .env && docker compose up -d` aborts before starting anything. "
        "These are interpolated variables — putting them in .env.docker does not help."
    )


def test_datastore_urls_point_at_compose_services_not_localhost() -> None:
    """`.env.example` becomes the *container's* environment, where localhost is the container.

    A URL naming `localhost` here is not a route to the host machine — it is the app
    talking to itself, so `docker compose up -d` renders cleanly and then the app cannot
    reach its own database. Measured from inside the compose network: `-h postgres`
    answers `1`, `-h localhost` answers `Connection refused`.

    Derived from the manifest: the allowed hostnames are whatever services
    `docker-compose.yml` defines. The Helm chart already gets this right
    (`datanika.postgresHost` / `datanika.redisHost` in secret.yaml); only the Compose path
    shipped localhost.
    """
    services = set(_compose()["services"])
    offenders: dict[str, str] = {}
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        if line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if not key.strip().endswith("_URL"):
            continue
        host = re.search(r"//(?:[^@/]*@)?([A-Za-z0-9_.-]+):\d+", value)
        if host and host.group(1) not in services:
            offenders[key.strip()] = host.group(1)
    assert not offenders, (
        f"{ENV_EXAMPLE.name} points these at a host that is not a Compose service: "
        f"{offenders}. Inside a container that name does not reach the datastore; use the "
        f"service name. Known services: {sorted(services)}"
    )


def test_redis_url_carries_the_password_redis_demands() -> None:
    """If Redis starts with `--requirepass`, every URL for it must carry credentials.

    Not cosmetic, and not caught by anything else. `REDIS_URL` is interpolated into
    `REFLEX_REDIS_URL`, and **Reflex does not fail on a Redis it cannot reach** — it falls
    back to a per-process session store while `GRANIAN_WORKERS: "4"` runs four processes,
    so a reconnect landing on another worker is served a stale session. In this app that
    is a logout: 48% of production reconnects before core#646.

    So a credential-less `REDIS_URL` ships a silent S1 to every self-hoster. Verified
    against redis-py — the client Celery and Reflex actually use, not `redis-cli`, whose
    `-u` flag does not send the URI password and answers `NOAUTH` for a *correct* one:
    with credentials `ping -> True`; without, `AuthenticationError`.
    """
    compose = _compose()
    redis_cmd = str(compose["services"].get("redis", {}).get("command", ""))
    if "--requirepass" not in redis_cmd:
        return  # No auth demanded; nothing to assert.
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        if line.startswith("REDIS_URL="):
            value = line.split("=", 1)[1]
            assert re.match(r"redis://[^@/]*:[^@/]+@", value), (
                "redis starts with --requirepass, so REDIS_URL must carry the password "
                f"(redis://:PASSWORD@redis:6379/0). Got: {value.split('@')[-1]!r} with no "
                "credentials. Reflex will silently fall back to per-process session state."
            )
            return
    raise AssertionError(f"{ENV_EXAMPLE.name} defines no REDIS_URL at all.")


def test_plain_up_starts_the_application_not_the_observability_stack() -> None:
    """`docker compose up -d` must start the app, not eleven services.

    Six of the eleven were the production monitoring stack, which also made
    `GRAFANA_ADMIN_PASSWORD` mandatory for someone who never asked for Grafana —
    Compose interpolates the whole file before starting anything.
    """
    compose = _compose()
    unexpected = sorted(set(_default_profile_services(compose)) - _application_services(compose))
    assert not unexpected, (
        f"A plain `up -d` would start {unexpected}, which the application does not need. "
        "Put operator-optional services behind a profile "
        '(`profiles: ["monitoring"]`). Naming a profiled service explicitly still starts '
        "it, so the deploy's own `up -d … postgres-exporter cadvisor node-exporter` is "
        "unaffected."
    )


def test_default_profile_does_not_reach_outside_this_repository() -> None:
    """A self-hoster clones only `datanika-core`; nothing default may mount a sibling repo.

    `grafana` bind-mounted `../datanika-cloud/monitoring/*.json`. `datanika-cloud` is
    private, so for a self-hoster those paths do not exist — and Docker creates a
    *directory* at a missing bind-mount source, leaving Grafana reading directories where
    it expects dashboard JSON. Silent, and only visible once you look at Grafana.
    """
    compose = _compose()
    offenders: dict[str, list[str]] = {}
    for name, svc in _default_profile_services(compose).items():
        for volume in svc.get("volumes", []) or []:
            source = volume.split(":", 1)[0] if isinstance(volume, str) else ""
            if source.startswith(".."):
                offenders.setdefault(name, []).append(volume)
    assert not offenders, (
        f"Default-profile services mount paths outside this repository: {offenders}. "
        "A fresh clone of datanika-core alone does not have them."
    )

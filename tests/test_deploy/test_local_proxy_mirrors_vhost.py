"""The local proxy must route exactly what the deployed vhost routes (core#1197).

Why this test is the point of the sidecar, not an extra
------------------------------------------------------
The local stack is worth having *because it is production-shaped*. The moment its route
table drifts from `deploy/server/apache-app.datanika.io.conf`, a local green stops being
evidence about anything, and it stops being so **silently** — a mis-routed path does not
error, it returns the Reflex SPA with **HTTP 200**.

That is not hypothetical. Measured on 2026-09-10 against a proxy-less local stack:

    localhost:3000/api/v1/connections -> 200 text/html         (the SPA)
    localhost:8000/api/v1/connections -> 401 application/json  (the API)

The gating suite failed 33 of 62 that way. It failed only because those particular
assertions test a leak condition HTML cannot satisfy; an assertion shaped
``expect(status).not.toBe(200)`` would have **passed against the SPA**. So a routing
regression here lands directly on the tenant-isolation suite — the specs whose whole job is
proving org A cannot read org B — and turns them green.

Anti-vacuity
------------
Both parsers have a floor. A regex that silently matches nothing would otherwise make
"the two sets are equal" true by both being empty, which is the failure mode this whole
session kept finding in other people's checks.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
VHOST = ROOT / "deploy" / "server" / "apache-app.datanika.io.conf"
NGINX = ROOT / "deploy" / "local" / "nginx-local-proxy.conf"
LOCAL_COMPOSE = ROOT / "docker-compose.local.yml"
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy-pointer.yml"

#: Paths Apache sends to the backend but which are not ``ProxyPass`` lines — ``/_event``
#: goes through ``RewriteRule … [P]`` because 2.4.52's ``upgrade=websocket`` is broken on
#: that box. Parsing only ``ProxyPass`` would miss it and call the sets equal.
_REWRITE_BACKEND = re.compile(r"^\s*RewriteRule\s+\^/?\\?(?P<path>[\w/_.-]+)", re.M)


def _apache_backend_paths() -> set[str]:
    text = VHOST.read_text(encoding="utf-8")
    paths: set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        m = re.match(r"ProxyPass\s+(?P<path>/\S+)\s+http://", stripped)
        if m and "DATANIKA_BE" in stripped:
            paths.add(m.group("path").rstrip("/") or "/")
    # /_event is routed by RewriteRule [P], not ProxyPass.
    if re.search(r"RewriteRule\s+\^/_event", text):
        paths.add("/_event")
    return paths


def _nginx_backend_paths() -> set[str]:
    text = NGINX.read_text(encoding="utf-8")
    paths: set[str] = set()
    # A `location <path> { … proxy_pass http://datanika_backend; … }` block.
    for m in re.finditer(r"location\s+(?P<path>/\S*)\s*\{(?P<body>[^}]*)\}", text, re.S):
        if "datanika_backend" in m.group("body"):
            p = m.group("path").rstrip("/") or "/"
            paths.add(p)
    return paths


# --------------------------------------------------------------------------------------
# Anti-vacuity. These run first and are why a green below means something.
# --------------------------------------------------------------------------------------


def test_both_files_exist() -> None:
    assert VHOST.is_file(), f"{VHOST} missing"
    assert NGINX.is_file(), f"{NGINX} missing"


def test_the_apache_parser_finds_routes() -> None:
    """An empty set would make the equality assertion below trivially true."""
    found = _apache_backend_paths()
    assert len(found) >= 8, f"vhost parser found only {found} — the parser is broken"
    assert "/api" in found
    assert "/_event" in found, "the RewriteRule [P] websocket route was not picked up"


def test_the_nginx_parser_finds_routes() -> None:
    found = _nginx_backend_paths()
    assert len(found) >= 8, f"nginx parser found only {found} — the parser is broken"
    assert "/api" in found


# --------------------------------------------------------------------------------------
# The contract.
# --------------------------------------------------------------------------------------


def test_local_proxy_routes_exactly_what_the_vhost_routes() -> None:
    apache = _apache_backend_paths()
    nginx = _nginx_backend_paths()
    missing = apache - nginx
    extra = nginx - apache
    assert not missing, (
        f"the deployed vhost sends {sorted(missing)} to the backend and the local proxy "
        "does not. Local is no longer production-shaped: those paths will resolve to the "
        "Reflex SPA with HTTP 200, which is a PASS for a wrongly-shaped assertion."
    )
    assert not extra, (
        f"the local proxy sends {sorted(extra)} to the backend and the deployed vhost "
        "does not. Local would pass specs that staging fails."
    )


def test_oauth_consent_stays_on_the_frontend_in_both() -> None:
    """It is a Reflex page (`app.add_page route="/oauth/consent"`), not a backend route.

    Routing it to the backend breaks the consent screen, and the vhost carries an explicit
    warning about exactly this. A local proxy that got it wrong would let a change through
    that staging then rejects.
    """
    assert "/oauth/consent" not in _apache_backend_paths()
    assert "/oauth/consent" not in _nginx_backend_paths()


# --------------------------------------------------------------------------------------
# The inverse of the two docker-compose.yml guards.
#
# `test_deploy_service_coverage.py` and `test_selfhost_quickstart.py` both read
# `docker-compose.yml` only, so this overlay sits outside their reach. That is deliberate —
# a local-only proxy belongs in no deploy step — but it means the containment has to be
# asserted here, or the separate file becomes a way to sneak a service past both.
# --------------------------------------------------------------------------------------


def test_the_local_overlay_is_never_named_by_a_deploy_step() -> None:
    if not DEPLOY_WORKFLOW.is_file():  # pragma: no cover - workflow always present
        return
    text = DEPLOY_WORKFLOW.read_text(encoding="utf-8")
    assert "docker-compose.local.yml" not in text, (
        "docker-compose.local.yml is referenced by the deploy workflow. It is a "
        "development overlay and must never reach a server."
    )
    assert "deploy/local/" not in text, (
        "deploy/local/ is referenced by the deploy workflow. Nothing in that directory "
        "is installed on a server."
    )


def test_the_local_overlay_defines_only_the_proxy() -> None:
    """It must not redefine app/celery/postgres — an overlay that silently changed the
    application's own definition would make local diverge in a way nothing else checks."""
    data = yaml.safe_load(LOCAL_COMPOSE.read_text(encoding="utf-8"))
    services = set(data.get("services", {}))
    assert services == {"proxy"}, (
        f"docker-compose.local.yml defines {sorted(services)}. It may define only 'proxy'; "
        "anything else changes the application's shape locally without any guard noticing."
    )


def test_the_proxy_healthcheck_asks_through_the_proxy() -> None:
    """A healthcheck on nginx's own port that never reaches the backend would report
    healthy for a proxy that routes nothing — the same class of defect as the SPA
    answering 200."""
    data = yaml.safe_load(LOCAL_COMPOSE.read_text(encoding="utf-8"))
    test = data["services"]["proxy"]["healthcheck"]["test"]
    joined = " ".join(test)
    assert "/healthz" in joined, "the healthcheck must exercise a BACKEND-routed path"

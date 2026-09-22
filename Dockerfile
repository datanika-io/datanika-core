# Two editions, one file (core#1014).
#
#   DATANIKA_IMAGE_EDITION=cloud  (DEFAULT)  core + the private datanika-cloud plugin at /cloud
#   DATANIKA_IMAGE_EDITION=core              AGPL core alone, no /cloud tree at all
#
# WHY A BUILD ARG AND NOT A SECOND DOCKERFILE. `COPY datanika-cloud/ /cloud/`
# hard-fails when the tree is absent and Docker has no conditional COPY, so the
# edition has to be a stage selection. A second Dockerfile would work and is the
# wrong shape: this file carries three build-time assertions (the VCS check, the
# uv-cache check, the /mcp import check) and duplicating it is how one copy
# silently loses a guard. Both editions descend from the SAME `final` stage, so
# every assertion below runs in both by construction, not by upkeep.
#
# WHY THE DEFAULT IS `cloud`. `docker-compose.yml` and `deploy-pointer.yml` pass
# no build arg. The default is what production builds, so an unqualified
# `docker build` / `docker compose build` behaves exactly as it did before this
# split.
#
# MEASURED, on buildx 0.30, four arms plus a negative control:
#
#   context WITHOUT datanika-cloud/, EDITION=core   -> builds, /cloud absent
#   context WITHOUT datanika-cloud/, EDITION=cloud  -> FAILS at the COPY  <- control
#   context WITH    datanika-cloud/, EDITION=cloud  -> builds, /cloud present
#   context WITH    datanika-cloud/, EDITION=core   -> builds, /cloud ABSENT
#   EDITION=<typo>                                  -> FAILS resolving the stage
#
# The fourth arm is the discriminating one: BuildKit does not build a stage the
# selected target does not descend from, so the cloud tree cannot reach a core
# image even from a context that contains it. The second arm is what proves the
# first is not passing because the COPY was skipped for some unrelated reason.
ARG DATANIKA_IMAGE_EDITION=cloud

# =============================================================================
# base — everything both editions share, including all the expensive work.
# =============================================================================
# 🚨 TWO LEVERS REFRESH THE OS PACKAGES, AND NEITHER IS ENOUGH ALONE (core#1313).
#
# 1. THE BASE IS PINNED BY DIGEST — reproducibility, and a base bump becomes a reviewable diff.
#    This was `FROM python:3.12-slim`, a floating tag Docker never re-fetched: the box built against
#    a copy cached 2026-07-14 while upstream had moved on, and every layer above it stayed valid.
#    The image serving production reported `Created` as the previous night and carried an apt layer
#    dated seven weeks earlier. A REBUILD IS NOT A REFRESH.
#
# 2. `APT_REFRESHED_ON` (just below the FROM) RE-RUNS THE PACKAGE LAYER against today's Debian
#    mirrors, upgrade included — the lever for fixes Debian publishes between upstream base builds.
#
# 🔴 CORRECTED 2026-09-15. This comment used to say the pin refreshed the packages because
# "changing this line's TEXT invalidates this instruction and everything after it". MEASURED
# FALSE. A layer's cache key follows the RESOLVED parent's content, not the FROM line's text, and
# the pin named the exact digest the floating tag already resolved to. Run 34695877776, the first
# build carrying the pin:
#
#     [base 1/9] FROM python:3.12-slim@sha256:78387bc3...     <- the same parent as before
#     [base 2/9] RUN apt-get update && apt-get install ...    -> CACHED
#
# `pull: true` (run 34692477424) failed the same way, for the same reason.
#
# ⚠️ Nor would a fresh `apt-get install <list>` have been enough without the cache: install never
# upgrades a package the base image already ships. Measured on this digest, 2026-09-15: a fresh
# install fixed only the packages the list pulls in and left the base's own packages upgradable;
# `apt-get upgrade` first left none. Upstream had published no newer base, so no digest bump
# could have helped either.
#
# 🔑 HOW TO REFRESH. When `image-cve` reports a fixable OS-package finding:
#   - bump APT_REFRESHED_ON to today. A new build-arg value invalidates the RUN after it —
#     measured, with a positive control in the same run (same value -> CACHED, new value ->
#     EXECUTED) — so the package step re-runs in CI and on the box alike;
#   - bump the digest as well when upstream has published a newer base:
#       docker buildx imagetools inspect python:3.12-slim    # the index digest is on `Digest:`
#   The step prints `apt layer refresh <date> ran at <UTC time>` ONLY when it actually executes
#   (the step header shows the format string, never a time), followed by the count still
#   upgradable. A cached step prints neither. Read those lines, not this file, as the evidence.
#
# ⚠️ Do NOT "simplify" either lever away because a bump felt like friction. The friction is the
# mechanism.
#
# Pinned 2026-09-12 to the 2026-09-01 upstream build — still upstream's newest on 2026-09-15.
FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea AS base

# The package layer's refresh date (see HOW TO REFRESH above). Declared directly before the RUN
# it invalidates and printed by it, so a reorder cannot quietly detach the two
# (tests/test_deploy/test_base_image_freshness.py).
ARG APT_REFRESHED_ON=2026-09-21

# System deps for psycopg2, bcrypt, cryptography, and xmlsec/lxml (SAML).
# libxml2-dev/libxslt1-dev/libxmlsec1-dev/pkg-config + zlib1g-dev/libssl-dev let
# us build lxml + xmlsec FROM SOURCE (below) against the SAME system libxml2 —
# the prebuilt wheels each bundle a different libxml2 and mismatch at import on
# debian-slim ("lxml & xmlsec libxml2 library version mismatch").
#
# unixodbc + tdsodbc (core#1379): the ODBC runtime and Debian's FreeTDS driver, which the SQL Server
# and Synapse DESTINATIONS load through (pyodbc). Founder decision: FreeTDS, not Microsoft's driver,
# so no third-party apt repository and no driver EULA in this image. `tdsodbc`'s own postinst
# registers `[FreeTDS]` in /etc/odbcinst.ini; the `final` stage asserts that registration, because a
# listed package is not a working driver (measured on #1379).
#
# `apt-get upgrade` runs BEFORE install, because install never upgrades what the base already
# ships; its stdout is discarded (errors still reach stderr) because this repository's build logs
# are public. The step then asserts nothing is left upgradable: a non-zero count means an upgrade
# was kept back (it needs a new dependency), which is a decision for a person, not something to
# ship silently. Only the COUNT is printed, for the same reason.
RUN printf 'apt layer refresh %s ran at %s\n' "${APT_REFRESHED_ON}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
      -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold > /dev/null && \
    apt-get install -y --no-install-recommends \
    curl unzip gcc libpq-dev \
    libxml2-dev libxslt1-dev libxmlsec1-dev libxmlsec1-openssl pkg-config \
    zlib1g-dev libssl-dev \
    unixodbc tdsodbc && \
    apt-get -s upgrade > /tmp/apt-upgradable && \
    left="$(grep -c '^Inst' /tmp/apt-upgradable || true)" && \
    printf 'apt layer refresh: %s package(s) still upgradable\n' "${left}" && \
    if [ "${left}" != "0" ]; then \
      echo "apt layer refresh: upgrades were kept back; decide between full-upgrade and a pin" >&2; \
      exit 1; \
    fi && \
    rm -rf /var/lib/apt/lists/* /tmp/apt-upgradable

# ---------------------------------------------------------------------------
# Pinned bun, installed BEFORE `reflex init` so the build does not depend on a
# third-party CDN at the moment it runs (core#1498).
#
# `uv run reflex init` calls reflex's install_bun(), which fetches
#     https://raw.githubusercontent.com/reflex-dev/reflex/main/scripts/bun_install.sh
# and runs it, and that script then downloads the bun binary. A 504 from either
# host fails the build - and THIS Dockerfile is what the production deploy builds
# (`docker compose build app celery app_b`), so an upstream outage removes the
# only route back: core#1014 records that the box cannot pull a pinned image.
# Measured 2026-09-21: a documentation-only PR was ejected from the merge queue
# by exactly this, `curl: (22) ... 504`.
#
# Read the URL again: branch `main`, unpinned. The pre-fix build also executed
# whatever sat on a third party's default branch at build time. Removing that is
# a supply-chain reduction, not only an availability one - which is why this is a
# pre-install rather than a retry around the same fetch.
#
# Why this WORKS rather than merely narrowing the window: install_bun() returns
# early when a bun >= Bun.MIN_VERSION already exists at Bun.DEFAULT_PATH
# (reflex/utils/js_runtimes.py). Measured on the built image, control included -
# the second line is what makes the first one mean anything:
#     bun present, --network none  ->  install_bun() returns OK
#     bun removed, --network none  ->  "Failed to download bun install script"
#     bun removed, with network    ->  re-downloads 1.3.5
#
# Fails SOFT by design. If reflex raises MIN_VERSION above BUN_VERSION,
# install_bun() just downloads as it does today - degraded to the status quo, not
# broken. tests/test_deploy/test_bun_pin.py stops that fallback from becoming
# silently permanent, and the stamp check after `reflex init` notices it here.
#
# `bun --version` below is not decoration: bun ships a separate `baseline` build
# for CPUs without AVX2, so RUNNING the binary is what turns a build-host /
# run-host mismatch into a loud build failure instead of a SIGILL later.
ARG BUN_VERSION=1.3.5
ARG BUN_SHA256_AMD64=7051d86a924aefea3e0b96213b5fd8f79c0793f9cae6534233e627e5c3db4669
RUN set -eu; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) asset="bun-linux-x64.zip"; sum="${BUN_SHA256_AMD64}" ;; \
      *) echo "FATAL: no pinned bun checksum for architecture '${arch}' (core#1498)." >&2; \
         echo "       Add the asset and its sha256 above; do not let the pin fall" >&2; \
         echo "       back to the unpinned installer, which is what this removes." >&2; \
         exit 1 ;; \
    esac; \
    url="https://github.com/oven-sh/bun/releases/download/bun-v${BUN_VERSION}/${asset}"; \
    ok=""; \
    for attempt in 1 2 3 4 5; do \
      if curl -fsSL --connect-timeout 10 --max-time 300 -o /tmp/bun.zip "$url"; then ok=1; break; fi; \
      echo "bun download attempt ${attempt}/5 failed; retrying" >&2; \
      sleep "$((attempt * 5))"; \
    done; \
    [ -n "$ok" ] || { echo "FATAL: could not download pinned bun ${BUN_VERSION} after 5 attempts" >&2; exit 1; }; \
    echo "${sum}  /tmp/bun.zip" | sha256sum -c -; \
    mkdir -p /root/.local/share/reflex/bun/bin; \
    unzip -j -o -q /tmp/bun.zip "*/bun" -d /root/.local/share/reflex/bun/bin; \
    chmod 0755 /root/.local/share/reflex/bun/bin/bun; \
    rm -f /tmp/bun.zip; \
    got="$(/root/.local/share/reflex/bun/bin/bun --version)"; \
    [ "$got" = "${BUN_VERSION}" ] || { echo "FATAL: pinned bun reports '${got}', expected '${BUN_VERSION}'" >&2; exit 1; }; \
    { sha256sum /root/.local/share/reflex/bun/bin/bun; \
      stat -c '%Y %s' /root/.local/share/reflex/bun/bin/bun; } > /tmp/bun-pin.stamp; \
    echo "bun ${BUN_VERSION} pinned at Bun.DEFAULT_PATH - reflex init will skip its download"

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files + README (hatchling needs it) for layer caching
COPY datanika/pyproject.toml datanika/uv.lock datanika/README.md ./

# Stub the package so uv sync can resolve it without full source
RUN mkdir -p datanika && touch datanika/__init__.py

# Install dependencies (no dev deps in production image). Build lxml + xmlsec
# from source (against the system libxml2 above) so they share ONE libxml2 — the
# prebuilt wheels mismatch at import on debian-slim. A satisfied, source-built
# package survives reflex-init's idempotent re-sync below.
RUN uv sync --frozen --no-dev --no-binary-package lxml --no-binary-package xmlsec

# Freeze the resolved tree into a constraints file, and apply it to every later
# install (core#602).
#
# `uv sync --frozen` installs exactly what uv.lock resolved. Every
# `uv pip install` below re-resolves against PyPI and never reads the lock, so
# without a constraint it is free to move ANY already-installed package -- not
# just the new one's own dependencies. `mcp` alone pulls anyio, httpx, pydantic,
# starlette and uvicorn, all of which core locks.
#
# That is the actual #602 mechanism, and it is broader than "a pin got
# clobbered": core declares `mcp` only in its **dev extra**, and this image
# builds `--no-dev`, so core installs no `mcp` at all and nothing was clobbered.
# `datanika-mcp`'s then-unbounded `mcp>=1.0.0` was the sole constraint deciding
# what shipped, and uv.lock's `mcp==1.27.0` never bound it. Ceilings in the
# sub-package close that one dependency; this closes the class.
#
# --all-extras so the dev extra's pins (mcp included) are in the constraints:
# a constraint only binds a package something else asks to install, so listing
# packages this image never installs is free.
RUN uv export --frozen --all-extras --no-hashes --no-emit-project --no-annotate     -o /tmp/lock-constraints.txt &&     echo "constraints: $(wc -l < /tmp/lock-constraints.txt) lines"

# Copy full application code
COPY datanika/ .

# Note: dlt verified sources (Stripe, GitHub, HubSpot, etc.) use REST API
# fallback when not installed via `dlt init`. This avoids dependency conflicts
# in Docker. To enable native verified sources, run `dlt init <source> <dest>`
# inside the container after build.

# =============================================================================
# variant-* — the ONLY difference between the two editions. Keep these stages
# to the cloud graft and nothing else: anything added here has to be added
# twice, which is the duplication this split exists to avoid.
# =============================================================================

# Core-only: deliberately empty. `/cloud` never exists, so `datanika_cloud` is
# not importable and `DATANIKA_EDITION=cloud` cannot be honoured at runtime —
# which is correct, because the plugin an OSS user cannot obtain is also the one
# they cannot be billed by. All three of core's references to it are already
# edition-gated or ImportError-suppressed (datanika.py, tasks/celery_app.py,
# migrations/env.py), so no application change is needed for this to work.
FROM base AS variant-core

# Cloud: graft the private plugin source in.
FROM base AS variant-cloud
COPY datanika-cloud/ /cloud/

# =============================================================================
# final — every assertion lives here, so both editions run all of them.
# =============================================================================
FROM variant-${DATANIKA_IMAGE_EDITION} AS final

# ⚠️ An ARG declared before the first FROM is visible to FROM lines and to
# NOTHING ELSE. Re-declaring it here is what makes it readable by the RUN steps
# below; without this line `$DATANIKA_IMAGE_EDITION` expands to the empty string
# and the edition assertion falls through to its error branch.
ARG DATANIKA_IMAGE_EDITION

# core#1014 - the COPYs above are unfiltered unless an ignore file applies, and
# the repo's own `.dockerignore` applies to NEITHER build path (see the header of
# `Dockerfile.dockerignore`, which is the file that does). Assert the outcome here
# rather than trusting that file to be read: a published image carrying /cloud/.git
# publishes the PRIVATE datanika-cloud repository's entire history the moment the
# GHCR package's visibility changes. The deploy tarball uses --exclude-vcs, so this
# cannot fire on the box; it fires on a GHA build that lost its ignore file.
#
# Runs in BOTH editions. In the core edition the /cloud/.git half is vacuous by
# construction — that is the point of also asserting the edition invariant below,
# which is the check that can actually tell the two images apart.
RUN set -e; \
    for d in /app/.git /cloud/.git; do \
      if [ -e "$d" ]; then \
        echo "FATAL: $d is in the image - the build context carries VCS history (core#1014)"; \
        exit 1; \
      fi; \
    done; \
    echo "build context VCS check: /app/.git and /cloud/.git both absent"

# Assert the image is the edition it was asked for (core#1014).
#
# Without this, the two failure directions are both silent. A core build that
# somehow acquired /cloud ships the closed plugin in the artifact we intend to
# publish openly; a cloud build that lost it starts, serves, and enforces no
# quota at all — the shape of core#772, where hooks were subscribed nowhere and
# every container read healthy. `test -e` on the tree, not an env var: the whole
# point is what is IN the image.
RUN set -e; \
    case "$DATANIKA_IMAGE_EDITION" in \
      cloud) \
        [ -f /cloud/pyproject.toml ] || { echo "FATAL: edition=cloud but /cloud/pyproject.toml is missing"; exit 1; }; \
        echo "edition check: cloud - /cloud present" ;; \
      core) \
        [ ! -e /cloud ] || { echo "FATAL: edition=core but /cloud exists - the core-only image must carry no closed-source tree"; exit 1; }; \
        echo "edition check: core - /cloud absent" ;; \
      *) \
        echo "FATAL: DATANIKA_IMAGE_EDITION must be 'core' or 'cloud', got '$DATANIKA_IMAGE_EDITION'"; \
        exit 1 ;; \
    esac

# Reflex needs to initialize on first run (recreates .venv)
RUN uv run reflex init

# Assert `reflex init` did NOT replace the pinned bun, and that we pinned it
# where reflex actually looks (core#1498). TWO checks, because each is blind to
# what the other catches.
#
# 1. THE PATH. The stamp below cannot catch a path change: if reflex started
#    looking somewhere else, our file would sit untouched at the old path, the
#    stamp would pass, and `reflex init` would have downloaded to the new one -
#    green, and back on the CDN. So ask reflex where it looks, on the real
#    installed reflex, and compare against the literal used above.
#
# 2. THE STAMP. A content hash ALONE cannot do this job either: a re-download of
#    the same 1.3.5 produces identical bytes, so sha256 would pass while the
#    build had quietly gone back to depending on the network. The stamp therefore
#    carries mtime and size as well, which a rewrite necessarily changes.
#    Verified before shipping it: with bun pre-present, `reflex init` leaves
#    mtime, size, inode AND sha256 all identical, so it does not false-alarm on
#    the healthy path.
#
# Without these, a MIN_VERSION bump or a moved default path would silently
# restore the outage risk while every comment above still claimed it was gone - a
# fix whose own documentation describes a world it no longer produces.
RUN set -eu; \
    want="$(/app/.venv/bin/python -c 'from reflex.constants import Bun; print(Bun.DEFAULT_PATH)')"; \
    [ "$want" = "/root/.local/share/reflex/bun/bin/bun" ] || { \
      echo "FATAL: reflex looks for bun at '${want}', but this image pinned it at" >&2; \
      echo "       /root/.local/share/reflex/bun/bin/bun (core#1498). The pre-install" >&2; \
      echo "       is a no-op and the build is back on the CDN. Update both together." >&2; \
      exit 1; }; \
    cur="$(sha256sum /root/.local/share/reflex/bun/bin/bun; \
           stat -c '%Y %s' /root/.local/share/reflex/bun/bin/bun)"; \
    [ "$cur" = "$(cat /tmp/bun-pin.stamp)" ] || { \
      echo "FATAL: the pinned bun was replaced during 'reflex init' (core#1498)." >&2; \
      echo "       The build depends on the network again. Compare reflex's" >&2; \
      echo "       Bun.MIN_VERSION against BUN_VERSION above." >&2; \
      exit 1; }; \
    echo "bun pin intact after reflex init, at reflex's own DEFAULT_PATH"

# Install cloud plugin AFTER reflex init (which recreates the venv).
# Skipped entirely in the core edition — there is nothing at /cloud to install,
# and the edition assertion above has already proven that.
RUN set -e; \
    if [ "$DATANIKA_IMAGE_EDITION" = "cloud" ]; then \
      uv pip install --constraint /tmp/lock-constraints.txt /cloud; \
    else \
      echo "edition=core: cloud plugin deliberately not installed"; \
    fi

# Install the datanika-mcp tool-surface package so the app can mount the remote
# MCP endpoint (/mcp). Copied in via `COPY datanika/ .` above → /app/datanika-mcp.
# Optional at runtime (the mount is guarded), but present in prod. Remote-MCP P1.
RUN uv pip install --constraint /tmp/lock-constraints.txt ./datanika-mcp

# Drop uv's DOWNLOAD CACHE from the image (core#835).
#
# This build is single-stage, so `/root/.cache/uv` ships. It holds unpacked
# `archive-v0/` trees and `sdists-v9/` sources, each carrying a real
# `*.dist-info/METADATA` or `*.egg-info/PKG-INFO` -- and trivy's python
# analyzer reads those as INSTALLED PACKAGES.
#
# Measured on the `image-cve` run for `dev 89e7e2b`: **51 of 306** scanned
# targets were cache paths, and **6 of the 11 HIGH findings** came from
# packages the application cannot import. lxml is the clearest instance -- the
# venv ships 6.1.2 (floored in pyproject.toml for CVE-2026-41066) while the
# cache still holds the 6.0.2 sdist the build resolved through, so the scanner
# reported a CVE we had already fixed. `jaraco.context` and `wheel` are worse:
# setuptools' VENDORED copies, inside a cache entry, never importable at all.
#
# That is not merely noise. A scanner reporting six findings nobody can act on
# is how a red check stops being read, and this job has been red on every push
# for weeks.
#
# ⚠️ AFTER every uv command, deliberately. `uv run reflex init` re-syncs from
# the lock, so a clean placed earlier is undone and the build goes cold for
# nothing. `test_image_cve_signal.py` asserts the ordering, not just the line.
#
# ⚠️ `uv cache clean`, not `rm -rf`: uv honours UV_CACHE_DIR, and a hardcoded
# path silently stops cleaning anything the day that is set -- a command that
# exits 0 having cleaned the wrong directory.
#
# The venv is unaffected: `uv sync` hardlinks into it, so removing the cache's
# link leaves the data alive under the venv's. That is a claim, and the /mcp
# import assertion immediately below is its control -- which is why the clean
# goes ABOVE it. If cleaning ever did gut the venv, the BUILD fails, before
# anything reaches a registry.
RUN uv cache clean && \
    /app/.venv/bin/python -c "import pathlib, sys; p = pathlib.Path('/root/.cache/uv'); n = sum(1 for _ in p.rglob('*')) if p.exists() else 0; sys.exit(f'uv cache still in the image: {n} paths under {p}') if n else print('uv cache absent from the image')"

# Assert the artifact works, in the artifact (core#602).
#
# `datanika.py` mounts /mcp inside `except ImportError:` that logs a warning and
# continues -- deliberate, because the package is optional in dev/CI, but it
# means a broken tool surface does not fail app startup. It just serves no /mcp,
# and the first thing that notices is the blue/green post-swap probe in
# production, which is the most expensive possible place to learn it.
#
# Import what the mount actually imports. `datanika_mcp/__init__.py` is a
# docstring and a version string, so `import datanika_mcp` would have passed
# happily against this exact break; `datanika_mcp.server` is the module that
# does `from mcp.server.fastmcp import FastMCP`.
RUN /app/.venv/bin/python -c "import importlib.metadata as m; import datanika_mcp.server, datanika_mcp.client, datanika_mcp.session; print('/mcp surface imports OK -- mcp', m.version('mcp'), 'datanika-mcp', m.version('datanika-mcp'))"

# Assert the OTHER THREE entrypoints, for the same reason (core#1201).
#
# This is one image with four commands run against it, and until now exactly one of
# them was asserted here. `datanika_mcp` is a sub-import of the app, so the line above
# proves a slice of the `app` path and nothing about the rest:
#
#   app / app_b   reflex run                                        <- partly covered above
#   celery        celery -A datanika.tasks.celery_app:celery_app worker -E
#   beat          celery -A datanika.tasks.celery_app:celery_app beat
#   scheduler     python -m datanika.scheduler_main                 (core#648)
#
# The two least-covered are exactly the two that serve no HTTP, so unlike `app` they
# have no pre-repoint assertion in front of them: `deploy-bluegreen.sh` checks /healthz
# and /mcp against the target's own backend port before repointing Apache, and a worker
# that cannot import is discovered by `container-restart-loop` -- i.e. AFTER production
# has no worker.
#
# 🚨 ONE PROCESS PER ENTRYPOINT, deliberately, and not one interpreter importing all
# three. Ask the process, and import the entrypoint IT imports (WORKFLOW_RULES §13).
# Importing them together would also invent an order that does not exist in production:
# core#832 records that `celery_app.py` imports `bootstrap_cloud` at module level while
# `plugin.py` imports back into `datanika.ui.state.auth_state`, which reaches
# `celery_app` again. The real entrypoints happen to import in a safe order; a combined
# probe is a new one, and a build failing on a cycle production never takes is worse
# than no assertion -- it fails on correct code (docs/QA_RULES.md §29).
#
# Runs in `final`, i.e. AFTER the variant graft, so it asserts the variant being built.
# On `variant-core` there is no `datanika_cloud`, and `celery_app`'s cloud import sits
# behind `if settings.datanika_edition == "cloud"`, so a plain import is correct there
# too -- but CI builds both (`image-probe` cloud, `core-only-image` core) and that is
# what verifies it rather than this comment.
RUN set -eu; \
    for mod in datanika.datanika datanika.tasks.celery_app datanika.scheduler_main; do \
      /app/.venv/bin/python -c "import $mod" \
        || { echo "entrypoint module '$mod' does not import in this image"; exit 1; }; \
      echo "entrypoint imports OK: $mod"; \
    done

# FreeTDS for the SQL Server and Synapse destinations (core#1379) -- asserted, not assumed.
#
# 🚨 A LISTED PACKAGE IS NOT A WORKING DRIVER. Measured on #1379: installing `unixodbc` alone clears
# `ImportError: libodbc.so.2`, builds green, and leaves `pyodbc.drivers() == []`, so every load moves
# to "No supported ODBC driver found". So the build asks pyodbc -- in the artifact -- for the exact
# name the loader and the dbt profile use (`datanika.services.dlt_mssql_freetds.FREETDS_DRIVER`).
#
# ⚠️ ENCRYPTION IS NOT CONFIGURED HERE, and that is deliberate. FreeTDS ignores Microsoft's
# `Encrypt=yes` without an error, and a `[global] encryption = require` in freetds.conf was MEASURED
# INERT for these DSN-less connections (`SERVER=host,port`): the session read `encrypt_option = FALSE`
# from the server's DMV. So each consumer carries FreeTDS's own keyword in its connection string --
# see `datanika/services/dlt_mssql_freetds.py`. The server certificate is not verified (no CA), which
# the destination documentation states (founder decision on #1379).
RUN /app/.venv/bin/python -c "import pyodbc, sys; d = pyodbc.drivers(); sys.exit(f'FreeTDS ODBC driver not registered: {d}') if 'FreeTDS' not in d else print('odbc drivers:', d)"

EXPOSE 3000 8000

CMD ["uv", "run", "reflex", "run", "--env", "prod"]

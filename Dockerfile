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
# 🚨 PINNED BY DIGEST, AND THE PIN IS WHAT REFRESHES THE OS PACKAGES (core#1313).
#
# This was `FROM python:3.12-slim` — a floating tag. Docker does not re-fetch a tag it already
# holds, so the box built against a copy cached 2026-07-14 while upstream had moved on, and every
# layer above it stayed valid. The image serving production reported `Created` as the previous
# night and carried an apt layer dated seven weeks earlier. A REBUILD IS NOT A REFRESH.
#
# ⚠️ `pull: true` ALONE DOES NOT FIX THIS — measured, not assumed. Run 34692477424 pulled this
# exact digest and the apt layer STILL came from cache:
#
#     #6  [base 1/9] FROM python:3.12-slim@sha256:78387bc3...     <- base current
#     #11 CACHED     RUN apt-get update && apt-get install ...    <- apt NOT re-run
#     Get:1 = 0 | Reading package lists = 0 | Unpacking <any pkg> = 0
#
# `cache-from: type=gha` restored the layer regardless of the refreshed base. The pin works where
# the pull did not because changing this line's TEXT invalidates this instruction and everything
# after it — ordinary, deterministic layer-cache semantics rather than a property of the remote
# cache's key derivation.
#
# 🔑 HOW TO BUMP, and why the process is the point. `image-cve` going red IS the signal: it means
# the pinned base has accumulated fixable CVEs. Then:
#
#     docker pull python:3.12-slim
#     docker image inspect python:3.12-slim --format '{{index .RepoDigests 0}}'
#     # paste the digest below, commit, let image-cve confirm green
#
# That makes the base a REVIEWABLE INPUT — the version is in git, the bump is a diff, and the
# gate that tells you to bump is the same gate that confirms the bump worked. A floating tag gave
# neither reproducibility nor freshness; this gives both, at the cost of a deliberate bump.
#
# ⚠️ Do NOT "simplify" this back to a bare tag because a bump felt like friction. The friction is
# the mechanism.
#
# Pinned 2026-09-12 to the 2026-09-01 upstream build.
FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea AS base

# System deps for psycopg2, bcrypt, cryptography, and xmlsec/lxml (SAML).
# libxml2-dev/libxslt1-dev/libxmlsec1-dev/pkg-config + zlib1g-dev/libssl-dev let
# us build lxml + xmlsec FROM SOURCE (below) against the SAME system libxml2 —
# the prebuilt wheels each bundle a different libxml2 and mismatch at import on
# debian-slim ("lxml & xmlsec libxml2 library version mismatch").
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip gcc libpq-dev \
    libxml2-dev libxslt1-dev libxmlsec1-dev libxmlsec1-openssl pkg-config \
    zlib1g-dev libssl-dev && \
    rm -rf /var/lib/apt/lists/*

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

EXPOSE 3000 8000

CMD ["uv", "run", "reflex", "run", "--env", "prod"]

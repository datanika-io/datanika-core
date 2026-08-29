FROM python:3.12-slim

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

# Copy cloud edition plugin source
COPY datanika-cloud/ /cloud/

# Reflex needs to initialize on first run (recreates .venv)
RUN uv run reflex init

# Install cloud plugin AFTER reflex init (which recreates the venv)
RUN uv pip install --constraint /tmp/lock-constraints.txt /cloud

# Install the datanika-mcp tool-surface package so the app can mount the remote
# MCP endpoint (/mcp). Copied in via `COPY datanika/ .` above → /app/datanika-mcp.
# Optional at runtime (the mount is guarded), but present in prod. Remote-MCP P1.
RUN uv pip install --constraint /tmp/lock-constraints.txt ./datanika-mcp

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

EXPOSE 3000 8000

CMD ["uv", "run", "reflex", "run", "--env", "prod"]

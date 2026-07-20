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
RUN uv pip install /cloud

# Install the datanika-mcp tool-surface package so the app can mount the remote
# MCP endpoint (/mcp). Copied in via `COPY datanika/ .` above → /app/datanika-mcp.
# Optional at runtime (the mount is guarded), but present in prod. Remote-MCP P1.
RUN uv pip install ./datanika-mcp

EXPOSE 3000 8000

CMD ["uv", "run", "reflex", "run", "--env", "prod"]

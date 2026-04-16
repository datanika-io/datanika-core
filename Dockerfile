FROM python:3.12-slim

# System deps for psycopg2, bcrypt, cryptography
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files + README (hatchling needs it) for layer caching
COPY datanika/pyproject.toml datanika/uv.lock datanika/README.md ./

# Stub the package so uv sync can resolve it without full source
RUN mkdir -p datanika && touch datanika/__init__.py

# Install dependencies (no dev deps in production image)
RUN uv sync --frozen --no-dev

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

# Patch Vite config to proxy /api/* to backend (fixes #200 — SSO/OAuth
# routes return SPA shell instead of proper HTTP status on :3000)
RUN uv run python scripts/patch-vite-proxy.py

# Install cloud plugin AFTER reflex init (which recreates the venv)
RUN uv pip install /cloud

EXPOSE 3000 8000

CMD ["uv", "run", "reflex", "run", "--env", "prod"]

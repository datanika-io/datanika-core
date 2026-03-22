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

# Initialize dlt verified sources (SaaS connectors)
RUN echo "Y" | uv run dlt init stripe_analytics duckdb && \
    echo "Y" | uv run dlt init github duckdb && \
    echo "Y" | uv run dlt init hubspot duckdb && \
    echo "Y" | uv run dlt init shopify_dlt duckdb && \
    echo "Y" | uv run dlt init jira duckdb && \
    echo "Y" | uv run dlt init slack duckdb && \
    echo "Y" | uv run dlt init salesforce duckdb

# Copy cloud edition plugin source
COPY datanika-cloud/ /cloud/

# Reflex needs to initialize on first run (recreates .venv)
RUN uv run reflex init

# Install cloud plugin AFTER reflex init (which recreates the venv)
RUN uv pip install /cloud

EXPOSE 3000 8000

CMD ["uv", "run", "reflex", "run", "--env", "prod"]

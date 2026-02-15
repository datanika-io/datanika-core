FROM python:3.12-slim

# System deps for psycopg2, bcrypt, cryptography
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files + README (hatchling needs it) for layer caching
COPY pyproject.toml uv.lock README.md ./

# Stub the package so uv sync can resolve it without full source
RUN mkdir -p etlfabric && touch etlfabric/__init__.py

# Install dependencies (no dev deps in production image)
RUN uv sync --frozen --no-dev

# Copy full application code
COPY . .

# Reflex needs to initialize on first run
RUN uv run reflex init

EXPOSE 3000 8000

CMD ["uv", "run", "reflex", "run", "--env", "prod"]

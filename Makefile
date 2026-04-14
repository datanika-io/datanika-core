.PHONY: e2e-seed e2e-seed-ci

# Deterministic E2E fixture (owner user + org + DuckDB connection).
# Emits a JSON payload on stdout the Playwright harness can load.
# Refuses to run against hosts that look like production — override
# for ephemeral CI stacks with E2E_SEED_ALLOW_ANY_HOST=1.
e2e-seed:
	uv run python -m datanika.scripts.e2e_seed

# CI variant: bypasses the prod-host guard because ephemeral docker-compose
# runners use database_url hostnames that would otherwise trip it.
e2e-seed-ci:
	E2E_SEED_ALLOW_ANY_HOST=1 uv run python -m datanika.scripts.e2e_seed

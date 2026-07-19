# Datanika MCP Server

MCP server for [Datanika](https://datanika.io) — browse connections, preview data, compile and validate dbt transformations, monitor runs, and manage pipelines from Claude Desktop.

**Read-only by default.** Pass `--allow-write` to enable creating resources and triggering pipeline runs.

## Install

```bash
# From PyPI (when published)
uvx datanika-mcp --help

# From git
uvx --from "git+https://github.com/datanika-io/datanika-core#subdirectory=datanika-mcp" datanika-mcp --help
```

## Claude Desktop Configuration

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS, `%APPDATA%\Claude\claude_desktop_config.json` on Windows):

### Read-only (recommended)

```json
{
  "mcpServers": {
    "datanika": {
      "command": "uvx",
      "args": [
        "--from", "git+https://github.com/datanika-io/datanika-core#subdirectory=datanika-mcp",
        "datanika-mcp",
        "--url", "https://app.datanika.io",
        "--api-key", "YOUR_API_KEY"
      ]
    }
  }
}
```

### With write access

```json
{
  "mcpServers": {
    "datanika": {
      "command": "uvx",
      "args": [
        "--from", "git+https://github.com/datanika-io/datanika-core#subdirectory=datanika-mcp",
        "datanika-mcp",
        "--url", "https://app.datanika.io",
        "--api-key", "YOUR_API_KEY",
        "--allow-write"
      ]
    }
  }
}
```

### Environment variables

You can also configure via environment variables:

```json
{
  "mcpServers": {
    "datanika": {
      "command": "uvx",
      "args": [
        "--from", "git+https://github.com/datanika-io/datanika-core#subdirectory=datanika-mcp",
        "datanika-mcp"
      ],
      "env": {
        "DATANIKA_URL": "https://app.datanika.io",
        "DATANIKA_API_KEY": "YOUR_API_KEY",
        "DATANIKA_ALLOW_WRITE": "true"
      }
    }
  }
}
```

## Available Tools

### Always available (read-only)

| Tool | Description |
|------|-------------|
| `get_agent_tiers` | Get the 5-tier agent capability stack |
| `get_connection_types` | List supported connection types with config schemas |
| `list_connections` | List all connections in the org |
| `get_connection` | Get connection details by ID |
| `introspect_connection` | List schemas/tables of a source connection |
| `preview_connection` | Preview first N rows of a table |
| `query_connection` | Execute a read-only SQL query |
| `compile_transformation` | Compile a dbt transformation (no execution) |
| `preview_transformation` | Compile + execute, return preview rows |
| `list_uploads` | List all uploads |
| `list_pipelines` | List all pipelines |
| `list_transformations` | List all transformations |
| `list_runs` | List runs with optional filters |
| `get_run` | Get run details by ID |
| `get_run_logs` | Get run logs |
| `list_catalog` | List catalog entries (source tables + dbt models) |
| `get_catalog_entry` | Get catalog entry details |

### Requires `--allow-write`

| Tool | Description |
|------|-------------|
| `create_connection` | Create a new data connection |
| `create_upload` | Create a new upload (extract + load) |
| `create_pipeline` | Create a new pipeline (dbt orchestration) |
| `create_transformation` | Create a new dbt SQL transformation |
| `bulk_import` | Bulk-create resources from JSON v2 format |
| `trigger_upload` | Trigger an upload run |
| `trigger_pipeline` | Trigger a pipeline run |
| `trigger_transformation` | Trigger a transformation run |

## Self-hosted

Point `--url` at your instance:

```bash
datanika-mcp --url http://localhost:3000 --api-key etf_your_key
```

## Releasing (maintainers)

`datanika-mcp` publishes to PyPI via GitHub Actions **Trusted Publishing** (OIDC — no stored API token). To cut a release (from `master`):

1. Bump `version` in [`pyproject.toml`](pyproject.toml).
2. Tag and push — the tag version must match `pyproject.toml`:

   ```bash
   git tag mcp-v0.1.0
   git push origin mcp-v0.1.0
   ```

3. The [`Release datanika-mcp to PyPI`](../.github/workflows/release-mcp.yml) workflow builds the sdist + wheel and publishes. Verify: `uvx datanika-mcp --help` resolves from PyPI.

> First-release setup: a one-time PyPI trusted-publisher must be configured (project `datanika-mcp`, repo `datanika-io/datanika-core`, workflow `release-mcp.yml`, environment `pypi`) before the first tag will publish. See the infra human-locker.

## License

AGPL-3.0 — same as the core Datanika platform.

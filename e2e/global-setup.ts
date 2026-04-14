import { execSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import type { FullConfig } from "@playwright/test";

/**
 * Playwright globalSetup — shells `uv run python -m datanika.scripts.e2e_seed`
 * against the target stack (usually the ephemeral docker-compose in CI or
 * the locally-running stack in dev), parses the JSON payload on stdout, and
 * writes each field to `process.env.DATANIKA_E2E_*` so fixtures can read
 * deterministic credentials without knowing anything about the seed script
 * internals.
 *
 * We invoke the Python module directly rather than `make e2e-seed-ci`
 * because `make` is not available on Git Bash / MSYS2 on Windows, and local
 * Windows devs running the harness interactively would hit
 * `make: command not found`. The env var `E2E_SEED_ALLOW_ANY_HOST=1` is the
 * only thing the make target provides on top of the module call — we set
 * it directly here.
 *
 * Payload contract (pinned in tests/test_scripts/test_e2e_seed.py):
 *   org_id, org_slug, user_id, user_email, user_password,
 *   connection_id, connection_name, connection_type, seeded_at
 *
 * See datanika-cloud/docs/billing_contract.md for the quota implications of
 * the fixture user and datanika/scripts/e2e_seed.py for the source of truth.
 *
 * Skip seed execution by setting DATANIKA_E2E_SKIP_SEED=1 (useful when
 * iterating locally against an already-seeded stack).
 *
 * Override the seed command by setting DATANIKA_E2E_SEED_CMD. Whatever the
 * value is, it's shelled verbatim in the repo root and its stdout is parsed
 * as the same JSON payload. This is how the harness reaches a remote target
 * (e.g., staging) without the seed script needing network access to the
 * remote DB. Example for staging:
 *
 *   DATANIKA_E2E_BASE_URL=https://staging-app.datanika.io/ \
 *   DATANIKA_E2E_SEED_CMD="ssh root@46.225.214.120 'docker exec datanika-staging-app uv run python -m datanika.scripts.e2e_seed'" \
 *   npm test
 *
 * The local e2e_seed.py still runs inside the staging container and writes
 * to the staging DB directly — we just execute it over SSH.
 */

export type SeedFixture = {
  org_id: number;
  org_slug: string;
  user_id: number;
  user_email: string;
  user_password: string;
  connection_id: number;
  connection_name: string;
  connection_type: string;
  seeded_at: string;
};

async function globalSetup(_config: FullConfig): Promise<void> {
  if (process.env.DATANIKA_E2E_SKIP_SEED === "1") {
    if (!process.env.DATANIKA_E2E_USER_EMAIL) {
      throw new Error(
        "DATANIKA_E2E_SKIP_SEED=1 requires DATANIKA_E2E_USER_EMAIL etc. to be " +
          "pre-populated in the environment. Either unset DATANIKA_E2E_SKIP_SEED " +
          "or export the fixture fields manually.",
      );
    }
    return;
  }

  // Core repo root is one level up from e2e/.
  const repoRoot = join(__dirname, "..");
  const seedCommand =
    process.env.DATANIKA_E2E_SEED_CMD ?? "uv run python -m datanika.scripts.e2e_seed";
  let stdout: string;
  try {
    stdout = execSync(seedCommand, {
      cwd: repoRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      // E2E_SEED_ALLOW_ANY_HOST lets the seed script run even when
      // DATABASE_URL matches a PROD_HOST_MARKER. For local + CI stacks
      // the URL already passes the safety check, but the override keeps
      // the call site uniform and prevents surprises if compose renames
      // its postgres service later. For remote (SSH) seed commands the
      // env var is forwarded but the override applies inside the remote
      // shell only if the ssh call preserves it — callers wire that
      // themselves via `ssh -o SendEnv=E2E_SEED_ALLOW_ANY_HOST ...`.
      env: { ...process.env, E2E_SEED_ALLOW_ANY_HOST: "1" },
    });
  } catch (err) {
    const error = err as { stdout?: Buffer; stderr?: Buffer; message: string };
    const detail = [
      `${seedCommand} failed: ${error.message}`,
      error.stdout ? `--- stdout ---\n${error.stdout.toString()}` : "",
      error.stderr ? `--- stderr ---\n${error.stderr.toString()}` : "",
    ]
      .filter(Boolean)
      .join("\n");
    throw new Error(detail);
  }

  let payload: SeedFixture;
  try {
    payload = JSON.parse(stdout);
  } catch (err) {
    throw new Error(
      `${seedCommand} did not return valid JSON on stdout. First 500 chars:\n${stdout.slice(
        0,
        500,
      )}\nUnderlying error: ${(err as Error).message}`,
    );
  }

  const requiredKeys: (keyof SeedFixture)[] = [
    "org_id",
    "org_slug",
    "user_id",
    "user_email",
    "user_password",
    "connection_id",
    "connection_name",
    "connection_type",
    "seeded_at",
  ];
  const missing = requiredKeys.filter((k) => payload[k] === undefined);
  if (missing.length > 0) {
    throw new Error(
      `Seed payload is missing required keys: ${missing.join(", ")}. ` +
        `Got keys: ${Object.keys(payload).join(", ")}. ` +
        `Update e2e/global-setup.ts and e2e/fixtures/auth.ts to match the new shape.`,
    );
  }

  process.env.DATANIKA_E2E_ORG_ID = String(payload.org_id);
  process.env.DATANIKA_E2E_ORG_SLUG = payload.org_slug;
  process.env.DATANIKA_E2E_USER_ID = String(payload.user_id);
  process.env.DATANIKA_E2E_USER_EMAIL = payload.user_email;
  process.env.DATANIKA_E2E_USER_PASSWORD = payload.user_password;
  process.env.DATANIKA_E2E_CONNECTION_ID = String(payload.connection_id);
  process.env.DATANIKA_E2E_CONNECTION_NAME = payload.connection_name;
  process.env.DATANIKA_E2E_CONNECTION_TYPE = payload.connection_type;
  process.env.DATANIKA_E2E_SEEDED_AT = payload.seeded_at;

  // Also dump to a file so ad-hoc scripts (curl probes, manual tests) can
  // read it without setting env. .gitignore'd.
  const dumpPath = join(repoRoot, "e2e", ".e2e-fixture.json");
  writeFileSync(dumpPath, JSON.stringify(payload, null, 2));
}

export default globalSetup;

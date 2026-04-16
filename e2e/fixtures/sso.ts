/**
 * Shared SSO test helpers — CF Access headers + backend API context.
 *
 * Playwright's `request.newContext()` does NOT inherit `extraHTTPHeaders`
 * from `playwright.config.ts`. Those headers (CF-Access-Client-Id/Secret)
 * only apply to `page.*` calls. Any spec that creates its own API context
 * must forward them explicitly, or CF Access blocks the request and
 * redirects to a stale hostname (core#222).
 */

const BACKEND_URL = process.env.DATANIKA_E2E_BACKEND_URL ?? "http://localhost:8000";

/** CF Access headers — empty strings when env vars are unset (local dev). */
export function cfAccessHeaders(): Record<string, string> {
  const headers: Record<string, string> = {};
  const id = process.env.DATANIKA_STAGING_CF_ACCESS_CLIENT_ID;
  const secret = process.env.DATANIKA_STAGING_CF_ACCESS_CLIENT_SECRET;
  if (id && secret) {
    headers["CF-Access-Client-Id"] = id;
    headers["CF-Access-Client-Secret"] = secret;
  }
  return headers;
}

/** Options for `playwright.request.newContext()` targeting the backend. */
export function backendContextOptions(overrides?: {
  maxRedirects?: number;
  extraHTTPHeaders?: Record<string, string>;
}) {
  return {
    baseURL: BACKEND_URL,
    extraHTTPHeaders: {
      ...cfAccessHeaders(),
      ...(overrides?.extraHTTPHeaders ?? {}),
    },
    ...(overrides?.maxRedirects !== undefined
      ? { maxRedirects: overrides.maxRedirects }
      : {}),
  };
}

export { BACKEND_URL };

"""Patch the Reflex-generated vite.config.ts to add an API proxy.

Reflex's frontend (Vite SSR at :3000) has no proxy for /api/* paths,
so requests to SSO/OAuth/API endpoints return the SPA shell instead of
proxying to the backend (:8000). This script injects a Vite server.proxy
rule into the generated config.

Run after `reflex init` and before `reflex run`:
    python scripts/patch-vite-proxy.py

See: https://github.com/datanika-io/datanika-core/issues/200
"""

import re
import sys
from pathlib import Path

VITE_CONFIG = Path(".web/vite.config.ts")

PROXY_BLOCK = """\
    proxy: {
      "/api/": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },"""


def patch():
    if not VITE_CONFIG.exists():
        print(f"SKIP: {VITE_CONFIG} not found (run 'reflex init' first)", file=sys.stderr)
        return

    content = VITE_CONFIG.read_text(encoding="utf-8")

    if '"/api/"' in content:
        print(f"SKIP: {VITE_CONFIG} already has /api/ proxy")
        return

    # Insert proxy block inside the server: { ... } section, after the port line
    patched = re.sub(
        r"(server:\s*\{[^}]*?port:\s*process\.env\.PORT,?)",
        r"\1\n" + PROXY_BLOCK,
        content,
        count=1,
    )

    if patched == content:
        print(f"WARN: could not find injection point in {VITE_CONFIG}", file=sys.stderr)
        # Fallback: insert before the closing of defineConfig
        patched = content.replace(
            "server: {",
            "server: {\n" + PROXY_BLOCK,
            1,
        )

    VITE_CONFIG.write_text(patched, encoding="utf-8")
    print(f"PATCHED: {VITE_CONFIG} — /api/ proxy → http://127.0.0.1:8000")


if __name__ == "__main__":
    patch()

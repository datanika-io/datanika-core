"""Seed SSO configs in Datanika for E2E tests.

Creates two orgs with SSO configurations:
  - e2e-fixture       → OIDC pointing to Authentik
  - e2e-fixture-saml  → SAML pointing to Authentik

Run after bootstrap-authentik.sh and before Playwright specs:
    python e2e/scripts/seed-sso-configs.py

Reads Authentik URLs from e2e/.sso-fixture.json (written by bootstrap).
Requires the Datanika app's Python env with DB access.
"""

import json
import sys
from pathlib import Path

# Add project root to path so we can import datanika
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datanika.config import settings  # noqa: E402
from datanika.db import get_sync_session  # noqa: E402
from datanika.models.sso_config import SSOConfig  # noqa: E402
from datanika.models.user import Organization  # noqa: E402
from datanika.services.encryption import EncryptionService  # noqa: E402
from datanika.services.sso_service import SSOService  # noqa: E402
from sqlalchemy import select  # noqa: E402

FIXTURE_PATH = Path(__file__).parent.parent / ".sso-fixture.json"


def main():
    if not FIXTURE_PATH.exists():
        print(f"ERROR: {FIXTURE_PATH} not found. Run bootstrap-authentik.sh first.")
        return 1

    fixture = json.loads(FIXTURE_PATH.read_text())
    enc = EncryptionService(settings.credential_encryption_key)
    svc = SSOService(enc)

    with get_sync_session() as s:
        # --- OIDC org ---
        oidc_org = _ensure_org(s, "E2E Fixture", "e2e-fixture")
        _ensure_sso(s, svc, oidc_org.id, "oidc", "Authentik OIDC", {
            "issuer_url": fixture["oidc"]["issuer_url"],
            "client_id": fixture["oidc"]["client_id"],
            "client_secret": fixture["oidc"]["client_secret"],
        })
        print(f"OIDC: org_id={oidc_org.id} slug=e2e-fixture")

        # --- SAML org ---
        saml_org = _ensure_org(s, "E2E Fixture SAML", "e2e-fixture-saml")
        _ensure_sso(s, svc, saml_org.id, "saml", "Authentik SAML", {
            "idp_metadata_url": fixture["saml"]["idp_metadata_url"],
            "idp_entity_id": fixture["saml"]["idp_entity_id"],
            "idp_sso_url": fixture["saml"]["idp_sso_url"],
            "sp_entity_id": fixture["saml"]["sp_entity_id"],
        })
        print(f"SAML: org_id={saml_org.id} slug=e2e-fixture-saml")

        s.commit()
    print("SSO configs seeded.")
    return 0


def _ensure_org(s, name: str, slug: str) -> Organization:
    org = s.execute(
        select(Organization).where(Organization.slug == slug)
    ).scalar_one_or_none()
    if not org:
        org = Organization(name=name, slug=slug)
        s.add(org)
        s.flush()
    return org


def _ensure_sso(s, svc: SSOService, org_id: int, protocol: str, name: str, config: dict):
    existing = s.execute(
        select(SSOConfig).where(SSOConfig.org_id == org_id, SSOConfig.deleted_at.is_(None))
    ).scalar_one_or_none()
    if existing:
        svc.delete_sso_config(s, org_id)
        s.flush()
    svc.create_sso_config(s, org_id, protocol, name, config)


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env bash
# bootstrap-authentik.sh — configure Authentik 2024.12 with OIDC + SAML apps for E2E SSO tests.
#
# Prerequisites:
#   docker compose -f e2e/docker-compose.test.yml up -d
#   Wait for healthcheck: docker compose -f e2e/docker-compose.test.yml ps
#
# This script:
#   1. Waits for Authentik API to be ready
#   2. Creates an API token via direct DB insert (AUTHENTIK_BOOTSTRAP_TOKEN
#      env var alone doesn't produce a usable Bearer token in 2024.12)
#   3. Creates a test user (sso-user@datanika.test)
#   4. Creates an OIDC provider + application
#   5. Creates a SAML provider + application
#   6. Writes connection details to e2e/.sso-fixture.json (gitignored)
#
# All values are deterministic so the script is idempotent on a fresh container.

set -euo pipefail

AUTHENTIK_URL="${AUTHENTIK_URL:-http://localhost:9000}"
API="${AUTHENTIK_URL}/api/v3"
TOKEN_KEY="e2e-bootstrap-token"

DATANIKA_BASE="${DATANIKA_E2E_BASE_URL:-http://localhost:8000}"
OIDC_REDIRECT_URI="${DATANIKA_BASE}/api/auth/sso/callback"
SAML_ACS_URL="${DATANIKA_BASE}/api/auth/sso/callback"
SAML_SP_ENTITY_ID="datanika"

FIXTURE_FILE="$(dirname "$0")/../.sso-fixture.json"

log() { echo "[bootstrap-authentik] $*"; }
py() { python3 -c "$1"; }

# --- 1. Wait for API ---
log "Waiting for Authentik API at ${AUTHENTIK_URL}..."
for i in $(seq 1 30); do
  if curl -sf "${AUTHENTIK_URL}/-/health/ready/" > /dev/null 2>&1; then
    log "Authentik ready (attempt ${i})."
    break
  fi
  [ "$i" -eq 30 ] && { log "ERROR: Authentik not ready after 30 attempts."; exit 1; }
  sleep 2
done

# --- 2. Create API token via DB (2024.12 workaround) ---
log "Ensuring API token exists in DB..."
docker exec e2e-authentik-db-1 psql -U authentik -d authentik -c "
INSERT INTO authentik_core_token (token_uuid, identifier, key, intent, expiring, description, user_id)
SELECT gen_random_uuid(), 'e2e-api-token', '${TOKEN_KEY}', 'api', false, 'E2E bootstrap',
       id FROM authentik_core_user WHERE username = 'akadmin' LIMIT 1
ON CONFLICT (identifier) DO NOTHING;
" > /dev/null 2>&1
log "API token ready."

# --- 3. Set admin password (may not apply from env var on first boot) ---
log "Setting admin password..."
docker exec -i e2e-authentik-server-1 ak changepassword akadmin <<< $'e2e-admin-password\ne2e-admin-password' > /dev/null 2>&1 || true

AUTH_HEADER="Authorization: Bearer ${TOKEN_KEY}"
api() {
  local method="$1" path="$2"; shift 2
  local response
  response=$(curl -s -w "\n%{http_code}" -X "${method}" "${API}${path}" -H "${AUTH_HEADER}" -H "Content-Type: application/json" "$@")
  local http_code
  http_code=$(echo "$response" | tail -1)
  local body
  body=$(echo "$response" | sed '$d')
  if [ "$http_code" -ge 400 ] 2>/dev/null || [ -z "$body" ]; then
    log "ERROR: API ${method} ${path} → HTTP ${http_code}"
    log "Response: ${body:-<empty>}"
    return 1
  fi
  echo "$body"
}

# --- Verify token works ---
log "Verifying API token..."
if ! api GET '/core/users/?search=akadmin' > /dev/null; then
  log "ERROR: API token not working. Retrying token insert..."
  docker exec e2e-authentik-db-1 psql -U authentik -d authentik -c "
  DELETE FROM authentik_core_token WHERE identifier = 'e2e-api-token';
  INSERT INTO authentik_core_token (token_uuid, identifier, key, intent, expiring, description, user_id)
  SELECT gen_random_uuid(), 'e2e-api-token', '${TOKEN_KEY}', 'api', false, 'E2E bootstrap',
         id FROM authentik_core_user WHERE username = 'akadmin' LIMIT 1;
  " > /dev/null 2>&1
  sleep 2
  api GET '/core/users/?search=akadmin' > /dev/null || { log "FATAL: API token still not working after retry."; exit 1; }
fi
log "API token verified."

# --- 4. Fetch reusable PKs ---
AUTH_FLOW_PK=$(api GET '/flows/instances/?slug=default-provider-authorization-implicit-consent' | py "import json,sys; print(json.load(sys.stdin)['results'][0]['pk'])")
INVAL_FLOW_PK=$(api GET '/flows/instances/?slug=default-provider-invalidation-flow' | py "import json,sys; print(json.load(sys.stdin)['results'][0]['pk'])")
SIGNING_KEY_PK=$(api GET '/crypto/certificatekeypairs/' | py "import json,sys; r=json.load(sys.stdin)['results']; print(r[0]['pk'] if r else '')")
log "Flows: auth=${AUTH_FLOW_PK} inval=${INVAL_FLOW_PK} key=${SIGNING_KEY_PK}"

# --- 5. Create test user ---
log "Creating test user sso-user@datanika.test..."
SSO_USER=$(api POST /core/users/ -d '{
  "username": "sso-user", "name": "SSO Test User",
  "email": "sso-user@datanika.test", "is_active": true
}' 2>/dev/null || true)

if [ -z "$SSO_USER" ]; then
  SSO_USER=$(api GET "/core/users/?search=sso-user@datanika.test" | py "
import json, sys
data = json.load(sys.stdin)
print(json.dumps(data['results'][0]) if data['results'] else '')
")
fi
SSO_USER_ID=$(echo "$SSO_USER" | py "import json,sys; print(json.load(sys.stdin)['pk'])")
log "Test user ID: ${SSO_USER_ID}"

api POST "/core/users/${SSO_USER_ID}/set_password/" -d '{"password": "SsoTestPassword-2026"}' > /dev/null

# --- 6. Create OIDC provider + application ---
log "Creating OIDC provider..."
OIDC_PROVIDER=$(api POST /providers/oauth2/ -d "{
  \"name\": \"datanika-oidc-e2e\",
  \"authorization_flow\": \"${AUTH_FLOW_PK}\",
  \"invalidation_flow\": \"${INVAL_FLOW_PK}\",
  \"client_type\": \"confidential\",
  \"client_id\": \"datanika-oidc-e2e\",
  \"client_secret\": \"oidc-e2e-secret-not-for-production\",
  \"redirect_uris\": [{\"matching_mode\": \"strict\", \"url\": \"${OIDC_REDIRECT_URI}\"}],
  \"signing_key\": \"${SIGNING_KEY_PK}\",
  \"sub_mode\": \"user_email\"
}" 2>/dev/null || true)

if [ -z "$OIDC_PROVIDER" ]; then
  OIDC_PROVIDER=$(api GET "/providers/oauth2/?name=datanika-oidc-e2e" | py "
import json, sys; d = json.load(sys.stdin)
print(json.dumps(d['results'][0]) if d['results'] else '')
")
fi
OIDC_PROVIDER_ID=$(echo "$OIDC_PROVIDER" | py "import json,sys; print(json.load(sys.stdin)['pk'])")
log "OIDC provider ID: ${OIDC_PROVIDER_ID}"

api POST /core/applications/ -d "{
  \"name\": \"Datanika OIDC E2E\", \"slug\": \"datanika-oidc-e2e\",
  \"provider\": ${OIDC_PROVIDER_ID}
}" > /dev/null 2>&1 || log "OIDC application may already exist."

# --- 7. Create SAML provider + application ---
log "Creating SAML provider..."
SAML_PROVIDER=$(api POST /providers/saml/ -d "{
  \"name\": \"datanika-saml-e2e\",
  \"authorization_flow\": \"${AUTH_FLOW_PK}\",
  \"invalidation_flow\": \"${INVAL_FLOW_PK}\",
  \"acs_url\": \"${SAML_ACS_URL}\",
  \"audience\": \"${SAML_SP_ENTITY_ID}\",
  \"issuer\": \"${AUTHENTIK_URL}/application/saml/datanika-saml-e2e/sso/binding/redirect/\",
  \"sp_binding\": \"redirect\"
}" 2>/dev/null || true)

if [ -z "$SAML_PROVIDER" ]; then
  SAML_PROVIDER=$(api GET "/providers/saml/?name=datanika-saml-e2e" | py "
import json, sys; d = json.load(sys.stdin)
print(json.dumps(d['results'][0]) if d['results'] else '')
")
fi
SAML_PROVIDER_ID=$(echo "$SAML_PROVIDER" | py "import json,sys; print(json.load(sys.stdin)['pk'])")
log "SAML provider ID: ${SAML_PROVIDER_ID}"

api POST /core/applications/ -d "{
  \"name\": \"Datanika SAML E2E\", \"slug\": \"datanika-saml-e2e\",
  \"provider\": ${SAML_PROVIDER_ID}
}" > /dev/null 2>&1 || log "SAML application may already exist."

# --- 8. Write fixture file ---
OIDC_ISSUER="${AUTHENTIK_URL}/application/o/datanika-oidc-e2e/"
SAML_METADATA_URL="${AUTHENTIK_URL}/application/saml/datanika-saml-e2e/metadata/"
SAML_SSO_URL="${AUTHENTIK_URL}/application/saml/datanika-saml-e2e/sso/binding/redirect/"

cat > "$FIXTURE_FILE" << FIXTURE_EOF
{
  "authentik_url": "${AUTHENTIK_URL}",
  "sso_user_email": "sso-user@datanika.test",
  "sso_user_password": "SsoTestPassword-2026",
  "oidc": {
    "issuer_url": "${OIDC_ISSUER}",
    "client_id": "datanika-oidc-e2e",
    "client_secret": "oidc-e2e-secret-not-for-production"
  },
  "saml": {
    "idp_metadata_url": "${SAML_METADATA_URL}",
    "idp_entity_id": "${AUTHENTIK_URL}/application/saml/datanika-saml-e2e/sso/binding/redirect/",
    "idp_sso_url": "${SAML_SSO_URL}",
    "sp_entity_id": "${SAML_SP_ENTITY_ID}"
  }
}
FIXTURE_EOF

log "Fixture written to ${FIXTURE_FILE}"
log "Done. Authentik is ready for SSO E2E tests."

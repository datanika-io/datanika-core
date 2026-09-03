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

# >&2 is load-bearing. stdout is this script's DATA channel: `api` echoes
# response bodies there and nearly every call is captured with `$(...)`. A
# log line on stdout is therefore indistinguishable from an API response —
# which is how a failed POST came to be returned as the created object, and
# how the 400 body that explained it was destroyed instead of printed.
log() { echo "[bootstrap-authentik] $*" >&2; }
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
  if [ "$http_code" -ge 400 ] 2>/dev/null; then
    log "ERROR: API ${method} ${path} → HTTP ${http_code}"
    log "Response: ${body:-<empty>}"
    return 1
  fi
  # 2xx with empty body (e.g. 204 No Content) is valid — just print nothing
  [ -n "$body" ] && echo "$body"
  return 0
}

# --- create-or-UPDATE -------------------------------------------------------
#
# 🚨 The idiom this replaces was `POST ... 2>/dev/null || true`, then a GET
# fallback when the POST produced nothing. On a FRESH container the POST wins
# and the object gets the body you wrote. On a box where the object ALREADY
# EXISTS — which is the staging authentik, i.e. the only box the suite actually
# runs against — the POST 400s, `|| true` swallows it, and the fallback fetches
# the existing object **unchanged**.
#
# So every edit to a creation body was inert exactly where it mattered, while
# reading in the diff as though it had taken effect. Three separate SAML fixes
# could have been "shipped" this way with no observable difference; core#830's
# own triage flagged the shape before anyone had a fix to apply through it.
#
# `ensure_object <collection-path> <name-or-slug> <json-body>` POSTs, and on
# any failure PATCHes the existing object by name, so the body is authoritative
# either way. Echoes the resulting object.
ensure_object() {
  local path="$1" ident="$2" body="$3"
  local created existing pk
  # Test the EXIT CODE, not just non-emptiness. `|| true` discarded the one
  # signal that says whether the POST worked, leaving `-n` to decide — and
  # `-n` was satisfied by the error message itself.
  if created=$(api POST "$path" -d "$body" 2>/dev/null) && [ -n "$created" ]; then
    echo "$created"
    return 0
  fi
  # `?name=` is not supported on every collection (applications key on slug),
  # so search and match client-side rather than trusting a query parameter to
  # filter — an unsupported filter returns EVERYTHING and `results[0]` would
  # then silently be some other object.
  existing=$(api GET "${path}?search=${ident}" | py "
import json, sys
d = json.load(sys.stdin)
# name | slug | username: providers key on name, applications on slug, users on
# username. Compare all three rather than per-collection special cases.
for r in d.get('results', []):
    if '${ident}' in (r.get('name'), r.get('slug'), r.get('username')):
        print(json.dumps(r)); break
else:
    print('')
")
  if [ -z "$existing" ]; then
    log "FATAL: ${path} ${ident} could neither be created nor found."
    log "Re-running the POST so its refusal is visible (it is suppressed"
    log "above, where an existing object makes a failed POST expected):"
    api POST "$path" -d "$body" > /dev/null || true
    return 1
  fi
  pk=$(echo "$existing" | py "import json,sys; print(json.load(sys.stdin)['pk'])")
  log "${ident} exists (pk=${pk}) — PATCHing it to match this script's body."
  api PATCH "${path}${pk}/" -d "$body"
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

# Scope mappings — required for userinfo to return claims. Without these
# the OIDC provider issues a token but /userinfo returns 403 because
# Authentik has nothing to map into the response.
SCOPE_PKS=$(api GET '/propertymappings/provider/scope/' | py "
import json, sys
data = json.load(sys.stdin)
wanted = {'openid', 'email', 'profile'}
pks = [p['pk'] for p in data['results'] if p.get('scope_name') in wanted]
print(','.join(pks))
")
log "Flows: auth=${AUTH_FLOW_PK} inval=${INVAL_FLOW_PK} key=${SIGNING_KEY_PK}"
log "Scope mappings (openid+email+profile): ${SCOPE_PKS}"

# An empty SIGNING_KEY_PK used to be tolerated: `r[0]['pk'] if r else ''`. That
# is the quiet path to an UNSIGNED SAML assertion (core#768) and an OIDC
# provider that cannot mint a verifiable id_token, with the script still exiting
# 0. Fail here instead, where the message can say what is missing.
if [ -z "${SIGNING_KEY_PK}" ]; then
  log "FATAL: authentik has no certificate keypair, so nothing can be signed."
  log "       /crypto/certificatekeypairs/ returned no results. A fresh authentik"
  log "       self-generates one on first boot; if this fires, the container is"
  log "       not finished starting or its DB was seeded without one."
  exit 1
fi

# --- 5. Create test user ---
log "Creating test user sso-user@datanika.test..."
# Same create-or-update path as the providers. The user's fields are duller
# than a provider's, but the defect is identical: on a box where `sso-user`
# already exists — staging — a change to its email or `is_active` was inert.
SSO_USER=$(ensure_object /core/users/ sso-user '{
  "username": "sso-user", "name": "SSO Test User",
  "email": "sso-user@datanika.test", "is_active": true
}')
SSO_USER_ID=$(echo "$SSO_USER" | py "import json,sys; print(json.load(sys.stdin)['pk'])")
log "Test user ID: ${SSO_USER_ID}"

api POST "/core/users/${SSO_USER_ID}/set_password/" -d '{"password": "SsoTestPassword-2026"}' > /dev/null
# set_password via API sometimes doesn't apply (2024.12 quirk) — belt-and-suspenders
# via `ak changepassword` inside the server container.
docker exec -i e2e-authentik-server-1 ak changepassword sso-user <<< $'SsoTestPassword-2026\nSsoTestPassword-2026' > /dev/null 2>&1 || true

# --- 6. Create OIDC provider + application ---
log "Creating OIDC provider..."
# Build property_mappings JSON array from comma-separated PKs
SCOPE_JSON=$(echo "${SCOPE_PKS}" | py "
import sys
pks = [p.strip() for p in sys.stdin.read().strip().split(',') if p.strip()]
import json
print(json.dumps(pks))
")
OIDC_PROVIDER=$(ensure_object /providers/oauth2/ datanika-oidc-e2e "{
  \"name\": \"datanika-oidc-e2e\",
  \"authorization_flow\": \"${AUTH_FLOW_PK}\",
  \"invalidation_flow\": \"${INVAL_FLOW_PK}\",
  \"client_type\": \"confidential\",
  \"client_id\": \"datanika-oidc-e2e\",
  \"client_secret\": \"oidc-e2e-secret-not-for-production\",
  \"redirect_uris\": [{\"matching_mode\": \"strict\", \"url\": \"${OIDC_REDIRECT_URI}\"}],
  \"signing_key\": \"${SIGNING_KEY_PK}\",
  \"sub_mode\": \"user_email\",
  \"property_mappings\": ${SCOPE_JSON}
}")
OIDC_PROVIDER_ID=$(echo "$OIDC_PROVIDER" | py "import json,sys; print(json.load(sys.stdin)['pk'])")
log "OIDC provider ID: ${OIDC_PROVIDER_ID}"

# ⚠️ The OIDC half is GREEN on staging today and is routed through
# `ensure_object` anyway. It has the same latent defect — a redirect_uri change
# would have been inert on the existing provider — and "it happens to work" is
# not a reason to leave one of two identical call sites unfixed.
ensure_object /core/applications/ datanika-oidc-e2e "{
  \"name\": \"Datanika OIDC E2E\", \"slug\": \"datanika-oidc-e2e\",
  \"provider\": ${OIDC_PROVIDER_ID}
}" > /dev/null

# --- 7. Create SAML provider + application ---
#
# 🚨 SAML property mappings are NOT optional here (core#830, defect 3 of 5).
# With none, Authentik emits `<saml:AttributeStatement/>` — an EMPTY element,
# which saml-schema-assertion-2.0.xsd forbids (AttributeStatementType requires
# at least one Attribute). python3-saml then refuses the whole Response with
# "Not match the saml-schema-protocol-2.0.xsd" *before* looking at the
# signature. OIDC's mappings were configured from the start and SAML's were
# not, which is why this one hid behind the other two for six weeks.
SAML_MAPPING_PKS=$(api GET '/propertymappings/provider/saml/' | py "
import json, sys
data = json.load(sys.stdin)
# authentik ships 'authentik default SAML Mapping: <name>' entries. Take the
# email/username/name ones — enough to make the AttributeStatement non-empty
# and to give the SP something to map, without depending on an exact label.
wanted = ('email', 'username', 'name')
pks = [p['pk'] for p in data['results']
       if any(w in (p.get('name') or '').lower() for w in wanted)]
print(json.dumps(pks))
")
log "SAML property mappings: ${SAML_MAPPING_PKS}"

# 🚨 NameID must be the EMAIL, not authentik's default opaque hash. Our SP
# metadata requests nameid-format:emailAddress and `_saml_parse` reads the
# NameID *as* the user's address. The captured staging assertion carried
# `Format="...:unspecified"` with value `22cdddb8bfee...` — a 64-hex digest —
# so even past every check above, SSO would have provisioned a user whose email
# is a hash. Defect 5 of 5.
SAML_NAMEID_PK=$(api GET '/propertymappings/provider/saml/' | py "
import json, sys
data = json.load(sys.stdin)
for p in data['results']:
    if 'email' in (p.get('name') or '').lower():
        print(p['pk']); break
else:
    print('')
")

# 🚨 sp_binding MUST be 'post'.
#
# With 'redirect' authentik returns the Response as a DEFLATE'd GET query
# parameter. Our ACS advertises HTTP-POST in its metadata and `_saml_parse`
# reads `await request.form()`, so a GET has no body and the refusal is
# `Missing SAMLResponse` — the FIRST of six raise sites, masking every defect
# behind it. That is core#830, and it is not an application bug: an assertion
# in a URL is logged by proxies, kept in browser history and replayable from
# either, which is why the spec puts Responses on the POST binding.
#
# 🚨 signing_kp is what makes the assertion SIGNED. Without it authentik emits
# no <ds:Signature> at all, and `wantAssertionsSigned: True` refuses it — that
# is core#768, and `wantAssertionsSigned` is the fix for the 2026-07-20
# auth-bypass, so it must never be relaxed to make this pass.
SAML_BODY="{
  \"name\": \"datanika-saml-e2e\",
  \"authorization_flow\": \"${AUTH_FLOW_PK}\",
  \"invalidation_flow\": \"${INVAL_FLOW_PK}\",
  \"acs_url\": \"${SAML_ACS_URL}\",
  \"audience\": \"${SAML_SP_ENTITY_ID}\",
  \"issuer\": \"${AUTHENTIK_URL}/application/saml/datanika-saml-e2e/sso/binding/post/\",
  \"sp_binding\": \"post\",
  \"signing_kp\": \"${SIGNING_KEY_PK}\",
  \"name_id_mapping\": \"${SAML_NAMEID_PK}\",
  \"property_mappings\": ${SAML_MAPPING_PKS}
}"
SAML_PROVIDER=$(ensure_object /providers/saml/ datanika-saml-e2e "$SAML_BODY")
SAML_PROVIDER_ID=$(echo "$SAML_PROVIDER" | py "import json,sys; print(json.load(sys.stdin)['pk'])")
log "SAML provider ID: ${SAML_PROVIDER_ID}"

# Assert the settings that matter actually landed. `ensure_object` PATCHes an
# existing provider, but a PATCH can be accepted and ignored (an unknown field
# name, a read-only attribute), and this script's whole failure mode has been
# changes that read correctly in the diff and never reached the box.
api GET "/providers/saml/${SAML_PROVIDER_ID}/" | py "
import json, sys
p = json.load(sys.stdin)
problems = []
if p.get('sp_binding') != 'post':
    problems.append(f\"sp_binding is {p.get('sp_binding')!r}, not 'post' (core#830)\")
if not p.get('signing_kp'):
    problems.append('signing_kp is unset, so assertions are UNSIGNED (core#768)')
if not p.get('property_mappings'):
    problems.append('no property_mappings, so AttributeStatement is empty and schema-invalid')
if not p.get('name_id_mapping'):
    problems.append('no name_id_mapping, so NameID is an opaque hash, not an email')
if problems:
    sys.exit('SAML provider is misconfigured after ensure:\n  - ' + '\n  - '.join(problems))
print('[bootstrap-authentik] SAML provider verified: post binding, signed, mapped.')
"

ensure_object /core/applications/ datanika-saml-e2e "{
  \"name\": \"Datanika SAML E2E\", \"slug\": \"datanika-saml-e2e\",
  \"provider\": ${SAML_PROVIDER_ID}
}" > /dev/null

# --- 8. Write fixture file ---
OIDC_ISSUER="${AUTHENTIK_URL}/application/o/datanika-oidc-e2e/"
SAML_METADATA_URL="${AUTHENTIK_URL}/application/saml/datanika-saml-e2e/metadata/"
# The Issuer authentik stamps into the assertion and the SSO endpoint the SP
# redirects to must both name the POST binding now (they are compared against
# `saml_idp_entity_id` during validation).
SAML_SSO_URL="${AUTHENTIK_URL}/application/saml/datanika-saml-e2e/sso/binding/post/"

# 🚨 THE TRUST ANCHOR. Defect 4 of 5: this fixture carried a metadata URL, an
# entity id and an SSO url — and no certificate. `seed-sso-configs.py` passes
# `config` straight to `SSOService.create_sso_config`, which reads `idp_cert`,
# so `saml_idp_cert` was seeded EMPTY and `_saml_parse` would refuse with
# "SAML IdP certificate not configured" — the second of six raise sites.
#
# Nothing noticed because the binding defect fires first, so this refusal has
# never once been reached. Fetch it from authentik and fail loudly if absent;
# an empty cert here means an unverifiable IdP, which is the auth-bypass shape.
SAML_IDP_CERT=$(api GET "/crypto/certificatekeypairs/${SIGNING_KEY_PK}/view_certificate/" | py "
import json, sys
pem = json.load(sys.stdin).get('data', '')
# SSOConfig.saml_idp_cert stores the bare base64 body, no PEM armour —
# OneLogin_Saml2_Utils.format_cert re-adds it. Keep it on one line so the
# heredoc below stays valid JSON.
print(''.join(l.strip() for l in pem.splitlines() if 'CERTIFICATE' not in l))
")
if [ -z "${SAML_IDP_CERT}" ]; then
  log "FATAL: could not read the IdP certificate for keypair ${SIGNING_KEY_PK}."
  log "       Seeding an SSO config without one gives the SP no trust anchor,"
  log "       so every assertion is refused (core#768)."
  exit 1
fi
log "IdP certificate: ${#SAML_IDP_CERT} base64 chars"

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
    "idp_entity_id": "${SAML_SSO_URL}",
    "idp_sso_url": "${SAML_SSO_URL}",
    "idp_cert": "${SAML_IDP_CERT}",
    "sp_entity_id": "${SAML_SP_ENTITY_ID}"
  }
}
FIXTURE_EOF

# The heredoc interpolates; a malformed value would produce a file that
# `seed-sso-configs.py` fails to parse minutes later, in another job step.
py "
import json, sys
d = json.load(open('${FIXTURE_FILE}'))
missing = [k for k in ('idp_cert', 'idp_entity_id', 'idp_sso_url', 'sp_entity_id')
           if not d['saml'].get(k)]
if missing:
    sys.exit(f'fixture is missing SAML keys: {missing}')
if 'binding/redirect' in d['saml']['idp_sso_url']:
    sys.exit('fixture still names the redirect binding (core#830)')
"

log "Fixture written to ${FIXTURE_FILE}"
log "Done. Authentik is ready for SSO E2E tests."

#!/usr/bin/env bash
# Post-deploy smoke gate. Fails loud on any broken URL.
#
# Usage: smoke-check.sh BASE_URL URL_LIST_FILE
#
# URL list format (pipe-separated, one per line, # for comments):
#   path|content-type-substring|body-substring-or-empty
#
# Checks per row: HTTP 200, non-empty body, content-type contains substring,
# body contains literal substring (skipped if empty). Sequential, 10s/URL,
# no retries. See plans/growth/SMOKE_URLS.md for the curated list rationale.

set -uo pipefail

BASE_URL="${1:?usage: smoke-check.sh BASE_URL URL_LIST_FILE}"
LIST_FILE="${2:?usage: smoke-check.sh BASE_URL URL_LIST_FILE}"

if [[ ! -f "$LIST_FILE" ]]; then
  echo "ERROR: URL list file not found: $LIST_FILE" >&2
  exit 2
fi

FAIL=0
CHECKED=0

while IFS='|' read -r path ctype body || [[ -n "$path" ]]; do
  # Skip blanks and comments
  trimmed="${path#"${path%%[![:space:]]*}"}"
  [[ -z "$trimmed" || "$trimmed" == \#* ]] && continue

  url="${BASE_URL}${path}"
  CHECKED=$((CHECKED + 1))
  printf "%-60s " "$url"

  headers=$(mktemp)
  body_file=$(mktemp)

  code=$(curl -sS --max-time 10 --compressed \
    -o "$body_file" -D "$headers" -w "%{http_code}" \
    "$url" 2>/dev/null || echo "000")

  size=$(wc -c < "$body_file" | tr -d ' ')
  got_ctype=$(grep -i '^content-type:' "$headers" | head -1 | tr -d '\r\n' || true)

  reason=""
  if [[ "$code" != "200" ]]; then
    reason="status=$code"
  elif [[ "$size" -eq 0 ]]; then
    reason="empty-body"
  elif [[ -n "${ctype// }" ]] && ! echo "$got_ctype" | grep -qi -- "$ctype"; then
    reason="content-type mismatch: want '$ctype' got '${got_ctype#content-type: }'"
  elif [[ -n "${body// }" ]] && ! grep -qF -- "$body" "$body_file"; then
    reason="body missing substring: '$body'"
  fi

  if [[ -z "$reason" ]]; then
    echo "OK  ($code, ${size}b)"
  else
    echo "FAIL  $reason"
    FAIL=$((FAIL + 1))
  fi

  rm -f "$headers" "$body_file"
done < "$LIST_FILE"

echo
if [[ "$FAIL" -gt 0 ]]; then
  echo "Smoke gate FAILED: $FAIL of $CHECKED URL(s) broken"
  exit 1
fi
echo "Smoke gate passed: $CHECKED URL(s) OK"

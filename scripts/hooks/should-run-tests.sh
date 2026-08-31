#!/usr/bin/env bash
# Decides whether the pre-push hook must run the full pytest suite.
#
#   exit 0 = RUN the suite      exit 1 = SKIP it
#
# The whole design rule here is FAIL CLOSED: every error path, every ambiguity, and every
# unrecognised file exits 0. Skipping is the privileged outcome and has to be *earned* by
# positively proving that nothing the suite reads has changed.
#
# ── Why this is a separate file rather than four lines inside `pre-push` ──────────────────
# So it can be tested. `tests/test_hooks/test_pre_push_gating.py` drives it directly with a
# broken range, a missing base ref and a detached HEAD, and asserts it still says RUN. A path
# filter nobody has watched refuse to skip is exactly the kind of guard that silently stops
# guarding — which is the defect class this repo has spent two days finding.
#
# ── The bug this must not reproduce (#556-adjacent) ──────────────────────────────────────
# The helm gate in `pre-push` reads:
#     CHANGED=$(git diff --name-only @{upstream}..HEAD -- deploy/helm/ 2>/dev/null || true)
#     if [ -n "$CHANGED" ]; then helm lint ...; else echo "skipped"; fi
# On a branch that has never been pushed, `@{upstream}` does not resolve. git errors, the
# `2>/dev/null || true` swallows it, CHANGED is empty, and the lint is SKIPPED — on precisely
# the push where a chart change is most likely and least reviewed. It fails OPEN.
#
# That same shape applied to pytest would skip ~4,000 tests on every first push of a branch.
# Note the irony this file exists to end: `pre-push` already documents this exact trap 70 lines
# above the helm gate, where the branch/commit check deliberately uses `origin/dev..HEAD` and
# comments "NOT @{upstream}..HEAD". The lesson was written down and then not applied twelve
# lines later. Hence: resolved against origin/dev, and every failure means RUN.
set -u

BASE_REF="${DATANIKA_HOOK_BASE:-origin/dev}"
EXPLICIT_RANGE="${1:-}"

run() { echo "RUN: $1"; exit 0; }
skip() { echo "SKIP: $1"; exit 1; }

# --- resolve the range, failing closed at every step -------------------------------------
if [ -n "$EXPLICIT_RANGE" ]; then
  RANGE="$EXPLICIT_RANGE"
else
  git rev-parse --verify --quiet "$BASE_REF" >/dev/null 2>&1 \
    || run "cannot resolve base ref '$BASE_REF' (never fetched, or no remote) — running everything"
  git rev-parse --verify --quiet HEAD >/dev/null 2>&1 \
    || run "cannot resolve HEAD (unborn branch?) — running everything"
  MB=$(git merge-base "$BASE_REF" HEAD 2>/dev/null) \
    || run "no merge-base with '$BASE_REF' (unrelated histories?) — running everything"
  [ -n "$MB" ] || run "empty merge-base against '$BASE_REF' — running everything"
  RANGE="$MB..HEAD"
fi

# --- what changed? any git failure here means RUN ----------------------------------------
FILES=$(git diff --name-only "$RANGE" 2>/dev/null) \
  || run "git diff failed for range '$RANGE' — running everything"

# No files is NOT proof of safety — it is equally consistent with a range that silently
# resolved to nothing. The empty case is the one the helm gate got wrong; it runs.
[ -n "$FILES" ] || run "range '$RANGE' reported no changed files — running everything"

# --- mode-only changes cannot affect behaviour -------------------------------------------
# `git diff --numstat` prints "<added> <deleted> <path>", and 0/0 for a file whose content is
# byte-identical (a chmod). Binary files print "-" and are therefore never 0/0, so they run.
NUMSTAT=$(git diff --numstat "$RANGE" 2>/dev/null) \
  || run "git diff --numstat failed for range '$RANGE' — running everything"
# An empty numstat while --name-only reported files should be impossible; treat the
# impossible case as RUN rather than letting it fall into the skip arm.
[ -n "$NUMSTAT" ] \
  || run "numstat empty for '$RANGE' though files changed — running everything"
if printf '%s\n' "$NUMSTAT" | awk '{ if ($1 != "0" || $2 != "0") exit 1 }'; then
  skip "every changed file is mode-only (0 added, 0 deleted) — no content changed"
fi

# --- is every changed file inert to the suite? -------------------------------------------
# Relevant patterns are tested FIRST so that e.g. docs/conf.py is never mistaken for a doc.
# The default arm is "not inert": an unrecognised path always runs.
#
# Deliberately NOT inert, each for a concrete reason:
#   i18n/**            test_i18n.py asserts key parity across all 9 locale files
#   docker-compose*    test_deploy/test_deploy_service_coverage.py reads the service list
#   scripts/**         the same test reads the deploy steps
#   deploy/**          helm templates + service coverage
#   .github/**         cheap to run; a workflow assertion test would otherwise be bypassed
is_inert() {
  case "$1" in
    *.py|*.toml|*.lock|*.cfg|*.ini|*.txt|*.json|*.yml|*.yaml) return 1 ;;
    Dockerfile*|*/Dockerfile*|*.sh|*.conf|*.sql|*.j2|*.tpl) return 1 ;;
    *.md|docs/*|*.png|*.jpg|*.jpeg|*.gif|*.svg|*.ico|*.webp) return 0 ;;
    LICENSE|LICENSE.*|.gitignore|.gitattributes|.editorconfig) return 0 ;;
    *) return 1 ;;
  esac
}

NON_INERT=""
while IFS= read -r f; do
  [ -z "$f" ] && continue
  if ! is_inert "$f"; then
    NON_INERT="$f"
    break
  fi
done <<EOF
$FILES
EOF

[ -z "$NON_INERT" ] \
  && skip "every changed file is documentation or an image (checked $(printf '%s\n' "$FILES" | grep -c .) files)"

run "'$NON_INERT' can affect the suite"

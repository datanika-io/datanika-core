#!/usr/bin/env bash
#
# Resolve the `datanika-cloud` branch that a core CI job should pair against, and
# REFUSE rather than guess (core#923).
#
# Why this is a script and not an inline expression
# -------------------------------------------------
# It used to be `ref: ${{ github.base_ref || github.ref_name }}`, inline, in two
# places. That is correct for `pull_request` and for `push`, and wrong for
# `merge_group`:
#
#   event          base_ref   ref_name                          resolved
#   pull_request   dev        <pr>/merge                         dev   OK
#   push           (empty)    dev / master                       dev   OK
#   merge_group    (EMPTY)    gh-readonly-queue/dev/pr-N-<sha>   that  BROKEN
#
# `gh-readonly-queue/...` exists only in this repo, so the cloud checkout 404s and
# `image-probe` — a REQUIRED check on `dev` — fails in about 46 seconds. Every
# queue entry is then ejected with `reason: failed_checks`, which is what forced
# the rollback of core's merge queue during the core#904 rollout.
#
# The failure mode this script exists for
# ---------------------------------------
# core#923 warns, correctly, that the obvious fix is unverifiable right now: core
# has no merge queue enabled, so nobody can read a real `merge_group` payload, and
# if `github.event.merge_group.base_ref` is not the right field name the
# expression yields empty, falls through to `ref_name`, and reproduces the
# identical failure with the identical symptom.
#
# So the fix is not "use the right field" — it is "make the wrong field
# impossible to ship quietly". This script refuses an empty result and refuses a
# queue branch, by name, before either can reach `actions/checkout`. A wrong field
# now costs one red job with a message that says exactly what to change, instead
# of a silent ejection that reads like a flaky check.
#
# It also prints all three inputs, so the first genuine merge-group run on core
# records the payload's real value in its log — which is the verification #923
# asked for, obtained the only way currently available: by running.
#
# Accepts a full ref (`refs/heads/dev`) as well as a bare branch name, because it
# is not established which shape the merge-group payload uses and both are valid
# inputs to `actions/checkout` anyway. Normalising here keeps the two callers
# identical.

set -euo pipefail

MERGE_GROUP_BASE="${MERGE_GROUP_BASE:-}"
PR_BASE="${PR_BASE:-}"
REF_NAME="${REF_NAME:-}"

echo "merge_group.base_ref = [${MERGE_GROUP_BASE}]"
echo "github.base_ref      = [${PR_BASE}]"
echo "github.ref_name      = [${REF_NAME}]"

ref="${MERGE_GROUP_BASE:-${PR_BASE:-$REF_NAME}}"
ref="${ref#refs/heads/}"

case "$ref" in
  "")
    echo "::error::Could not resolve a cloud branch to pair against: merge_group.base_ref, github.base_ref and github.ref_name are all empty. Refusing to hand an empty ref to actions/checkout (core#923)."
    exit 1
    ;;
  gh-readonly-queue/*)
    echo "::error::Resolved cloud ref [$ref] is a merge-queue branch, which exists only in datanika-core. That means github.event.merge_group.base_ref was empty and the resolution fell through to github.ref_name — i.e. the field name is wrong for this event. Read the three values printed above and correct .github/scripts/resolve-cloud-ref.sh (core#923). NOT a token problem and NOT a flaky check."
    exit 1
    ;;
esac

echo "resolved cloud ref: $ref"
echo "ref=$ref" >> "${GITHUB_OUTPUT:-/dev/stdout}"

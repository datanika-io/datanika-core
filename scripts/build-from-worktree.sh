#!/usr/bin/env bash
#
# Build the production image from an agent WORKTREE (core#1197).
#
# The problem this exists for
# ---------------------------
# `docker-compose.yml` declares `context: ..` and the `Dockerfile` does
# `COPY datanika/ .` + `COPY datanika-cloud/ /cloud/`, so the build context must
# contain directories literally named `datanika` and `datanika-cloud`. That is true
# of the monorepo root and NOT of `worktrees/`, whose members are named
# `datanika-core-<agent>` / `datanika-cloud-<agent>`. So `docker compose build app`
# cannot succeed from the only directory WORKFLOW_RULES §1 permits us to work in.
#
# Measured alternatives that do NOT work, so nobody re-derives them:
#
#   * Junctions/symlinks named `datanika` + `datanika-cloud` in a scratch directory.
#     The shell resolves them; **docker does not follow them in a build context**.
#     A real directory in the same context builds, which is what makes that
#     conclusive rather than a broken probe.
#   * `additional_contexts` + `COPY --from=<ctx>`. Works, but it edits the Dockerfile,
#     which is the path that BUILDS PRODUCTION (`build-push-image.yml`,
#     `deploy-pointer.yml`, `ci.yml`, compose all pass the monorepo root today).
#     Not a trade worth making for a local convenience.
#
# What this does instead
# ----------------------
# Streams a tar of the two worktrees to `docker build -`, renaming them on the fly
# with GNU tar's `--transform` so the stream carries the names the Dockerfile
# expects. Nothing is written to disk, which satisfies core#1197 AC4 by construction.
#
# ⚠️ The CONTEXT cannot collide with another agent; the TAG could, and did (QA,
# 2026-09-15). Every worktree used to build `:worktree`, so one agent's build silently
# re-pointed the tag under another agent's running stack -- and on a containerd image
# store a moved tag cannot be put back. So the default tag is per worktree:
# `:worktree-<agent>`, and `:worktree-main` for the main checkout. `--tag` still wins.
#
# Usage
#   bash scripts/build-from-worktree.sh                    # cloud edition, :worktree-<agent>
#   bash scripts/build-from-worktree.sh --edition core      # no cloud tree
#   bash scripts/build-from-worktree.sh --tag foo:bar
#   bash scripts/build-from-worktree.sh --target variant-cloud
#   bash scripts/build-from-worktree.sh --list-context      # print members, build nothing
#   bash scripts/build-from-worktree.sh --print-tag         # print the tag, build nothing
#
# Then run the stack WITHOUT rebuilding, isolated from every other worktree's stack:
# its own compose project, container names, host-port band and backend URL, and a
# refusal to start any configuration that is not isolated.
#
#   bash scripts/worktree-stack.sh up
#
set -euo pipefail

TAG=""
EDITION="cloud"
TARGET=""
LIST_ONLY=0
PRINT_TAG=0

while [ $# -gt 0 ]; do
  case "$1" in
    --tag)     TAG="${2:?--tag needs a value}"; shift 2 ;;
    --edition) EDITION="${2:?--edition needs a value}"; shift 2 ;;
    --target)  TARGET="${2:?--target needs a value}"; shift 2 ;;
    --list-context) LIST_ONLY=1; shift ;;
    --print-tag) PRINT_TAG=1; shift ;;
    -h|--help) sed -n '2,50p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "$EDITION" in
  core|cloud) ;;
  *) echo "--edition must be 'core' or 'cloud', got '$EDITION'" >&2; exit 2 ;;
esac

# ---------------------------------------------------------------------------
# Locate the trees. Derived from THIS script's own location, not from $PWD, so it
# behaves the same however it is invoked.
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PARENT="$(cd "$CORE_DIR/.." && pwd)"
CORE_NAME="$(basename "$CORE_DIR")"

# `datanika-core-<agent>` -> `<agent>`; the main checkout is plain `datanika`.
if [ "$CORE_NAME" = "datanika" ]; then
  AGENT="main"
  CLOUD_NAME="datanika-cloud"
else
  AGENT="${CORE_NAME#datanika-core-}"
  if [ "$AGENT" = "$CORE_NAME" ]; then
    echo "FATAL: '$CORE_NAME' is not 'datanika' or 'datanika-core-<agent>'." >&2
    echo "       Cannot infer the matching cloud tree. Run from a canonical worktree." >&2
    exit 1
  fi
  CLOUD_NAME="datanika-cloud-${AGENT}"
fi

# Per worktree, never shared (see the header). scripts/worktree_stack.py derives the same
# tag for the stack it runs; tests/test_deploy/test_build_from_worktree.py holds them together.
TAG="${TAG:-ghcr.io/datanika-io/datanika-core:worktree-${AGENT}}"
if [ "$PRINT_TAG" -eq 1 ]; then
  echo "$TAG"
  exit 0
fi

# ---------------------------------------------------------------------------
# Fail CLOSED. Each of these is a way the build would otherwise produce a wrong
# image quietly rather than an error: a missing pyproject means the rename landed
# somewhere unexpected, and a missing cloud tree in the cloud edition means
# `COPY datanika-cloud/` would fail deep in the build after minutes of work.
# ---------------------------------------------------------------------------
[ -f "$CORE_DIR/Dockerfile" ]      || { echo "FATAL: no Dockerfile in $CORE_DIR" >&2; exit 1; }
[ -f "$CORE_DIR/pyproject.toml" ]  || { echo "FATAL: no pyproject.toml in $CORE_DIR" >&2; exit 1; }

MEMBERS=("$CORE_NAME")
if [ "$EDITION" = "cloud" ]; then
  if [ ! -f "$PARENT/$CLOUD_NAME/pyproject.toml" ]; then
    echo "FATAL: cloud edition needs '$PARENT/$CLOUD_NAME' and it is missing or empty." >&2
    echo "       Use --edition core to build without the cloud tree." >&2
    exit 1
  fi
  MEMBERS+=("$CLOUD_NAME")
fi

tar --version 2>/dev/null | head -1 | grep -qi 'GNU tar' || {
  echo "FATAL: GNU tar is required for --transform (found: $(tar --version 2>&1 | head -1))" >&2
  exit 1
}

# ---------------------------------------------------------------------------
# The stream. `--transform` rewrites the leading path component to the name the
# Dockerfile expects. Exclusions mirror the deploy's own tar in CLAUDE.md.
# ---------------------------------------------------------------------------
tar_stream() {
  tar -cf - -C "$PARENT" \
    --exclude='*/.venv' \
    --exclude='*/.web' \
    --exclude='*/node_modules' \
    --exclude='*/__pycache__' \
    --exclude='*/.pytest_cache' \
    --exclude='*/.ruff_cache' \
    --exclude-vcs \
    --transform="s|^${CORE_NAME}\(/\|$\)|datanika\1|" \
    --transform="s|^${CLOUD_NAME}\(/\|$\)|datanika-cloud\1|" \
    "${MEMBERS[@]}"
}

if [ "$LIST_ONLY" -eq 1 ]; then
  tar_stream | tar -tf -
  exit 0
fi

echo "build-from-worktree: $CORE_NAME -> datanika/"
[ "$EDITION" = "cloud" ] && echo "build-from-worktree: $CLOUD_NAME -> datanika-cloud/"
echo "build-from-worktree: edition=$EDITION tag=$TAG${TARGET:+ target=$TARGET}"

BUILD_ARGS=(build -f datanika/Dockerfile -t "$TAG" --build-arg "DATANIKA_IMAGE_EDITION=$EDITION")
[ -n "$TARGET" ] && BUILD_ARGS+=(--target "$TARGET")

tar_stream | docker "${BUILD_ARGS[@]}" -

echo "build-from-worktree: built $TAG"
echo
if [ "$TAG" = "ghcr.io/datanika-io/datanika-core:worktree-${AGENT}" ]; then
  echo "Run the stack without rebuilding, isolated from every other worktree's:"
  echo "  bash scripts/worktree-stack.sh up"
else
  echo "Built a custom --tag. scripts/worktree-stack.sh runs :worktree-${AGENT}, not $TAG."
fi

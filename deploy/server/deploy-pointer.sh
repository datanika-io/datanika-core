#!/bin/bash
# One-command deploy to the pointer.gr prod box (build-from-source).
#
# Why not GitHub-Actions CD? The old Hetzner CD did `git pull + docker build` on
# the server. This box has NO GitHub auth and NO .git checkouts (source was
# tar-transferred), and GHCR is inaccessible — so we build from local source and
# push it. Full GHA CD needs a build-on-GHA + image/source transfer workflow
# (tracked as I3c in PLAN_INFRASTRUCTURE.md); until then this is the deploy path.
#
# Run from the dev machine (Git Bash):  bash plans/infra/scripts/deploy-pointer.sh
# It NEVER touches the box's .env.docker (excluded from the tar).

set -euo pipefail

BOX=root@185.25.22.188
KEY=~/.ssh/id_ed25519
SRC=/d/Projects/Datanika
SSH="ssh -i ${KEY} -o ConnectTimeout=20 -o ServerAliveInterval=30"

echo ">> building source tarball..."
tar czf /tmp/datanika-deploy.tgz -C "${SRC}" \
    --exclude-vcs --exclude='*/.venv' --exclude='datanika/.web' \
    --exclude='*/node_modules' --exclude='*/__pycache__' --exclude='*.pyc' \
    --exclude='*/.ruff_cache' --exclude='*/.pytest_cache' --exclude='*/htmlcov' \
    --exclude='datanika/.env' --exclude='datanika/.env.docker' \
    --exclude='datanika/dbt_projects/tenant_*' \
    datanika datanika-cloud

echo ">> transferring + extracting (box .env.docker preserved)..."
cat /tmp/datanika-deploy.tgz | ${SSH} "${BOX}" 'cd /opt/datanika && tar xzf -'
rm -f /tmp/datanika-deploy.tgz

echo ">> building image + recreating app/celery..."
${SSH} "${BOX}" 'cd /opt/datanika/datanika && set -a && . ./.env.docker && set +a && \
    docker compose build app celery && \
    docker compose up -d postgres redis app celery'

echo ">> waiting for health..."
${SSH} "${BOX}" 'for i in $(seq 1 40); do \
    h=$(docker inspect --format "{{.State.Health.Status}}" datanika-app 2>/dev/null); \
    if [ "$h" = "healthy" ]; then echo "app healthy"; exit 0; fi; sleep 5; done; \
    echo "app did not become healthy in time"; docker logs --tail 30 datanika-app; exit 1'

echo ">> smoke through Cloudflare..."
curl -s -o /dev/null -w "app.datanika.io/login -> HTTP %{http_code}\n" https://app.datanika.io/login
echo ">> deploy complete."

#!/usr/bin/env bash
# Build the service image on the NUC and (re)deploy the stack at /srv/distsem.
# Usage: deploy/nuc/deploy.sh [--skip-build] [--skip-tests]
set -euo pipefail
cd "$(dirname "$0")/../.."

NUC_SSH=nuc              # ssh alias for the WSL Docker host
NUC_CONTEXT=nuc          # docker context pointing at it
STACK_DIR=/srv/distsem
TAG=$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d%H%M%S)
SKIP_BUILD=false; SKIP_TESTS=false
for arg in "$@"; do
  case "$arg" in
    --skip-build) SKIP_BUILD=true ;;
    --skip-tests) SKIP_TESTS=true ;;
    *) echo "unknown option $arg" >&2; exit 2 ;;
  esac
done

if [ "$SKIP_BUILD" = false ]; then
  echo "==> building jar"
  if [ "$SKIP_TESTS" = true ]; then mvn -B -q package -DskipTests; else mvn -B -q verify; fi
  echo "==> building image distsem/semaphore-service:$TAG on the NUC"
  docker --context "$NUC_CONTEXT" build -q \
    -t "distsem/semaphore-service:$TAG" -t distsem/semaphore-service:latest semaphore-service
fi

echo "==> syncing stack files to $STACK_DIR"
ssh "$NUC_SSH" "mkdir -p $STACK_DIR"
scp -q deploy/nuc/docker-compose.yml deploy/nuc/Caddyfile deploy/nuc/.env.example "$NUC_SSH:$STACK_DIR/"
ssh "$NUC_SSH" "test -f $STACK_DIR/.env" || { echo "!! $STACK_DIR/.env missing (see .env.example)" >&2; exit 1; }

echo "==> docker compose up"
ssh "$NUC_SSH" "cd $STACK_DIR && DISTSEM_TAG=$TAG docker compose up -d --wait --wait-timeout 120" 2>&1 | tail -5

PROM_CFG=/srv/observability/prometheus/prometheus.yml
if ! ssh "$NUC_SSH" "grep -q 'job_name: distsem' $PROM_CFG"; then
  echo "==> registering distsem scrape job with the shared Prometheus"
  ssh "$NUC_SSH" "cp $PROM_CFG $PROM_CFG.bak-distsem && cat >> $PROM_CFG" < deploy/nuc/prometheus-job.yml
  ssh "$NUC_SSH" "docker kill -s HUP obs-prometheus >/dev/null"
fi

echo "==> health"
ssh "$NUC_SSH" "curl -fsS http://127.0.0.1:8183/actuator/health/readiness" && echo
echo "==> done. Tunnel: ssh -N -L 8183:127.0.0.1:8183 $NUC_SSH  ->  http://localhost:8183/swagger-ui.html"

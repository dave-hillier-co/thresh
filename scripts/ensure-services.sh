#!/usr/bin/env bash
#
# Ensure the backing services the provider-backed suites need (Redis, Postgres,
# Kafka) are up and healthy, and print the URLs to point the suites at.
#
# These run on NON-DEFAULT host ports on purpose. The suites default to
# localhost:6379 / 5432 / 9092, and a dev box or a shared CI runner very often
# already has something else on those -- another project's Postgres, a stray
# Redis. Reusing whatever happens to answer is worse than not running at all:
# the suites probe their service at load time and `describe.skipIf` themselves
# when it is unusable, so a foreign service on the right port produces a green
# run that silently tested nothing. Dedicated ports make that impossible.
#
# Usage:
#   ./scripts/ensure-services.sh            # start, wait, print the env
#   ./scripts/ensure-services.sh --quiet    # only speak up on failure
#   ./scripts/ensure-services.sh --env      # print only the export lines
#   ./scripts/ensure-services.sh --down     # stop and remove the containers
#
# To use the URLs in a shell:
#   eval "$(./scripts/ensure-services.sh --env)"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE=("docker" "compose" "-f" "$REPO_ROOT/docker-compose.yml")

# Keep in step with the published ports in docker-compose.yml.
REDIS_PORT=6390
PG_PORT=5433
KAFKA_PORT=9192

REDIS_URL="redis://localhost:${REDIS_PORT}"
PG_URL="postgres://postgres@localhost:${PG_PORT}/postgres"
KAFKA_BROKERS="localhost:${KAFKA_PORT}"

print_env() {
  echo "export REDIS_URL='${REDIS_URL}'"
  echo "export PG_URL='${PG_URL}'"
  echo "export KAFKA_BROKERS='${KAFKA_BROKERS}'"
}

QUIET=false
case "${1:-}" in
  --quiet) QUIET=true ;;
  --env) print_env; exit 0 ;;
  --down) "${COMPOSE[@]}" down --remove-orphans; exit 0 ;;
  "") ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac

log() { [[ "$QUIET" == true ]] || echo "$@"; }
err() { echo "$@" >&2; }

# Probe each service the way its suite does -- a real protocol round trip, not a
# TCP connect. `docker compose --wait` already gates on the healthchecks, but
# these run from the host, so they also prove the published port is actually
# wired through, which on macOS can lag the container becoming healthy.
probe_redis() { "${COMPOSE[@]}" exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; }
probe_postgres() { "${COMPOSE[@]}" exec -T postgres pg_isready -U postgres -q 2>/dev/null; }
probe_kafka() { "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; }

all_healthy() { probe_redis && probe_postgres && probe_kafka; }

if all_healthy; then
  log "[ok] redis, postgres and kafka are already up"
  [[ "$QUIET" == true ]] || print_env
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  err "[error] docker is not installed. Install Docker Desktop or colima and retry."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  err "[error] docker is installed but the daemon is not responding."
  err "        Start Docker Desktop (or colima) and retry."
  exit 1
fi

log "[..] starting services"
"${COMPOSE[@]}" up -d --wait --wait-timeout 180

deadline=$(( SECONDS + 90 ))
until all_healthy; do
  if (( SECONDS >= deadline )); then
    err "[error] services started but did not become usable:"
    probe_redis    || err "        redis (${REDIS_PORT})"
    probe_postgres || err "        postgres (${PG_PORT})"
    probe_kafka    || err "        kafka (${KAFKA_PORT})"
    err ""
    "${COMPOSE[@]}" ps >&2 || true
    exit 1
  fi
  sleep 2
done

log "[ok] redis, postgres and kafka are up"
[[ "$QUIET" == true ]] || print_env

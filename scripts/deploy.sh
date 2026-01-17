#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT_DIR}/docker/compose.prod.yml}"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env.prod}"

ENV_FILE="${ENV_FILE}" "${ROOT_DIR}/scripts/preflight.sh"

docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" pull
docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" build --pull
docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" up -d --remove-orphans

ENV_FILE="${ENV_FILE}" "${ROOT_DIR}/scripts/migrate.sh"
ENV_FILE="${ENV_FILE}" "${ROOT_DIR}/scripts/smoke_prod.sh"

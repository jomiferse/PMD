#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA_DIR="${INFRA_DIR:-${ROOT_DIR}/../pmd_infra}"
COMPOSE_FILE="${COMPOSE_FILE:-${INFRA_DIR}/compose/compose.prod.yml}"
ENV_FILE="${ENV_FILE:-${INFRA_DIR}/.env}"

docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" ps

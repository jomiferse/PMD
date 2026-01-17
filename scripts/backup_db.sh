#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT_DIR}/docker/compose.prod.yml}"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env.prod}"
BACKUP_DIR="${BACKUP_DIR:-${ROOT_DIR}/backups}"
RETENTION_COUNT="${RETENTION_COUNT:-7}"

mkdir -p "${BACKUP_DIR}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup_name="pmd_${timestamp}.sql"

docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" exec -T postgres \
  sh -c "pg_dump -U \"\${POSTGRES_USER}\" \"\${POSTGRES_DB}\" > /backups/${backup_name}"

ls -1t "${BACKUP_DIR}"/pmd_*.sql | tail -n +"$((RETENTION_COUNT + 1))" | xargs -r rm --

echo "Backup saved to ${BACKUP_DIR}/${backup_name}"

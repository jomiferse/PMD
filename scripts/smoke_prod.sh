#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT_DIR}/docker/compose.prod.yml}"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env.prod}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

if [[ -z "${NEXT_PUBLIC_API_BASE_URL:-}" || -z "${APP_URL:-}" ]]; then
  echo "NEXT_PUBLIC_API_BASE_URL and APP_URL must be set for smoke tests." >&2
  exit 1
fi

echo "Checking API health..."
curl -fsS "${NEXT_PUBLIC_API_BASE_URL%/}/health" >/dev/null

echo "Checking frontend..."
curl -fsS "${APP_URL%/}/" >/dev/null

echo "Checking DB connectivity from api container..."
docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" exec -T api \
  python - <<'PY'
import os
import psycopg

url = os.environ["DATABASE_URL"]
with psycopg.connect(url) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        print(cur.fetchone()[0])
PY

echo "Smoke tests OK."

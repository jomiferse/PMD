#!/usr/bin/env bash
set -euo pipefail

API_KEY="${1:-}"
if [[ -z "${API_KEY}" ]]; then
  echo "Usage: $0 <api_key>"
  exit 1
fi

base_url="${PMD_BASE_URL:-http://localhost:8000}"

curl -sS -H "X-API-Key: ${API_KEY}" "${base_url}/status"
echo
curl -sS -H "X-API-Key: ${API_KEY}" "${base_url}/alerts/summary"
echo

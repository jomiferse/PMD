#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-.env.prod}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

required_vars=(
  ENV
  APP_DOMAIN
  API_DOMAIN
  CADDY_EMAIL
  APP_URL
  NEXT_PUBLIC_API_BASE_URL
  NEXT_PUBLIC_TELEGRAM_BOT_USERNAME
  POSTGRES_PASSWORD
  DATABASE_URL
  REDIS_URL
  SESSION_SECRET
  ADMIN_API_KEY
  STRIPE_SECRET_KEY
  STRIPE_WEBHOOK_SECRET
  STRIPE_BASIC_PRICE_ID
  STRIPE_PRO_PRICE_ID
  STRIPE_ELITE_PRICE_ID
  TELEGRAM_BOT_TOKEN
  POLYMARKET_BASE_URL
  OPENAI_API_KEY
)

missing=()
for var in "${required_vars[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    missing+=("${var}")
  fi
done

if (( ${#missing[@]} > 0 )); then
  echo "Missing required env vars: ${missing[*]}" >&2
  exit 1
fi

if [[ "${ENV}" != "production" ]]; then
  echo "ENV must be set to production for deploys." >&2
  exit 1
fi

if [[ "${SESSION_SECRET}" == "change-me" ]]; then
  echo "SESSION_SECRET must be changed for production." >&2
  exit 1
fi

if [[ "${POSTGRES_PASSWORD}" == "postgres" || "${POSTGRES_PASSWORD}" == "change-me" ]]; then
  echo "POSTGRES_PASSWORD must be a strong, non-default value." >&2
  exit 1
fi

echo "Preflight OK."

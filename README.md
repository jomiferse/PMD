# PMD Backend

## Overview
PMD backend is the FastAPI service that powers the PMD dashboard and digests.
It runs the HTTP API plus background worker and scheduler, backed by Postgres and Redis.

## Quickstart
- Use the infra dev stack (recommended):

```bash
cd ../pmd_infra
cp env/dev.env.example .env
./scripts/dev.sh
```

- Run migrations:

```bash
docker compose -f compose/compose.dev.yml exec api alembic upgrade head
```

- Plan definitions (basic/pro/elite) are seeded via Alembic data migrations. No manual seed scripts are required.

## Configuration
- Common env vars (names only): DATABASE_URL, REDIS_URL, POLYMARKET_BASE_URL, SESSION_SECRET, TELEGRAM_BOT_TOKEN, OPENAI_API_KEY, STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET, STRIPE_BASIC_PRICE_ID, STRIPE_PRO_PRICE_ID, STRIPE_ELITE_PRICE_ID.
- See `../pmd_infra/env/dev.env.example` for full list and defaults.

## Links
- Infra runbook and deployment: `../pmd_infra/README.md`
- Key endpoints: `/health`, `/me`, `/alerts/latest`, `/copilot/runs`, `/billing/*`

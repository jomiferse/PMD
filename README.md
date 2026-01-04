# PMD - Polymarket Mispricing Detector (v2.3)

Read-only analytics. Not financial advice. No guarantee of outcomes. No custody. No execution.

PMD ingests Polymarket Gamma data, stores 5-minute snapshots, and emits dislocation alerts
based on percentage price movement over a time window. It does not execute trades.
Alerts are delivered as per-user Telegram digests with operator-managed preferences.

## Quick start

1) Copy env file and fill required values:

```bash
cp .env.example .env
```

2) Build and start services:

```bash
docker compose up -d --build
```

3) Run migrations:

```bash
docker compose exec api alembic upgrade head
```

4) Create an API key (prints the raw key once):

```bash
docker compose exec api python -m app.scripts.create_api_key --name prod
```

5) Call endpoints:

```bash
curl -H "X-API-Key: <key>" http://localhost:8000/alerts/latest
```

## Endpoints

- `GET /health` (no auth)
- `POST /jobs/ingest` (auth)
- `GET /snapshots/latest` (auth)
- `GET /alerts/latest` (auth)
- `GET /alerts/summary` (auth)
- `GET /status` (auth)
- `GET /admin/users` (admin)
- `GET /admin/users/{id}/last-digest` (admin)
- `GET /admin/ai-recommendations` (admin)
- `GET /admin/stats` (admin)
- `POST /telegram/webhook` (no auth; Telegram callback)

## Alert logic (dislocation)

An alert triggers when:
- Price moves by at least `MEDIUM_MOVE_THRESHOLD` within `WINDOW_MINUTES`
- Liquidity is at least `MEDIUM_MIN_LIQUIDITY`
- Volume24h is at least `MEDIUM_MIN_VOLUME_24H`
- The same market has not alerted within `ALERT_COOLDOWN_MINUTES`

Alerts store old/new price, delta percent, and the trigger timestamp.
Per-user delivery applies preference filters at digest time and logs alert deliveries.

## Environment variables

Required:
- `DATABASE_URL`
- `REDIS_URL`
- `POLYMARKET_BASE_URL`

Optional:
- `POLY_LIMIT` (default 100)
- `POLY_PAGE_LIMIT` (default 100)
- `POLY_MAX_EVENTS` (default unset/None; failsafe only)
- `POLY_MAX_PAGES` (default 100; failsafe only)
- `POLY_START_OFFSET` (default 0)
- `POLY_ORDER` (default unset)
- `POLY_ASCENDING` (default unset)
- `POLY_USE_SERVER_FILTERS` (default true)
- `POLY_LIQUIDITY_MIN` (default unset/None)
- `POLY_VOLUME_MIN` (default unset/None)
- `POLY_USE_GLOBAL_MINIMUMS` (default true)
- `INGEST_INTERVAL_SECONDS` (default 300)
- `CLEANUP_ENABLED` (default true)
- `CLEANUP_SCHEDULE_HOUR_UTC` (default 3)
- `SNAPSHOT_RETENTION_DAYS` (default 7)
- `ALERT_RETENTION_DAYS` (default 30)
- `DELIVERY_RETENTION_DAYS` (default 30)
- `MIN_LIQUIDITY` (default 1000)
- `MIN_VOLUME_24H` (default 1000)
- `WINDOW_MINUTES` (default 60)
- `ALERT_COOLDOWN_MINUTES` (default 30)
- `TELEGRAM_BOT_TOKEN`
- `ADMIN_API_KEY`
- `OPENAI_API_KEY`
- `LLM_API_BASE` (default `https://api.openai.com/v1/chat/completions`)
- `LLM_MODEL` (default `gpt-4o-mini`)
- `LLM_TIMEOUT_SECONDS` (default 15)
- `LLM_MAX_RETRIES` (default 2)
- `LLM_CACHE_TTL_SECONDS` (default 3600)
- `GLOBAL_MIN_LIQUIDITY` (default 1000)
- `GLOBAL_MIN_VOLUME_24H` (default 1000)
- `GLOBAL_DIGEST_WINDOW` (default 60)
- `GLOBAL_MAX_ALERTS` (default 7)
- `PYES_ACTIONABLE_MIN` (default 0.15)
- `PYES_ACTIONABLE_MAX` (default 0.85)
- `MAX_ACTIONABLE_PER_DIGEST` (default 5)
- `DIGEST_ACTIONABLE_ONLY` (default true)
- `THEME_GROUPING_ENABLED` (default true)
- `MAX_THEMES_PER_DIGEST` (default 5)
- `MAX_RELATED_MARKETS_PER_THEME` (default 3)
- `MAX_AI_RECS_PER_DAY` (default 5)
- `MAX_AI_RECS_PER_DIGEST` (default 2)
- `AI_RECOMMENDATION_EXPIRES_MINUTES` (default 30)
- `FAST_SIGNALS_ENABLED` (default false)
- `FAST_WINDOW_MINUTES` (default 15)
- `FAST_MIN_LIQUIDITY` (default 20000)
- `FAST_MIN_VOLUME_24H` (default 20000)
- `FAST_MIN_ABS_MOVE` (default 0.015)
- `FAST_MIN_PCT_MOVE` (default 0.05)
- `FAST_PYES_MIN` (default 0.15)
- `FAST_PYES_MAX` (default 0.85)
- `FAST_COOLDOWN_MINUTES` (default 10)
- `FAST_MAX_THEMES_PER_DIGEST` (default 2)
- `FAST_MAX_MARKETS_PER_THEME` (default 2)
- `FAST_DIGEST_MODE` (default "separate")
- `DEFAULT_TENANT_ID` (default "default")
- `RATE_LIMIT_DEFAULT_PER_MIN` (default 60)
- `LOG_LEVEL` (default INFO)
- `LOG_JSON` (default true)

Gamma ingestion can apply server-side filters for liquidity/volume (with local safeguards still applied).
When `POLY_USE_GLOBAL_MINIMUMS` is true, `POLY_LIQUIDITY_MIN`/`POLY_VOLUME_MIN` default to
`GLOBAL_MIN_LIQUIDITY`/`GLOBAL_MIN_VOLUME_24H` if unset. Server-side filters reduce payload
but local safeguards still apply.
Pagination continues until Gamma returns an empty page or a short page (< `POLY_PAGE_LIMIT`).
`POLY_MAX_EVENTS` and `POLY_MAX_PAGES` are safety guards to prevent runaway pagination and do not
change the default behavior of ingesting all available records. When `POLY_ORDER` or
`POLY_ASCENDING` are set, they are passed through to Gamma; otherwise, the API-defined order applies.

## Migrations

```bash
docker compose exec api alembic upgrade head
```

## API key management

Create a new key:

```bash
docker compose exec api python -m app.scripts.create_api_key --name prod
```

The command prints the raw key once. Store it securely and pass it in the `X-API-Key` header.
Keys are stored hashed in the database.

## Telegram alerts

Set `TELEGRAM_BOT_TOKEN` in `.env` and restart the services.
Each user has their own `telegram_chat_id` and preferences that override global defaults.
FAST watchlist signals are optional and gated by both `FAST_SIGNALS_ENABLED` and the
per-user `fast_signals_enabled` preference.

## AI Copilot (manual execution)

When `ai_copilot_enabled` is true for a user, PMD generates read-only recommendations
for actionable alerts and sends a Telegram message with a draft order block and
Confirm/Skip/Mute buttons. Confirming does not place any order; PMD only returns a
formatted payload for manual execution outside PMD.

Required user preference fields:
- `ai_copilot_enabled` (bool)
- `risk_budget_usd_per_day` (float)
- `max_usd_per_trade` (float)
- `max_liquidity_fraction` (float)

Safety constraints:
- PMD never submits orders.
- No private keys or trading endpoints are used.
- Draft sizing is deterministic and conservative.

## User management

Add a user:

```bash
docker compose exec api python -m app.scripts.manage_users add --name "Alice" --chat-id -12345
```

Disable a user:

```bash
docker compose exec api python -m app.scripts.manage_users disable --user Alice
```

Update preferences:

```bash
docker compose exec api python -m app.scripts.manage_users set-pref --user Alice --min-liquidity 50000
```

Enable FAST watchlist signals for a user:

```bash
docker compose exec api python -m app.scripts.manage_users set-pref --user Alice --fast-signals-enabled true
```

Send a test Telegram message:

```bash
docker compose exec api python -m app.scripts.manage_users test --user Alice
```

## Scheduler

The `scheduler` service enqueues ingestion jobs every `INGEST_INTERVAL_SECONDS`.
Cleanup runs once per day after `CLEANUP_SCHEDULE_HOUR_UTC` (UTC) when enabled, and
deletes rows older than the configured retention windows.
You can also trigger a manual ingest by calling `POST /jobs/ingest`.

## Smoke test

```bash
./scripts/smoke_test.sh <api_key>
```

## Disclaimer

Read-only analytics. Not financial advice. No guarantee of outcomes. No custody. No execution.

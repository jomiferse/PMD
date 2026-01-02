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
- `GET /admin/stats` (admin)

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
- `POLY_MAX_EVENTS` (default 1000)
- `POLY_START_OFFSET` (default 0)
- `POLY_ORDER` (default unset)
- `POLY_ASCENDING` (default unset)
- `INGEST_INTERVAL_SECONDS` (default 300)
- `MIN_LIQUIDITY` (default 1000)
- `MIN_VOLUME_24H` (default 1000)
- `WINDOW_MINUTES` (default 60)
- `ALERT_COOLDOWN_MINUTES` (default 30)
- `TELEGRAM_BOT_TOKEN`
- `ADMIN_API_KEY`
- `GLOBAL_MIN_LIQUIDITY` (default 1000)
- `GLOBAL_MIN_VOLUME_24H` (default 1000)
- `GLOBAL_DIGEST_WINDOW` (default 60)
- `GLOBAL_MAX_ALERTS` (default 7)
- `DEFAULT_TENANT_ID` (default "default")
- `RATE_LIMIT_DEFAULT_PER_MIN` (default 60)
- `LOG_LEVEL` (default INFO)
- `LOG_JSON` (default true)

Gamma ingestion uses `GLOBAL_MIN_LIQUIDITY` and `GLOBAL_MIN_VOLUME_24H` as server-side filters
when they are greater than zero (with local validation still applied).

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

Send a test Telegram message:

```bash
docker compose exec api python -m app.scripts.manage_users test --user Alice
```

## Scheduler

The `scheduler` service enqueues ingestion jobs every `INGEST_INTERVAL_SECONDS`.
You can also trigger a manual ingest by calling `POST /jobs/ingest`.

## Smoke test

```bash
./scripts/smoke_test.sh <api_key>
```

## Disclaimer

Read-only analytics. Not financial advice. No guarantee of outcomes. No custody. No execution.

# PMD - Polymarket Mispricing Detector (MVP v1)

Read-only analytics. Not financial advice. No guarantee of outcomes. No custody. No execution.

PMD ingests Polymarket Gamma data, stores 5-minute snapshots, and emits dislocation alerts
based on percentage price movement over a time window. It does not execute trades.

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

## Alert logic (dislocation)

An alert triggers when:
- Price moves by at least `MOVE_THRESHOLD` within `WINDOW_MINUTES`
- Liquidity is at least `MIN_LIQUIDITY`
- Volume24h is at least `MIN_VOLUME_24H`
- The same market has not alerted within `ALERT_COOLDOWN_MINUTES`

Alerts store old/new price, delta percent, and the trigger timestamp.

## Environment variables

Required:
- `DATABASE_URL`
- `REDIS_URL`
- `POLYMARKET_BASE_URL`

Optional:
- `POLY_LIMIT` (default 100)
- `INGEST_INTERVAL_SECONDS` (default 300)
- `EDGE_THRESHOLD` (default 0.08)
- `MIN_LIQUIDITY` (default 1000)
- `MIN_VOLUME_24H` (default 1000)
- `MOVE_THRESHOLD` (default 0.05)
- `WINDOW_MINUTES` (default 60)
- `ALERT_COOLDOWN_MINUTES` (default 30)
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `TELEGRAM_THROTTLE_SECONDS` (default 900)
- `DEFAULT_TENANT_ID` (default "default")
- `RATE_LIMIT_DEFAULT_PER_MIN` (default 60)
- `LOG_LEVEL` (default INFO)
- `LOG_JSON` (default true)

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

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `.env` and restart the services.
Alerts are throttled per market using `TELEGRAM_THROTTLE_SECONDS` to prevent spam.

## Scheduler

The `scheduler` service enqueues ingestion jobs every `INGEST_INTERVAL_SECONDS`.
You can also trigger a manual ingest by calling `POST /jobs/ingest`.

## Smoke test

```bash
./scripts/smoke_test.sh <api_key>
```

## Disclaimer

Read-only analytics. Not financial advice. No guarantee of outcomes. No custody. No execution.

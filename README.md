# PMD - Polymarket Mispricing Detector

PMD is a read-only analytics service that detects Polymarket mispricings and delivers Telegram digests.

## Architecture

- Polymarket Gamma ingestion -> 5-minute market snapshots
- Dislocation + FAST signals -> alerts
- Effective settings per user (code defaults -> plan -> user overrides)
- Telegram digests + AI Copilot (manual execution only)

## Quick start (fresh DB)

1) Copy the env file and fill required values:

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

4) Seed pricing plans (assigns all users to Basic):

```bash
docker compose exec api python -m app.scripts.seed_plans
```

5) Create an API key (prints the raw key once):

```bash
docker compose exec api python -m app.scripts.create_api_key --name prod
```

6) Call endpoints:

```bash
curl -H "X-API-Key: <key>" http://localhost:8000/alerts/latest
```

## Database reset / Alembic

The database is disposable. If you need a clean reset, drop the database and run:

```bash
docker compose exec api alembic upgrade head
```

## Configuration model

- Infra + secrets live in `.env` (DATABASE_URL, REDIS_URL, POLYMARKET_BASE_URL, TELEGRAM_BOT_TOKEN, LLM keys).
- User limits and pricing tiers are managed in the database (plans + overrides).
- Effective settings resolve in this order: code defaults -> plan -> user overrides.

## Plans

Basic (default):
- Copilot disabled, no draft orders
- 60m digest window, max 3 themes, max 3 alerts
- STRONG alerts only, higher liquidity/volume filters
- FAST disabled

Pro (recommended):
- Copilot enabled (3/day, 1 per digest), 6h theme TTL
- 30m digest window, max 5 themes
- STRONG + MEDIUM alerts
- FAST enabled (watchlist)

Elite:
- Copilot enabled (10/day, 2 per digest), 2h theme TTL
- 15m digest window, max 10 themes
- STRONG + MEDIUM alerts, lower liquidity/volume thresholds
- FAST enabled (watchlist)

Monetized limits: Copilot caps, digest cadence, theme/alert caps, FAST access,
and risk sizing defaults (budget + per-trade caps). Plan caps can be overridden per user.

## Environment variables

Required:
- `DATABASE_URL`
- `REDIS_URL`
- `POLYMARKET_BASE_URL`

Optional:
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
- `FAST_SIGNALS_GLOBAL_ENABLED` (default false)
- `TELEGRAM_BOT_TOKEN`
- `ADMIN_API_KEY`
- `OPENAI_API_KEY`
- `LLM_API_BASE` (default `https://api.openai.com/v1/chat/completions`)
- `LLM_MODEL` (default `gpt-4o-mini`)
- `LLM_TIMEOUT_SECONDS` (default 15)
- `LLM_MAX_RETRIES` (default 2)
- `LLM_CACHE_TTL_SECONDS` (default 3600)
- `DEFAULT_TENANT_ID` (default "default")
- `RATE_LIMIT_DEFAULT_PER_MIN` (default 60)
- `LOG_LEVEL` (default INFO)
- `LOG_JSON` (default true)

Gamma ingestion continues until Gamma returns an empty page or a short page (< `POLY_PAGE_LIMIT`).
`POLY_MAX_EVENTS` and `POLY_MAX_PAGES` are safety guards and log warnings if hit.
When `POLY_USE_GLOBAL_MINIMUMS` is true, `POLY_LIQUIDITY_MIN`/`POLY_VOLUME_MIN` default to code
minimums if unset; server-side filters reduce payloads but local safeguards still apply.

## Admin operations

Seed plans:

```bash
docker compose exec api python -m app.scripts.seed_plans
```

Create or update a plan:

```bash
docker compose exec api python -m app.scripts.create_plan --name pro --max-copilot-per-day 3
```

Assign a plan:

```bash
docker compose exec api python -m app.scripts.assign_plan --user Alice --plan-name pro
```

Add a user:

```bash
docker compose exec api python -m app.scripts.manage_users add --name "Alice" --chat-id -12345
```

Update preferences:

```bash
docker compose exec api python -m app.scripts.manage_users set-pref --user Alice --min-liquidity 50000
```

Send a test Telegram message:

```bash
docker compose exec api python -m app.scripts.manage_users test --user Alice
```

## AI Copilot (manual execution)

Copilot is read-only. It generates a draft order payload for manual execution and never
submits orders. Copilot must be enabled on the plan and per user via `copilot_enabled`.
Risk sizing limits live in plans or per-user overrides.

## FAST vs CONFIRMED signals

FAST signals are watchlist-only dislocations delivered in a separate FAST digest. They
never trigger Copilot. CONFIRMED signals are the regular dislocation alerts used for
theme grouping, digests, and Copilot eligibility.

## Troubleshooting

Why Copilot did not trigger (reason codes in `admin/users/{id}/copilot-last-status`):
Each digest run emits a `copilot_run_summary` log with per-user counts and skip reasons.
- `USER_DISABLED`: user toggle off
- `PLAN_DISABLED`: plan disables Copilot
- `CAP_REACHED`: daily or per-digest cap reached
- `COPILOT_DEDUPE_ACTIVE`: theme recently sent
- `MUTED`: market/theme muted
- `LABEL_MAPPING_UNKNOWN`: outcome label mapping not verified
- `NOT_REPRICING`: alert not classified as repricing
- `CONFIDENCE_NOT_HIGH`: confidence below HIGH
- `NOT_FOLLOW`: suggested action not FOLLOW
- `P_OUT_OF_BAND`: probability outside user band
- `INSUFFICIENT_SNAPSHOTS`: not enough snapshots for evidence
- `MISSING_PRICE_OR_LIQUIDITY`: missing price or liquidity inputs
- `MISSING_CHAT_ID`: user missing Telegram chat id
- `NO_ACTIONABLE_THEMES`: no actionable themes in the digest window
- `NO_ALERTS`: no alerts in the digest window
- `DIGEST_RECENTLY_SENT`: digest cooldown blocked Copilot

`p_outcome0` means the outcome label mapping was not verified or was unknown.
`CAP_REACHED` means the daily or per-digest Copilot cap was exhausted and is an upgrade moment.

Copilot debug harness (dry run):

```bash
docker compose exec api python -m app.scripts.copilot_debug --user-id <uuid> --alert-id <alert_id>
```

## Safety

PMD never executes trades. Copilot confirmations are read-only and manual only.
No custody. No private keys. No execution.

## Endpoints

- `GET /health` (no auth)
- `POST /jobs/ingest` (auth)
- `GET /snapshots/latest` (auth)
- `GET /alerts/latest` (auth)
- `GET /alerts/summary` (auth)
- `GET /status` (auth)
- `GET /admin/users` (admin)
- `GET /admin/users/{id}/last-digest` (admin)
- `GET /admin/users/{id}/copilot-last-status` (admin)
- `GET /admin/plans` (admin)
- `POST /admin/plans` (admin)
- `PATCH /admin/users/{id}/plan` (admin)
- `GET /admin/users/{id}/effective-settings` (admin)
- `GET /admin/ai-recommendations` (admin)
- `GET /admin/stats` (admin)
- `POST /telegram/webhook` (no auth; Telegram callback)

# Performance & Caching Inventory

This table captures current API endpoints, cacheability, recommended TTLs, and rate limits. Cache keys must include user identity for user-scoped data and tenant identity for API-key requests.

| Endpoint | Category | Cacheable | TTL | Rate limit |
| --- | --- | --- | --- | --- |
| GET /health | Public/marketing | No | n/a | IP: 120/min |
| GET /status | Public/marketing | Yes (tenant scoped) | 15s | API key: 60/min, IP: 120/min |
| POST /auth/register | Auth/session | No | n/a | IP: 20/min |
| POST /auth/login | Auth/session | No | n/a | IP: 20/min |
| POST /auth/logout | Auth/session | No | n/a | User: 60/min, IP: 120/min |
| GET /me | Auth/session | Yes (user scoped) | 20s | User: 60/min, API key: 60/min, IP: 120/min |
| GET /alerts/latest | Alerts reads | Yes (tenant + user scoped) | 20s | User/API key: 60/min, IP: 120/min |
| GET /alerts/{id}/history | Alerts reads | Yes (tenant scoped) | 300s | User/API key: 60/min, IP: 120/min |
| GET /alerts/summary | Alerts reads | Yes (tenant scoped) | 30s | API key: 60/min, IP: 120/min |
| GET /alerts/last-digest | Alerts reads | Yes (tenant scoped) | 30s | API key: 60/min, IP: 120/min |
| GET /copilot/recommendations | Copilot reads | Yes (user scoped) | 30s | User: 30/min, IP: 120/min |
| GET /copilot/runs | Copilot reads | Yes (user scoped) | 15s | User: 30/min, IP: 120/min |
| GET /settings/me | Settings reads | Yes (user scoped) | 60s | User: 60/min, IP: 120/min |
| PATCH /settings/me | Settings writes | No | n/a | User: 20/min, IP: 120/min |
| GET /entitlements/me | Settings reads | Yes (user scoped) | 60s | User: 60/min, IP: 120/min |
| POST /billing/checkout-session | Billing writes | No | n/a | User: 20/min, IP: 120/min |
| POST /billing/portal-session | Billing writes | No | n/a | User: 20/min, IP: 120/min |
| GET /snapshots/latest | Alerts reads | Yes (tenant scoped) | 15s | API key: 60/min, IP: 120/min |
| POST /jobs/ingest | Jobs | No | n/a | API key: 20/min, IP: 120/min |
| POST /webhooks/stripe | Webhooks | No (no cache) | n/a | IP: 120/min |
| POST /telegram/webhook | Webhooks | No (no cache) | n/a | IP: 120/min |
| GET /admin/* | Admin | No | n/a | IP: 120/min |

## Cache Behavior
- Redis-backed JSON cache with ETag + `Cache-Control` headers for safe GET endpoints.
- Cache keys include endpoint path + query params, plus user ID or tenant ID; plan ID included when plan affects payload.
- Stale-if-error: if Redis has a stale entry and refresh fails, serve stale and return 200 with `max-age=0`.

## Invalidation Triggers
- Settings update: invalidate `/settings/me`, `/entitlements/me`, `/me` for the user.
- Stripe webhook updates: invalidate user-scoped `/me` + `/entitlements` cache.
- Ingest job completes: invalidate alerts feeds, summary, last-digest, snapshots, and status cache prefixes.

## Rate Limiting
- Fixed-window Redis counters with per-route rules (alerts, copilot, session reads, writes, auth) plus hard IP cap.
- API key requests are rate limited by the middleware and by per-key limits.
- 429 responses include `Retry-After` with default detail string.

## External Call Safeguards
- Polymarket, Telegram, and LLM calls use timeouts, bounded retries with exponential backoff, circuit breakers, and per-worker concurrency caps.
- Circuit breakers fail fast after repeated failures and reset after the configured window.

## DB Load Shedding
- Postgres statement timeout enforced via connection options.
- Statement timeouts return 503 with `detail=db_timeout`.

## Tuning (env)
- Cache: `CACHE_*` values + `CACHE_STALE_GRACE_SECONDS`
- Rate limiting: `RATE_LIMIT_*`
- External calls: `POLY_*`, `LLM_*`, `TELEGRAM_*`, `EXTERNAL_MAX_CONCURRENT_*`
- DB timeout: `DB_STATEMENT_TIMEOUT_SECONDS`

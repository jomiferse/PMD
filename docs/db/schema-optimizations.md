# Schema Optimizations (Postgres)

## Overview
This set of migrations tightens constraints, standardizes timestamps to TIMESTAMPTZ, aligns indexes with hot query paths, and adds retention-ready `expires_at` columns for high-volume tables.

## Migrations
- `alembic/versions/20260120_constraints_typefixes.py`: Convert timestamps to TIMESTAMPTZ, add server defaults, and add CHECK constraints for ranges/enums.
- `alembic/versions/20260120_hot_query_indexes.py`: Replace redundant indexes with composite/ordered indexes aligned to hot queries.
- `alembic/versions/20260120_retention_expires.py`: Add `expires_at` columns + indexes and backfill with default retention windows.

## Type + Constraint Changes
- **TIMESTAMPTZ**: All core timestamp columns now store timezone-aware UTC values.
- **Server defaults**: `created_at`/`triggered_at`/`delivered_at`/`asof_ts` default to `now()` to reduce insert-time dependency on app-side defaults.
- **CHECK constraints**:
  - `market_snapshots`: probabilities in [0,1], non-negative liquidity/volume, and bounded `market_kind`/`mapping_confidence`.
  - `alerts`: probability bounds, non-negative liquidity/volume/best_ask, bounded strength/market_kind/mapping_confidence.
  - `plans` + `user_alert_preferences`: non-negative limits, valid probability bands, and `p_min < p_max` when both present.
  - `alert_deliveries`: delivery status restricted to `sent/skipped/filtered`.
  - `ai_recommendations`: recommendation/confidence/status restricted to known values.
  - `user_sessions`: `expires_at > created_at` to prevent stale sessions.

## Index Changes (Hot Paths)
- **Alerts list/pagination**: `alerts(tenant_id, created_at DESC, id DESC)`
- **Alerts with strength filter**: `alerts(tenant_id, strength, created_at DESC, id DESC)`
- **Alerts with category filter (case-insensitive)**: `alerts(tenant_id, lower(category), created_at DESC, id DESC)`
- **Copilot recommendations list**: `ai_recommendations(user_id, created_at DESC, id DESC)`
- **Subscription lookup**: `subscriptions(user_id, created_at DESC)`
- **Retention cleanup**: `expires_at` indexes on `market_snapshots`, `alerts`, and `alert_deliveries`

Redundant indexes removed:
- `market_snapshots` single-column `market_id` and duplicate `(market_id, snapshot_bucket)` index (unique constraint already covers it).
- `alerts` basic tenant-only indexes replaced by ordered composites.
- `ai_recommendations` and `subscriptions` single-column indexes replaced by ordered composites.

## Expected Query Wins
- **Alerts feed**: ordered composite index supports time-window scans and keyset pagination.
- **Alerts + strength/category filters**: reduced sort cost when applying filters in the feed.
- **Alert history**: existing `(market_id, snapshot_bucket)` unique index continues to power range scans.
- **Copilot recommendations**: ordered composite index aligns with cursor pagination by `created_at` + `id`.
- **Subscription lookup**: ordered composite index speeds latest-subscription lookups.

## Retention / Cleanup Notes
- High-volume tables now include `expires_at` columns with indexes.
- Backfill uses default retention values (snapshots=7d, alerts=30d, deliveries=30d). If your environment uses different retention windows, run a one-time backfill:
  - `UPDATE market_snapshots SET expires_at = asof_ts + (:days * INTERVAL '1 day') WHERE expires_at IS NULL;`
  - `UPDATE alerts SET expires_at = COALESCE(triggered_at, created_at) + (:days * INTERVAL '1 day') WHERE expires_at IS NULL;`
  - `UPDATE alert_deliveries SET expires_at = delivered_at + (:days * INTERVAL '1 day') WHERE expires_at IS NULL;`
- The cleanup job now prefers `expires_at` when present.

## Verification
Use `scripts/verify_db_schema.py` to run migrations and validate constraints/indexes.

Example (fresh temp DB):
```bash
python scripts/verify_db_schema.py --database-url postgresql://user:pass@host:5432/postgres --create-db
```

Example with EXPLAIN output:
```bash
python scripts/verify_db_schema.py --database-url postgresql://user:pass@host:5432/postgres --create-db --explain
```

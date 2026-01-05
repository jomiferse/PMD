import json
import logging
import os
from datetime import datetime, timezone, timedelta

import redis
from sqlalchemy import inspect, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ..polymarket.client import PolymarketClient
from ..core import defaults
from ..core.scoring import score_market
from ..core.dislocation import compute_dislocation_alerts
from ..core.fast_signals import compute_fast_signals
from ..core.alerts import send_user_digests
from ..settings import settings
from ..models import MarketSnapshot, Alert

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)
INGEST_LOCK_KEY = "lock:ingest"

async def run_ingest_and_alert(db: Session) -> dict:
    started_at = datetime.now(timezone.utc)
    result: dict = {"ok": False, "snapshots": 0, "alerts": 0}
    lock_value = f"{os.getpid()}:{started_at.isoformat()}"
    lock_ttl = max(settings.INGEST_INTERVAL_SECONDS * 2, 30)
    try:
        locked = redis_conn.set(INGEST_LOCK_KEY, lock_value, nx=True, ex=lock_ttl)
    except Exception:
        logger.exception("ingest_lock_failed")
        locked = True
    if not locked:
        logger.info("ingest_skipped reason=lock_held")
        result["reason"] = "ingest_locked"
        return result

    try:
        client = PolymarketClient()
        markets = await client.fetch_markets_paginated()

        snapshot_rows_map: dict[tuple[str, datetime], dict] = {}
        for m in markets:
            s = score_market(m.market_id, m.title, m.category or "unknown", m.p_primary, m.liquidity)

            market_id = _truncate_str(s.market_id, 128)
            title = _truncate_str(s.title, 512)
            category = _truncate_str(s.category, 128)

            source_ts = m.source_ts or started_at
            bucket = _snapshot_bucket(source_ts)

            key = (market_id, bucket)
            row = {
                "market_id": market_id,
                "title": title,
                "category": category,
                "market_p_yes": s.market_p_yes,
                "primary_outcome_label": m.primary_outcome_label,
                "is_yesno": m.is_yesno,
                "mapping_confidence": m.mapping_confidence,
                "market_kind": m.market_kind,
                "liquidity": s.liquidity,
                "volume_24h": m.volume_24h,
                "volume_1w": m.volume_1w,
                "best_ask": m.best_ask,
                "last_trade_price": m.last_trade_price,
                "model_p_yes": s.model_p_yes,
                "edge": s.edge,
                "source_ts": source_ts,
                "snapshot_bucket": bucket,
                "asof_ts": started_at,
            }
            existing = snapshot_rows_map.get(key)
            if existing is None or source_ts >= existing.get("source_ts", started_at):
                snapshot_rows_map[key] = row

        snapshot_rows = list(snapshot_rows_map.values())
        snapshot_columns = _table_columns(db, "market_snapshots")
        conflict_cols = ["market_id", "snapshot_bucket"]

        if snapshot_rows and snapshot_columns:
            for row in snapshot_rows:
                for key in list(row.keys()):
                    if key not in snapshot_columns:
                        row.pop(key, None)

            if "snapshot_bucket" not in snapshot_columns:
                conflict_cols = ["market_id", "asof_ts"]

        if snapshot_rows:
            stmt = pg_insert(MarketSnapshot).values(snapshot_rows)
            row_keys = set(snapshot_rows[0].keys())
            update_cols = [col for col in row_keys if col not in conflict_cols]
            update_set = {col: getattr(stmt.excluded, col) for col in update_cols}

            if update_set:
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_cols,
                    set_=update_set,
                )
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)

            db.execute(stmt)
            db.commit()

        alert_columns = _table_columns(db, "alerts")
        use_triggered_at = "triggered_at" in alert_columns
        alerts = compute_dislocation_alerts(
            db=db,
            snapshots=snapshot_rows,
            window_minutes=defaults.WINDOW_MINUTES,
            medium_move_threshold=defaults.MEDIUM_MOVE_THRESHOLD,
            min_price_threshold=defaults.MIN_PRICE_THRESHOLD,
            medium_abs_move_threshold=defaults.MEDIUM_ABS_MOVE_THRESHOLD,
            floor_price=defaults.FLOOR_PRICE,
            medium_min_liquidity=defaults.MEDIUM_MIN_LIQUIDITY,
            medium_min_volume_24h=defaults.MEDIUM_MIN_VOLUME_24H,
            strong_abs_move_threshold=defaults.STRONG_ABS_MOVE_THRESHOLD,
            strong_min_liquidity=defaults.STRONG_MIN_LIQUIDITY,
            strong_min_volume_24h=defaults.STRONG_MIN_VOLUME_24H,
            cooldown_minutes=defaults.ALERT_COOLDOWN_MINUTES,
            tenant_id=settings.DEFAULT_TENANT_ID,
            use_triggered_at=use_triggered_at,
        )

        if alerts:
            alert_rows = [a.__dict__ for a in alerts]
            for row in alert_rows:
                row.pop("_sa_instance_state", None)
                row.pop("id", None)
                if alert_columns:
                    for key in list(row.keys()):
                        if key not in alert_columns:
                            row.pop(key, None)
                else:
                    for key in OPTIONAL_ALERT_COLUMNS:
                        row.pop(key, None)
            alert_stmt = pg_insert(Alert).values(alert_rows)
            alert_stmt = alert_stmt.on_conflict_do_nothing(
                index_elements=["alert_type", "market_id", "snapshot_bucket"]
            )
            db.execute(alert_stmt)
            db.commit()

        fast_alerts: list[Alert] = []
        if settings.FAST_SIGNALS_GLOBAL_ENABLED:
            fast_alerts = compute_fast_signals(
                db=db,
                snapshots=snapshot_rows,
                window_minutes=defaults.DEFAULT_FAST_WINDOW_MINUTES,
                min_liquidity=defaults.FAST_MIN_LIQUIDITY,
                min_volume_24h=defaults.FAST_MIN_VOLUME_24H,
                min_abs_move=defaults.FAST_MIN_ABS_MOVE,
                min_pct_move=defaults.FAST_MIN_PCT_MOVE,
                p_yes_min=defaults.FAST_PYES_MIN,
                p_yes_max=defaults.FAST_PYES_MAX,
                cooldown_minutes=defaults.FAST_COOLDOWN_MINUTES,
                tenant_id=settings.DEFAULT_TENANT_ID,
                use_triggered_at=use_triggered_at,
            )
        if fast_alerts:
            fast_rows = [a.__dict__ for a in fast_alerts]
            for row in fast_rows:
                row.pop("_sa_instance_state", None)
                row.pop("id", None)
                if alert_columns:
                    for key in list(row.keys()):
                        if key not in alert_columns:
                            row.pop(key, None)
                else:
                    for key in OPTIONAL_ALERT_COLUMNS:
                        row.pop(key, None)
            fast_stmt = pg_insert(Alert).values(fast_rows)
            fast_stmt = fast_stmt.on_conflict_do_nothing(
                index_elements=["alert_type", "market_id", "snapshot_bucket"]
            )
            db.execute(fast_stmt)
            db.commit()

        await send_user_digests(db, settings.DEFAULT_TENANT_ID)

        result = {
            "ok": True,
            "snapshots": len(snapshot_rows),
            "alerts": len(alerts),
            "fast_alerts": len(fast_alerts),
        }
        return result
    except Exception:
        logger.exception("ingest_failed")
        result["error"] = "ingest_failed"
        raise
    finally:
        result["ts"] = datetime.now(timezone.utc).isoformat()
        try:
            current = redis_conn.get(INGEST_LOCK_KEY)
            if current and current.decode() == lock_value:
                redis_conn.delete(INGEST_LOCK_KEY)
        except Exception:
            logger.exception("ingest_lock_release_failed")
        try:
            redis_conn.set("ingest:last_ts", result["ts"])
            redis_conn.set("ingest:last_result", json.dumps(result, ensure_ascii=True))
        except Exception:
            logger.exception("ingest_status_update_failed")


def run_cleanup(db: Session) -> dict:
    if not settings.CLEANUP_ENABLED:
        logger.info("cleanup_skipped disabled=true")
        return {"ok": False, "disabled": True}

    now_ts = datetime.now(timezone.utc)
    snapshots_cutoff = now_ts - timedelta(days=settings.SNAPSHOT_RETENTION_DAYS)
    alerts_cutoff = now_ts - timedelta(days=settings.ALERT_RETENTION_DAYS)
    deliveries_cutoff = now_ts - timedelta(days=settings.DELIVERY_RETENTION_DAYS)

    snapshots_column = _pick_column(db, "market_snapshots", ["asof_ts", "source_ts"])
    alerts_column = _pick_column(db, "alerts", ["triggered_at", "created_at"])
    deliveries_column = _pick_column(db, "alert_deliveries", ["delivered_at", "created_at"])

    deleted_snapshots = 0
    deleted_alerts = 0
    deleted_deliveries = 0

    if snapshots_column:
        deleted_snapshots = _delete_older_than(
            db, "market_snapshots", snapshots_column, snapshots_cutoff
        )
        logger.info(
            "cleanup_deleted table=market_snapshots column=%s cutoff=%s count=%s",
            snapshots_column,
            snapshots_cutoff.isoformat(),
            deleted_snapshots,
        )
    else:
        logger.warning("cleanup_skipped table=market_snapshots reason=missing_column")

    if alerts_column:
        deleted_alerts = _delete_older_than(db, "alerts", alerts_column, alerts_cutoff)
        logger.info(
            "cleanup_deleted table=alerts column=%s cutoff=%s count=%s",
            alerts_column,
            alerts_cutoff.isoformat(),
            deleted_alerts,
        )
    else:
        logger.warning("cleanup_skipped table=alerts reason=missing_column")

    if deliveries_column:
        deleted_deliveries = _delete_older_than(
            db, "alert_deliveries", deliveries_column, deliveries_cutoff
        )
        logger.info(
            "cleanup_deleted table=alert_deliveries column=%s cutoff=%s count=%s",
            deliveries_column,
            deliveries_cutoff.isoformat(),
            deleted_deliveries,
        )
    else:
        logger.warning("cleanup_skipped table=alert_deliveries reason=missing_column")

    db.commit()
    return {
        "ok": True,
        "snapshots_deleted": deleted_snapshots,
        "alerts_deleted": deleted_alerts,
        "deliveries_deleted": deleted_deliveries,
        "ts": now_ts.isoformat(),
    }


def _snapshot_bucket(ts: datetime) -> datetime:
    minute = (ts.minute // 5) * 5
    return ts.replace(minute=minute, second=0, microsecond=0)


def _truncate_str(value: str | None, max_len: int) -> str:
    if not value:
        return ""
    if len(value) <= max_len:
        return value
    return value[:max_len]


def _table_columns(db: Session, table_name: str) -> set[str]:
    try:
        insp = inspect(db.get_bind())
        return {col["name"] for col in insp.get_columns(table_name)}
    except Exception:
        logger.exception("inspect_table_columns_failed table=%s", table_name)
        return set()


def _pick_column(db: Session, table_name: str, candidates: list[str]) -> str | None:
    columns = _table_columns(db, table_name)
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _delete_older_than(db: Session, table_name: str, column_name: str, cutoff: datetime) -> int:
    stmt = text(f"DELETE FROM {table_name} WHERE {column_name} < :cutoff")
    result = db.execute(stmt, {"cutoff": cutoff})
    return int(result.rowcount or 0)


OPTIONAL_ALERT_COLUMNS = {
    "old_price",
    "new_price",
    "delta_pct",
    "triggered_at",
    "strength",
    "mapping_confidence",
    "market_kind",
}

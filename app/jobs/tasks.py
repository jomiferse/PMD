import json
import logging
from datetime import datetime, timezone

import redis
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ..polymarket.client import PolymarketClient
from ..core.scoring import score_market
from ..core.dislocation import compute_dislocation_alerts
from ..core.alerts import send_telegram_alerts
from ..settings import settings
from ..models import MarketSnapshot, Alert

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)

async def run_ingest_and_alert(db: Session) -> dict:
    started_at = datetime.now(timezone.utc)
    result: dict = {"ok": False, "snapshots": 0, "alerts": 0}

    try:
        client = PolymarketClient()
        markets = await client.fetch_markets()

        snapshot_rows: list[dict] = []
        for m in markets:
            s = score_market(m.market_id, m.title, m.category or "unknown", m.p_yes, m.liquidity)

            source_ts = m.source_ts or started_at
            bucket = _snapshot_bucket(source_ts)

            snapshot_rows.append(
                {
                    "market_id": s.market_id,
                    "title": s.title,
                    "category": s.category,
                    "market_p_yes": s.market_p_yes,
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
            )

        if snapshot_rows:
            stmt = pg_insert(MarketSnapshot).values(snapshot_rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["market_id", "snapshot_bucket"],
                set_={
                    "title": stmt.excluded.title,
                    "category": stmt.excluded.category,
                    "market_p_yes": stmt.excluded.market_p_yes,
                    "liquidity": stmt.excluded.liquidity,
                    "volume_24h": stmt.excluded.volume_24h,
                    "volume_1w": stmt.excluded.volume_1w,
                    "best_ask": stmt.excluded.best_ask,
                    "last_trade_price": stmt.excluded.last_trade_price,
                    "model_p_yes": stmt.excluded.model_p_yes,
                    "edge": stmt.excluded.edge,
                    "source_ts": stmt.excluded.source_ts,
                    "asof_ts": stmt.excluded.asof_ts,
                },
            )
            db.execute(stmt)
            db.commit()

        alert_columns = _table_columns(db, "alerts")
        use_triggered_at = "triggered_at" in alert_columns
        alerts = compute_dislocation_alerts(
            db=db,
            snapshots=snapshot_rows,
            window_minutes=settings.WINDOW_MINUTES,
            move_threshold=settings.MOVE_THRESHOLD,
            min_liquidity=settings.MIN_LIQUIDITY,
            min_volume_24h=settings.MIN_VOLUME_24H,
            cooldown_minutes=settings.ALERT_COOLDOWN_MINUTES,
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

        await send_telegram_alerts(alerts)

        result = {
            "ok": True,
            "snapshots": len(snapshot_rows),
            "alerts": len(alerts),
        }
        return result
    except Exception:
        logger.exception("ingest_failed")
        result["error"] = "ingest_failed"
        raise
    finally:
        result["ts"] = datetime.now(timezone.utc).isoformat()
        try:
            redis_conn.set("ingest:last_ts", result["ts"])
            redis_conn.set("ingest:last_result", json.dumps(result, ensure_ascii=True))
        except Exception:
            logger.exception("ingest_status_update_failed")


def _snapshot_bucket(ts: datetime) -> datetime:
    minute = (ts.minute // 5) * 5
    return ts.replace(minute=minute, second=0, microsecond=0)


def _table_columns(db: Session, table_name: str) -> set[str]:
    try:
        insp = inspect(db.get_bind())
        return {col["name"] for col in insp.get_columns(table_name)}
    except Exception:
        logger.exception("inspect_table_columns_failed table=%s", table_name)
        return set()


OPTIONAL_ALERT_COLUMNS = {
    "old_price",
    "new_price",
    "delta_pct",
    "triggered_at",
}

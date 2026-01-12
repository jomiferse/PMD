from datetime import datetime, timedelta, timezone
import json
import logging
import time
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from ...core.ai_copilot import COPILOT_RUN_KEY
from ...core.alert_classification import classify_alert_with_snapshots
from ...core.alerts import _alert_direction, _reversal_flag, _sustained_snapshot_count
from ...core.market_links import attach_market_slugs, market_url
from ...auth import api_key_auth
from ...db import get_db
from ...deps import _get_session_token, _get_session_user, _resolve_default_user
from ...integrations.redis_client import redis_conn
from ...models import AiRecommendation, Alert, AlertDelivery, MarketSnapshot, User
from ...rate_limit import rate_limit
from ...settings import settings

router = APIRouter()
logger = logging.getLogger("app.main")

LAST_DIGEST_KEY = "alerts:last_digest:{tenant_id}"


def _resolve_request_auth(request: Request, db: Session) -> tuple[str, uuid.UUID | None]:
    api_key_header = (request.headers.get("x-api-key") or "").strip()
    if api_key_header:
        api_key = api_key_auth(x_api_key=api_key_header, db=db)
        rate_limit(api_key)
        return api_key.tenant_id, None

    user = _get_session_user(db, _get_session_token(request))
    if not user:
        raise HTTPException(status_code=401, detail="not_authenticated")
    return settings.DEFAULT_TENANT_ID, user.user_id


@router.get("/alerts/latest")
def alerts_latest(
    request: Request,
    db: Session = Depends(get_db),
    limit: int = 50,
    window_minutes: int = 24 * 60,
    strength: str | None = None,
    category: str | None = None,
    copilot: str | None = None,
    user_id: str | None = None,
    cursor: str | None = None,
    paginate: str | None = None,
    include_total: str | None = None,
):
    tenant_id, session_user_id = _resolve_request_auth(request, db)
    now_ts = datetime.now(timezone.utc)
    window_start = now_ts - timedelta(minutes=max(window_minutes, 1))
    query = db.query(Alert).filter(
        Alert.tenant_id == tenant_id,
        Alert.created_at >= window_start,
    )
    if strength:
        query = query.filter(func.upper(Alert.strength) == strength.strip().upper())
    if category:
        query = query.filter(func.lower(Alert.category) == category.strip().lower())

    resolved_user_id: uuid.UUID | None = None
    resolved_user = None
    if session_user_id:
        resolved_user_id = session_user_id
    elif user_id:
        try:
            resolved_user_id = uuid.UUID(user_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid_user_id")
    if resolved_user_id is None:
        resolved_user = (
            db.query(User)
            .filter(User.is_active.is_(True))
            .order_by(User.created_at.desc())
            .first()
        )
        if resolved_user:
            resolved_user_id = resolved_user.user_id

    if resolved_user_id:
        query = query.outerjoin(
            AlertDelivery,
            (AlertDelivery.alert_id == Alert.id)
            & (AlertDelivery.user_id == resolved_user_id),
        )
        if copilot:
            copilot_filter = copilot.strip().lower()
            if copilot_filter == "sent":
                query = query.filter(AlertDelivery.delivery_status == "sent")
            elif copilot_filter == "skipped":
                query = query.filter(AlertDelivery.delivery_status.in_(["skipped", "filtered"]))

    if cursor:
        try:
            cursor_ts_raw, cursor_id_raw = cursor.split(":")
            cursor_ts = datetime.fromtimestamp(int(cursor_ts_raw), tz=timezone.utc)
            cursor_id = int(cursor_id_raw)
            query = query.filter(
                or_(
                    Alert.created_at < cursor_ts,
                    (Alert.created_at == cursor_ts) & (Alert.id < cursor_id),
                )
            )
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid_cursor")

    total_count = None
    if include_total:
        total_count = query.with_entities(func.count()).scalar()

    query = query.order_by(Alert.created_at.desc(), Alert.id.desc()).limit(limit)
    rows = query.all()

    alerts: list[Alert] = []
    deliveries_by_alert_id: dict[int, AlertDelivery] = {}
    if resolved_user_id:
        for row in rows:
            if isinstance(row, tuple):
                alert, delivery = row
                alerts.append(alert)
                if delivery:
                    deliveries_by_alert_id[alert.id] = delivery
            else:
                alerts.append(row)
    else:
        alerts = [row[0] if isinstance(row, tuple) else row for row in rows]

    market_ids = {str(alert.market_id) for alert in alerts if alert.market_id}
    points_by_market: dict[str, list[tuple[datetime, float]]] = {}
    if market_ids:
        snapshot_rows = (
            db.query(MarketSnapshot.market_id, MarketSnapshot.snapshot_bucket, MarketSnapshot.market_p_yes)
            .filter(
                MarketSnapshot.market_id.in_(market_ids),
                MarketSnapshot.snapshot_bucket >= window_start,
                MarketSnapshot.snapshot_bucket <= now_ts,
                MarketSnapshot.market_p_yes.isnot(None),
            )
            .order_by(MarketSnapshot.market_id.asc(), MarketSnapshot.snapshot_bucket.asc())
            .all()
        )
        for market_id, bucket, price in snapshot_rows:
            points_by_market.setdefault(str(market_id), []).append((bucket, price))

    attach_market_slugs(db, alerts)

    results = []
    for alert in alerts:
        classification = classify_alert_with_snapshots(db, alert)
        points = points_by_market.get(str(alert.market_id), [])
        direction = _alert_direction(alert)
        sustained = _sustained_snapshot_count(points, direction)
        reversal = _reversal_flag(points, direction)
        delivery = deliveries_by_alert_id.get(alert.id)
        slug = getattr(alert, "market_slug", None)
        results.append(
            {
                "signal_type": classification.signal_type,
                "confidence": classification.confidence,
                "suggested_action": classification.suggested_action,
                "id": alert.id,
                "type": alert.alert_type,
                "market_id": alert.market_id,
                "title": alert.title,
                "category": alert.category,
                "move": alert.move,
                "delta_pct": alert.delta_pct,
                "market_p_yes": alert.market_p_yes,
                "prev_market_p_yes": alert.prev_market_p_yes,
                "old_price": alert.old_price,
                "new_price": alert.new_price,
                "liquidity": alert.liquidity,
                "volume_24h": alert.volume_24h,
                "strength": alert.strength,
                "sustained": sustained,
                "reversal": reversal,
                "delivery_status": delivery.delivery_status if delivery else None,
                "filter_reasons": delivery.filter_reasons if delivery else [],
                "market_slug": slug,
                "market_url": market_url(str(alert.market_id), slug),
                "snapshot_bucket": alert.snapshot_bucket.isoformat(),
                "source_ts": alert.source_ts.isoformat() if alert.source_ts else None,
                "triggered_at": alert.triggered_at.isoformat() if alert.triggered_at else None,
                "created_at": alert.created_at.isoformat(),
                "message": alert.message,
            }
        )
    if paginate or cursor:
        next_cursor = None
        if len(alerts) == limit and alerts:
            last = alerts[-1]
            next_cursor = f"{int(last.created_at.timestamp())}:{last.id}"
        return {"items": results, "next_cursor": next_cursor, "total": total_count}
    return results


@router.get("/copilot/recommendations")
def copilot_recommendations(
    request: Request,
    db: Session = Depends(get_db),
    limit: int = 50,
    window_minutes: int = 24 * 60,
    strength: str | None = None,
    category: str | None = None,
    copilot: str | None = None,
    user_id: str | None = None,
    cursor: str | None = None,
    paginate: str | None = None,
    include_total: str | None = None,
):
    tenant_id, session_user_id = _resolve_request_auth(request, db)
    now_ts = datetime.now(timezone.utc)
    window_start = now_ts - timedelta(minutes=max(window_minutes, 1))

    resolved_user_id: uuid.UUID | None = None
    if session_user_id:
        resolved_user_id = session_user_id
    elif user_id:
        try:
            resolved_user_id = uuid.UUID(user_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid_user_id")
    if resolved_user_id is None:
        resolved_user = _resolve_default_user(db)
        if resolved_user:
            resolved_user_id = resolved_user.user_id

    if resolved_user_id is None:
        return {"items": [], "next_cursor": None, "total": 0} if (paginate or cursor) else []

    query = (
        db.query(AiRecommendation, Alert, AlertDelivery)
        .join(Alert, Alert.id == AiRecommendation.alert_id)
        .outerjoin(
            AlertDelivery,
            (AlertDelivery.alert_id == Alert.id)
            & (AlertDelivery.user_id == AiRecommendation.user_id),
        )
        .filter(
            Alert.tenant_id == tenant_id,
            AiRecommendation.user_id == resolved_user_id,
            AiRecommendation.created_at >= window_start,
        )
    )
    if strength:
        query = query.filter(func.upper(Alert.strength) == strength.strip().upper())
    if category:
        query = query.filter(func.lower(Alert.category) == category.strip().lower())
    if copilot:
        copilot_filter = copilot.strip().lower()
        if copilot_filter == "sent":
            query = query.filter(AlertDelivery.delivery_status == "sent")
        elif copilot_filter == "skipped":
            query = query.filter(AlertDelivery.delivery_status.in_(["skipped", "filtered"]))

    if cursor:
        try:
            cursor_ts_raw, cursor_id_raw = cursor.split(":")
            cursor_ts = datetime.fromtimestamp(int(cursor_ts_raw), tz=timezone.utc)
            cursor_id = int(cursor_id_raw)
            query = query.filter(
                or_(
                    AiRecommendation.created_at < cursor_ts,
                    (AiRecommendation.created_at == cursor_ts) & (AiRecommendation.id < cursor_id),
                )
            )
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid_cursor")

    total_count = None
    if include_total:
        total_count = query.with_entities(func.count()).scalar()

    query = query.order_by(AiRecommendation.created_at.desc(), AiRecommendation.id.desc()).limit(limit)
    rows = query.all()

    alerts: list[Alert] = []
    deliveries_by_alert_id: dict[int, AlertDelivery] = {}
    recs_by_alert_id: dict[int, AiRecommendation] = {}
    for rec, alert, delivery in rows:
        alerts.append(alert)
        if delivery:
            deliveries_by_alert_id[alert.id] = delivery
        if rec:
            recs_by_alert_id[alert.id] = rec

    market_ids = {str(alert.market_id) for alert in alerts if alert.market_id}
    points_by_market: dict[str, list[tuple[datetime, float]]] = {}
    if market_ids:
        snapshot_rows = (
            db.query(MarketSnapshot.market_id, MarketSnapshot.snapshot_bucket, MarketSnapshot.market_p_yes)
            .filter(
                MarketSnapshot.market_id.in_(market_ids),
                MarketSnapshot.snapshot_bucket >= window_start,
                MarketSnapshot.snapshot_bucket <= now_ts,
                MarketSnapshot.market_p_yes.isnot(None),
            )
            .order_by(MarketSnapshot.market_id.asc(), MarketSnapshot.snapshot_bucket.asc())
            .all()
        )
        for market_id, bucket, price in snapshot_rows:
            points_by_market.setdefault(str(market_id), []).append((bucket, price))

    attach_market_slugs(db, alerts)

    results = []
    for alert in alerts:
        classification = classify_alert_with_snapshots(db, alert)
        points = points_by_market.get(str(alert.market_id), [])
        direction = _alert_direction(alert)
        sustained = _sustained_snapshot_count(points, direction)
        reversal = _reversal_flag(points, direction)
        delivery = deliveries_by_alert_id.get(alert.id)
        rec = recs_by_alert_id.get(alert.id)
        slug = getattr(alert, "market_slug", None)
        results.append(
            {
                "signal_type": classification.signal_type,
                "confidence": rec.confidence if rec else classification.confidence,
                "suggested_action": rec.recommendation if rec else classification.suggested_action,
                "id": alert.id,
                "type": alert.alert_type,
                "market_id": alert.market_id,
                "title": alert.title,
                "category": alert.category,
                "move": alert.move,
                "delta_pct": alert.delta_pct,
                "market_p_yes": alert.market_p_yes,
                "prev_market_p_yes": alert.prev_market_p_yes,
                "old_price": alert.old_price,
                "new_price": alert.new_price,
                "liquidity": alert.liquidity,
                "volume_24h": alert.volume_24h,
                "strength": alert.strength,
                "sustained": sustained,
                "reversal": reversal,
                "delivery_status": delivery.delivery_status if delivery else None,
                "filter_reasons": delivery.filter_reasons if delivery else [],
                "market_slug": slug,
                "market_url": market_url(str(alert.market_id), slug),
                "snapshot_bucket": alert.snapshot_bucket.isoformat(),
                "source_ts": alert.source_ts.isoformat() if alert.source_ts else None,
                "triggered_at": alert.triggered_at.isoformat() if alert.triggered_at else None,
                "created_at": rec.created_at.isoformat() if rec else alert.created_at.isoformat(),
                "message": rec.rationale if rec else alert.message,
            }
        )
    if paginate or cursor:
        next_cursor = None
        if len(alerts) == limit and alerts:
            last_rec = recs_by_alert_id.get(alerts[-1].id)
            if last_rec:
                next_cursor = f"{int(last_rec.created_at.timestamp())}:{last_rec.id}"
        return {"items": results, "next_cursor": next_cursor, "total": total_count}
    return results


@router.get("/alerts/summary")
def alerts_summary(
    db: Session = Depends(get_db),
    api_key=Depends(rate_limit),
):
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    rows = (
        db.query(Alert.alert_type)
        .filter(Alert.tenant_id == api_key.tenant_id, Alert.created_at >= since)
        .all()
    )
    counts: dict[str, int] = {}
    for (alert_type,) in rows:
        counts[alert_type] = counts.get(alert_type, 0) + 1
    return {"since": since.isoformat(), "counts": counts}


@router.get("/alerts/last-digest")
def alerts_last_digest(api_key=Depends(rate_limit)):
    key = LAST_DIGEST_KEY.format(tenant_id=api_key.tenant_id)
    payload = redis_conn.get(key)
    if not payload:
        return {"last_digest": None}
    try:
        return json.loads(payload)
    except Exception:
        return {"last_digest": None}


@router.get("/copilot/runs")
def copilot_runs(
    request: Request,
    db: Session = Depends(get_db),
    limit: int = 20,
    user_id: str | None = None,
):
    _, session_user_id = _resolve_request_auth(request, db)
    resolved_user_id: uuid.UUID | None = None
    if session_user_id:
        resolved_user_id = session_user_id
    elif user_id:
        try:
            resolved_user_id = uuid.UUID(user_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid_user_id")
    if resolved_user_id is None:
        resolved_user = _resolve_default_user(db)
        if resolved_user:
            resolved_user_id = resolved_user.user_id

    pattern = COPILOT_RUN_KEY.format(run_id="*")
    try:
        scan_iter = getattr(redis_conn, "scan_iter", None)
        raw_keys = list(scan_iter(match=pattern)) if scan_iter else redis_conn.keys(pattern)
    except Exception:
        logger.exception("copilot_run_list_failed")
        raw_keys = []

    runs: list[dict[str, object]] = []
    for raw_key in raw_keys:
        key = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
        try:
            data = redis_conn.hgetall(key) or {}
        except Exception:
            continue
        if not data:
            continue

        decoded: dict[str, str] = {}
        for raw_field, raw_value in data.items():
            field = raw_field.decode() if isinstance(raw_field, (bytes, bytearray)) else str(raw_field)
            if isinstance(raw_value, (bytes, bytearray)):
                decoded[field] = raw_value.decode()
            else:
                decoded[field] = str(raw_value)

        base_raw = decoded.get("base_summary")
        if not base_raw:
            continue
        try:
            summary = json.loads(base_raw)
        except Exception:
            continue

        run_user_id = summary.get("user_id")
        if resolved_user_id and run_user_id != str(resolved_user_id):
            continue

        def _get_int(field_name: str) -> int:
            raw = decoded.get(field_name)
            if raw is None:
                return 0
            try:
                return int(raw)
            except ValueError:
                return 0

        def _get_float(field_name: str) -> float | None:
            raw = decoded.get(field_name)
            if raw is None:
                return None
            try:
                return float(raw)
            except ValueError:
                return None

        started_at = _get_float("started_at")
        if started_at is None:
            started_at = time.time()

        summary.update(
            {
                "run_id": summary.get("run_id") or key.split(":")[-1],
                "llm_calls_attempted": _get_int("llm_calls_attempted"),
                "llm_calls_succeeded": _get_int("llm_calls_succeeded"),
                "telegram_sends_attempted": _get_int("telegram_sends_attempted"),
                "telegram_sends_succeeded": _get_int("telegram_sends_succeeded"),
                "sent": _get_int("telegram_sends_succeeded"),
                "duration_ms": int((time.time() - started_at) * 1000),
            }
        )

        reason_counts = summary.get("skipped_by_reason_counts") or {}
        if not isinstance(reason_counts, dict):
            reason_counts = {}
        llm_failures = max(
            int(summary.get("llm_calls_attempted") or 0) - int(summary.get("llm_calls_succeeded") or 0),
            0,
        )
        if llm_failures:
            reason_counts["LLM_ERROR"] = reason_counts.get("LLM_ERROR", 0) + llm_failures
        telegram_failures = max(
            int(summary.get("telegram_sends_attempted") or 0) - int(summary.get("telegram_sends_succeeded") or 0),
            0,
        )
        if telegram_failures:
            reason_counts["TELEGRAM_ERROR"] = reason_counts.get("TELEGRAM_ERROR", 0) + telegram_failures
        summary["skipped_by_reason_counts"] = reason_counts

        created_at = summary.get("created_at")
        if not created_at and started_at:
            created_at = datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat()
            summary["created_at"] = created_at

        runs.append(summary)

    runs.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return runs[: max(limit, 1)]

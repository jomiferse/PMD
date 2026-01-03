from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.jobs.tasks import run_cleanup
from app.settings import settings


def _create_tables(db) -> None:
    db.execute(
        text(
            """
            CREATE TABLE market_snapshots (
                id INTEGER PRIMARY KEY,
                asof_ts DATETIME,
                source_ts DATETIME
            )
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE alerts (
                id INTEGER PRIMARY KEY,
                triggered_at DATETIME,
                created_at DATETIME
            )
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE alert_deliveries (
                id INTEGER PRIMARY KEY,
                delivered_at DATETIME
            )
            """
        )
    )


def test_cleanup_removes_rows_older_than_retention():
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(bind=engine, future=True)
    session = SessionLocal()
    _create_tables(session)
    session.commit()

    now_ts = datetime.now(timezone.utc)
    old_ts = now_ts - timedelta(days=2)
    new_ts = now_ts - timedelta(hours=12)

    session.execute(
        text(
            "INSERT INTO market_snapshots (id, asof_ts, source_ts) VALUES (:id, :asof_ts, :source_ts)"
        ),
        [
            {"id": 1, "asof_ts": old_ts, "source_ts": old_ts},
            {"id": 2, "asof_ts": new_ts, "source_ts": new_ts},
        ],
    )
    session.execute(
        text("INSERT INTO alerts (id, triggered_at, created_at) VALUES (:id, :triggered_at, :created_at)"),
        [
            {"id": 1, "triggered_at": old_ts, "created_at": old_ts},
            {"id": 2, "triggered_at": new_ts, "created_at": new_ts},
        ],
    )
    session.execute(
        text("INSERT INTO alert_deliveries (id, delivered_at) VALUES (:id, :delivered_at)"),
        [
            {"id": 1, "delivered_at": old_ts},
            {"id": 2, "delivered_at": new_ts},
        ],
    )
    session.commit()

    original_snapshot_days = settings.SNAPSHOT_RETENTION_DAYS
    original_alert_days = settings.ALERT_RETENTION_DAYS
    original_delivery_days = settings.DELIVERY_RETENTION_DAYS
    original_cleanup_enabled = settings.CLEANUP_ENABLED
    settings.SNAPSHOT_RETENTION_DAYS = 1
    settings.ALERT_RETENTION_DAYS = 1
    settings.DELIVERY_RETENTION_DAYS = 1
    settings.CLEANUP_ENABLED = True
    try:
        result = run_cleanup(session)
    finally:
        settings.SNAPSHOT_RETENTION_DAYS = original_snapshot_days
        settings.ALERT_RETENTION_DAYS = original_alert_days
        settings.DELIVERY_RETENTION_DAYS = original_delivery_days
        settings.CLEANUP_ENABLED = original_cleanup_enabled

    assert result["ok"] is True
    assert result["snapshots_deleted"] == 1
    assert result["alerts_deleted"] == 1
    assert result["deliveries_deleted"] == 1

    remaining_snapshots = session.execute(text("SELECT COUNT(*) FROM market_snapshots")).scalar()
    remaining_alerts = session.execute(text("SELECT COUNT(*) FROM alerts")).scalar()
    remaining_deliveries = session.execute(text("SELECT COUNT(*) FROM alert_deliveries")).scalar()

    assert remaining_snapshots == 1
    assert remaining_alerts == 1
    assert remaining_deliveries == 1

    session.close()

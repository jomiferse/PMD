import argparse
import os
import sys
import uuid

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from alembic import command
from alembic.config import Config


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ALEMBIC_INI = os.path.join(PROJECT_ROOT, "alembic.ini")

EXPECTED_CONSTRAINTS = {
    "ck_market_snapshots_market_p_yes",
    "ck_market_snapshots_market_p_no",
    "ck_market_snapshots_model_p_yes",
    "ck_market_snapshots_liquidity_non_negative",
    "ck_market_snapshots_volume_24h_non_negative",
    "ck_market_snapshots_volume_1w_non_negative",
    "ck_market_snapshots_best_ask_non_negative",
    "ck_market_snapshots_last_trade_price_non_negative",
    "ck_market_snapshots_market_kind",
    "ck_market_snapshots_mapping_confidence",
    "ck_alerts_market_p_yes",
    "ck_alerts_prev_market_p_yes",
    "ck_alerts_old_price",
    "ck_alerts_new_price",
    "ck_alerts_liquidity_non_negative",
    "ck_alerts_volume_24h_non_negative",
    "ck_alerts_best_ask_non_negative",
    "ck_alerts_strength",
    "ck_alerts_market_kind",
    "ck_alerts_mapping_confidence",
    "ck_plans_price_monthly_non_negative",
    "ck_plans_max_copilot_per_day_non_negative",
    "ck_plans_max_fast_copilot_per_day_non_negative",
    "ck_plans_max_copilot_per_hour_non_negative",
    "ck_plans_max_copilot_per_digest_non_negative",
    "ck_plans_copilot_theme_ttl_minutes_positive",
    "ck_plans_digest_window_minutes_positive",
    "ck_plans_max_themes_per_digest_non_negative",
    "ck_plans_max_alerts_per_digest_non_negative",
    "ck_plans_max_markets_per_theme_non_negative",
    "ck_plans_min_liquidity_non_negative",
    "ck_plans_min_volume_24h_non_negative",
    "ck_plans_min_abs_move_non_negative",
    "ck_plans_p_min_range",
    "ck_plans_p_max_range",
    "ck_plans_p_range_order",
    "ck_plans_fast_window_minutes_positive",
    "ck_plans_fast_max_themes_per_digest_non_negative",
    "ck_plans_fast_max_markets_per_theme_non_negative",
    "ck_user_alert_preferences_min_liquidity_non_negative",
    "ck_user_alert_preferences_min_volume_24h_non_negative",
    "ck_user_alert_preferences_min_abs_price_move_non_negative",
    "ck_user_alert_preferences_digest_window_minutes_positive",
    "ck_user_alert_preferences_max_alerts_per_digest_non_negative",
    "ck_user_alert_preferences_max_themes_per_digest_non_negative",
    "ck_user_alert_preferences_max_markets_per_theme_non_negative",
    "ck_user_alert_preferences_p_min_range",
    "ck_user_alert_preferences_p_max_range",
    "ck_user_alert_preferences_p_range_order",
    "ck_user_alert_preferences_fast_window_minutes_positive",
    "ck_user_alert_preferences_fast_max_themes_per_digest_non_negative",
    "ck_user_alert_preferences_fast_max_markets_per_theme_non_negative",
    "ck_alert_deliveries_delivery_status",
    "ck_ai_recommendations_recommendation",
    "ck_ai_recommendations_confidence",
    "ck_ai_recommendations_status",
    "ck_user_sessions_expires_after_created",
}

EXPECTED_INDEXES = {
    "ix_alerts_tenant_created_desc",
    "ix_alerts_tenant_strength_created_desc",
    "ix_alerts_tenant_category_norm_created_desc",
    "ix_ai_recommendations_user_created_desc",
    "ix_subscriptions_user_created_desc",
    "ix_market_snapshots_expires_at",
    "ix_alerts_expires_at",
    "ix_alert_deliveries_expires_at",
}


def _run_alembic_upgrade(db_url: str) -> None:
    config = Config(ALEMBIC_INI)
    config.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(config, "head")


def _create_temp_db(base_url) -> tuple[str, str, str]:
    tmp_name = f"pmd_schema_verify_{uuid.uuid4().hex[:8]}"
    admin_url = base_url.set(database="postgres")
    engine = create_engine(admin_url)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"CREATE DATABASE {tmp_name}"))
    engine.dispose()
    temp_url = base_url.set(database=tmp_name)
    return str(temp_url), tmp_name, str(admin_url)


def _drop_temp_db(admin_url: str, db_name: str) -> None:
    engine = create_engine(admin_url)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))
    engine.dispose()


def _check_constraints(conn) -> None:
    rows = conn.execute(
        text(
            "SELECT c.conname "
            "FROM pg_constraint c "
            "JOIN pg_namespace n ON n.oid = c.connamespace "
            "WHERE n.nspname = 'public' AND c.contype = 'c'"
        )
    ).fetchall()
    found = {row[0] for row in rows}
    missing = EXPECTED_CONSTRAINTS - found
    if missing:
        raise RuntimeError(f"Missing constraints: {sorted(missing)}")


def _check_indexes(conn) -> None:
    rows = conn.execute(
        text("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
    ).fetchall()
    found = {row[0] for row in rows}
    missing = EXPECTED_INDEXES - found
    if missing:
        raise RuntimeError(f"Missing indexes: {sorted(missing)}")


def _seed_for_explain(conn) -> tuple[str, int]:
    tenant_id = "default"
    market_id = "mkt_test_1"
    user_id = uuid.uuid4()
    now_row = conn.execute(text("SELECT now()"))
    now_ts = now_row.scalar()

    conn.execute(
        text(
            "INSERT INTO users (user_id, name, is_active, copilot_enabled, created_at) "
            "VALUES (:user_id, 'schema-verify', true, false, :now)"
        ),
        {"user_id": user_id, "now": now_ts},
    )

    alert_id = conn.execute(
        text(
            "INSERT INTO alerts ("
            "tenant_id, alert_type, market_id, title, category, move, market_p_yes, prev_market_p_yes, "
            "old_price, new_price, delta_pct, liquidity, volume_24h, best_ask, strength, snapshot_bucket, "
            "message, triggered_at, created_at, market_kind, mapping_confidence"
            ") VALUES ("
            ":tenant_id, 'DISLOCATION', :market_id, 'Test alert', 'macro', 0.02, 0.52, 0.50, "
            "0.50, 0.52, 0.02, 1000, 2000, 0.51, 'MEDIUM', :now, '', :now, :now, 'yesno', 'verified'"
            ") RETURNING id"
        ),
        {"tenant_id": tenant_id, "market_id": market_id, "now": now_ts},
    ).scalar()

    conn.execute(
        text(
            "INSERT INTO market_snapshots ("
            "market_id, title, category, market_p_yes, model_p_yes, edge, liquidity, volume_24h, volume_1w, "
            "best_ask, last_trade_price, snapshot_bucket, asof_ts, market_kind, mapping_confidence, is_yesno"
            ") VALUES ("
            ":market_id, 'Test market', 'macro', 0.52, 0.51, -0.01, 1000, 2000, 3000, 0.53, 0.52, "
            ":now, :now, 'yesno', 'verified', true"
            ")"
        ),
        {"market_id": market_id, "now": now_ts},
    )

    conn.execute(
        text(
            "INSERT INTO ai_recommendations ("
            "user_id, alert_id, created_at, recommendation, confidence, rationale, risks, status"
            ") VALUES ("
            ":user_id, :alert_id, :now, 'WAIT', 'LOW', 'r', 'r', 'PROPOSED'"
            ")"
        ),
        {"user_id": user_id, "alert_id": alert_id, "now": now_ts},
    )

    conn.execute(
        text(
            "INSERT INTO subscriptions (user_id, status, created_at, updated_at) "
            "VALUES (:user_id, 'incomplete', :now, :now)"
        ),
        {"user_id": user_id, "now": now_ts},
    )

    return tenant_id, alert_id


def _run_explain(conn) -> None:
    conn.execute(text("SET LOCAL enable_seqscan = off"))
    tenant_id, alert_id = _seed_for_explain(conn)

    plans = {
        "alerts_latest": conn.execute(
            text(
                "EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) "
                "SELECT * FROM alerts "
                "WHERE tenant_id = :tenant_id AND created_at >= now() - INTERVAL '24 hours' "
                "ORDER BY created_at DESC, id DESC LIMIT 20"
            ),
            {"tenant_id": tenant_id},
        ).fetchall(),
        "alert_history": conn.execute(
            text(
                "EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) "
                "SELECT snapshot_bucket, market_p_yes FROM market_snapshots "
                "WHERE market_id = 'mkt_test_1' "
                "AND snapshot_bucket >= now() - INTERVAL '24 hours' "
                "ORDER BY snapshot_bucket ASC"
            )
        ).fetchall(),
        "copilot_recommendations": conn.execute(
            text(
                "EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) "
                "SELECT * FROM ai_recommendations "
                "WHERE user_id = (SELECT user_id FROM users LIMIT 1) "
                "ORDER BY created_at DESC, id DESC LIMIT 20"
            )
        ).fetchall(),
        "subscription_latest": conn.execute(
            text(
                "EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) "
                "SELECT * FROM subscriptions "
                "WHERE user_id = (SELECT user_id FROM users LIMIT 1) "
                "ORDER BY created_at DESC LIMIT 1"
            )
        ).fetchall(),
    }

    print("\nExplain plans (enable_seqscan=off):")
    for name, rows in plans.items():
        print(f"\n{name}:")
        for row in rows:
            print(row[0])


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify DB schema migrations + indexes.")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DATABASE_URL to run migrations against.",
    )
    parser.add_argument(
        "--create-db",
        action="store_true",
        help="Create a temporary database for verification.",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Seed minimal data and print EXPLAIN plans.",
    )
    args = parser.parse_args()

    if not args.database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        return 2

    url = make_url(args.database_url)
    if not url.drivername.startswith("postgresql"):
        print("DATABASE_URL must be Postgres.", file=sys.stderr)
        return 2

    temp_db_name = None
    admin_url = None
    db_url = args.database_url

    try:
        if args.create_db:
            db_url, temp_db_name, admin_url = _create_temp_db(url)

        _run_alembic_upgrade(db_url)

        engine = create_engine(db_url)
        with engine.begin() as conn:
            _check_constraints(conn)
            _check_indexes(conn)
            if args.explain:
                _run_explain(conn)
        engine.dispose()
    finally:
        if temp_db_name and admin_url:
            _drop_temp_db(admin_url, temp_db_name)

    print("Schema verification OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

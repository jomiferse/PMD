import uuid

from sqlalchemy import String, Float, DateTime, Integer, BigInteger, Boolean, ForeignKey, func, UniqueConstraint, Index, text, Text, JSON
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .db import Base


class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    __table_args__ = (
        UniqueConstraint("market_id", "snapshot_bucket", name="uq_market_bucket"),
        Index("ix_market_snapshots_bucket", "snapshot_bucket"),
        Index("ix_market_snapshots_market_asof", "market_id", "asof_ts"),
        Index("ix_market_snapshots_market_bucket", "market_id", "snapshot_bucket"),
        Index("ix_market_snapshots_asof_desc", text("asof_ts DESC")),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    market_id: Mapped[str] = mapped_column(String(128), index=True)
    title: Mapped[str] = mapped_column(String(512))
    category: Mapped[str] = mapped_column(String(128), default="unknown")
    slug: Mapped[str | None] = mapped_column(Text, nullable=True)

    market_p_yes: Mapped[float] = mapped_column(Float)  # implied prob (0-1)
    market_p_no: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_p_no_derived: Mapped[bool | None] = mapped_column(Boolean, nullable=True, default=True)
    primary_outcome_label: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_yesno: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    mapping_confidence: Mapped[str | None] = mapped_column(String(16), nullable=True)
    market_kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    liquidity: Mapped[float] = mapped_column(Float, default=0.0)
    volume_24h: Mapped[float] = mapped_column(Float, default=0.0)
    volume_1w: Mapped[float] = mapped_column(Float, default=0.0)
    best_ask: Mapped[float] = mapped_column(Float, default=0.0)
    last_trade_price: Mapped[float] = mapped_column(Float, default=0.0)

    model_p_yes: Mapped[float] = mapped_column(Float)
    edge: Mapped[float] = mapped_column(Float)  # model - market

    source_ts: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    snapshot_bucket: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    asof_ts: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), index=True)


class ApiKey(Base):
    __tablename__ = "api_keys"
    __table_args__ = (
        UniqueConstraint("key_hash", name="uq_api_key_hash"),
        Index("ix_api_keys_tenant_id", "tenant_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    plan: Mapped[str] = mapped_column(String(64), default="basic")
    rate_limit_per_min: Mapped[int] = mapped_column(Integer, default=60)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now())
    last_used_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    revoked_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class Alert(Base):
    __tablename__ = "alerts"
    __table_args__ = (
        UniqueConstraint("alert_type", "market_id", "snapshot_bucket", name="uq_alert_market_bucket"),
        Index("ix_alerts_created_at", "created_at"),
        Index("ix_alerts_tenant_type", "tenant_id", "alert_type"),
        Index("ix_alerts_tenant_created", "tenant_id", "created_at"),
        Index("ix_alerts_tenant_strength", "tenant_id", "strength"),
        Index("ix_alerts_tenant_category", "tenant_id", "category"),
        Index("ix_alerts_cooldown", "tenant_id", "alert_type", "market_id", "triggered_at"),
        Index("ix_alerts_market_triggered", "market_id", "triggered_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    alert_type: Mapped[str] = mapped_column(String(32), nullable=False)
    market_id: Mapped[str] = mapped_column(String(128), nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    category: Mapped[str] = mapped_column(String(128), default="unknown")
    move: Mapped[float] = mapped_column(Float, default=0.0)
    market_p_yes: Mapped[float] = mapped_column(Float, default=0.0)
    prev_market_p_yes: Mapped[float] = mapped_column(Float, default=0.0)
    primary_outcome_label: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_yesno: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    mapping_confidence: Mapped[str | None] = mapped_column(String(16), nullable=True)
    market_kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    old_price: Mapped[float] = mapped_column(Float, default=0.0)
    new_price: Mapped[float] = mapped_column(Float, default=0.0)
    delta_pct: Mapped[float] = mapped_column(Float, default=0.0)
    liquidity: Mapped[float] = mapped_column(Float, default=0.0)
    volume_24h: Mapped[float] = mapped_column(Float, default=0.0)
    best_ask: Mapped[float] = mapped_column(Float, default=0.0)
    strength: Mapped[str] = mapped_column(String(16), default="MEDIUM")
    snapshot_bucket: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    source_ts: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    message: Mapped[str] = mapped_column(String(1024), default="")
    triggered_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now())
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now())


class Plan(Base):
    __tablename__ = "plans"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    price_monthly: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    copilot_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    max_copilot_per_day: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_fast_copilot_per_day: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_copilot_per_hour: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_copilot_per_digest: Mapped[int | None] = mapped_column(Integer, nullable=True)
    copilot_theme_ttl_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fast_signals_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    digest_window_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_themes_per_digest: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_alerts_per_digest: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_markets_per_theme: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_liquidity: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_volume_24h: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_abs_move: Mapped[float | None] = mapped_column(Float, nullable=True)
    p_min: Mapped[float | None] = mapped_column(Float, nullable=True)
    p_max: Mapped[float | None] = mapped_column(Float, nullable=True)
    allowed_strengths: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fast_mode: Mapped[str | None] = mapped_column(String(16), nullable=True)
    fast_window_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fast_max_themes_per_digest: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fast_max_markets_per_theme: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("telegram_chat_id", name="uq_users_telegram_chat_id"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    telegram_chat_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    plan_id: Mapped[int | None] = mapped_column(ForeignKey("plans.id"), nullable=True)
    copilot_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    overrides_json: Mapped[dict | None] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=True,
    )
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)

    plan = relationship("Plan")


class UserAuth(Base):
    __tablename__ = "user_auth"
    __table_args__ = (
        UniqueConstraint("email", name="uq_user_auth_email"),
        Index("ix_user_auth_email", "email"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)

    user = relationship("User")


class UserSession(Base):
    __tablename__ = "user_sessions"
    __table_args__ = (
        Index("ix_user_sessions_user_id", "user_id"),
        Index("ix_user_sessions_expires_at", "expires_at"),
    )

    token: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)
    expires_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    revoked_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)

    user = relationship("User")


class Subscription(Base):
    __tablename__ = "subscriptions"
    __table_args__ = (
        UniqueConstraint("stripe_subscription_id", name="uq_subscriptions_stripe_subscription_id"),
        Index("ix_subscriptions_user_id", "user_id"),
        Index("ix_subscriptions_customer_id", "stripe_customer_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    plan_id: Mapped[int | None] = mapped_column(ForeignKey("plans.id"), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="incomplete")
    current_period_end: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    stripe_customer_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    stripe_subscription_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User")
    plan = relationship("Plan")


class StripeEvent(Base):
    __tablename__ = "stripe_events"

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)


class PendingTelegramChat(Base):
    __tablename__ = "pending_telegram_chats"

    telegram_chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    first_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")


class UserAlertPreference(Base):
    __tablename__ = "user_alert_preferences"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        primary_key=True,
    )
    min_liquidity: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_volume_24h: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_abs_price_move: Mapped[float | None] = mapped_column(Float, nullable=True)
    alert_strengths: Mapped[str | None] = mapped_column(String(32), nullable=True)
    digest_window_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_alerts_per_digest: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_themes_per_digest: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_markets_per_theme: Mapped[int | None] = mapped_column(Integer, nullable=True)
    p_min: Mapped[float | None] = mapped_column(Float, nullable=True)
    p_max: Mapped[float | None] = mapped_column(Float, nullable=True)
    fast_signals_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    fast_window_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fast_max_themes_per_digest: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fast_max_markets_per_theme: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)


class AlertDelivery(Base):
    __tablename__ = "alert_deliveries"
    __table_args__ = (
        UniqueConstraint("alert_id", "user_id", name="uq_alert_delivery_alert_user"),
        Index("ix_alert_deliveries_user_status", "user_id", "delivery_status"),
        Index("ix_alert_deliveries_delivered_at", "delivered_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    alert_id: Mapped[int] = mapped_column(ForeignKey("alerts.id", ondelete="CASCADE"), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    delivered_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)
    delivery_status: Mapped[str] = mapped_column(String(16), nullable=False)
    filter_reasons: Mapped[list | None] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=True,
        default=list,
    )


class AiRecommendation(Base):
    __tablename__ = "ai_recommendations"
    __table_args__ = (
        Index("ix_ai_recommendations_user_status", "user_id", "status"),
        Index("ix_ai_recommendations_user_created", "user_id", "created_at"),
        Index("ix_ai_recommendations_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    alert_id: Mapped[int] = mapped_column(ForeignKey("alerts.id", ondelete="CASCADE"), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)
    recommendation: Mapped[str] = mapped_column(String(8), nullable=False)
    confidence: Mapped[str] = mapped_column(String(8), nullable=False)
    rationale: Mapped[str] = mapped_column(Text, nullable=False)
    risks: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="PROPOSED", nullable=False)
    telegram_message_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    expires_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class AiMarketMute(Base):
    __tablename__ = "ai_market_mutes"
    __table_args__ = (
        UniqueConstraint("user_id", "market_id", name="uq_ai_market_mutes_user_market"),
        Index("ix_ai_market_mutes_expires_at", "expires_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    market_id: Mapped[str] = mapped_column(String(128), nullable=False)
    expires_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)


class AiThemeMute(Base):
    __tablename__ = "ai_theme_mutes"
    __table_args__ = (
        UniqueConstraint("user_id", "theme_key", name="uq_ai_theme_mutes_user_theme"),
        Index("ix_ai_theme_mutes_expires_at", "expires_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    theme_key: Mapped[str] = mapped_column(String(256), nullable=False)
    expires_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)


class AiRecommendationEvent(Base):
    __tablename__ = "ai_recommendation_events"
    __table_args__ = (
        Index("ix_ai_recommendation_events_user_created", "user_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    recommendation_id: Mapped[int] = mapped_column(
        ForeignKey("ai_recommendations.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    alert_id: Mapped[int] = mapped_column(
        ForeignKey("alerts.id", ondelete="CASCADE"),
        nullable=False,
    )
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    details: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)

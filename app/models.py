import uuid

from sqlalchemy import String, Float, DateTime, Integer, Boolean, ForeignKey, func, UniqueConstraint, Index, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base

class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    __table_args__ = (
        UniqueConstraint("market_id", "snapshot_bucket", name="uq_market_bucket"),
        Index("ix_market_snapshots_bucket", "snapshot_bucket"),
        Index("ix_market_snapshots_market_asof", "market_id", "asof_ts"),
        Index("ix_market_snapshots_asof_desc", text("asof_ts DESC")),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    market_id: Mapped[str] = mapped_column(String(128), index=True)
    title: Mapped[str] = mapped_column(String(512))
    category: Mapped[str] = mapped_column(String(128), default="unknown")

    market_p_yes: Mapped[float] = mapped_column(Float)  # implied prob (0-1)
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
    old_price: Mapped[float] = mapped_column(Float, default=0.0)
    new_price: Mapped[float] = mapped_column(Float, default=0.0)
    delta_pct: Mapped[float] = mapped_column(Float, default=0.0)
    liquidity: Mapped[float] = mapped_column(Float, default=0.0)
    volume_24h: Mapped[float] = mapped_column(Float, default=0.0)
    strength: Mapped[str] = mapped_column(String(16), default="MEDIUM")
    snapshot_bucket: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    source_ts: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    message: Mapped[str] = mapped_column(String(1024), default="")
    triggered_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), index=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), index=True)


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    telegram_chat_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)


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
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), nullable=False)


class AlertDelivery(Base):
    __tablename__ = "alert_deliveries"
    __table_args__ = (
        UniqueConstraint("alert_id", "user_id", name="uq_alert_delivery_alert_user"),
        Index("ix_alert_deliveries_user_status", "user_id", "delivery_status"),
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

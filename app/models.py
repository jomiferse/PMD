from sqlalchemy import String, Float, DateTime, Integer, func, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base

class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    __table_args__ = (
        UniqueConstraint("market_id", "snapshot_bucket", name="uq_market_bucket"),
        Index("ix_market_snapshots_bucket", "snapshot_bucket"),
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
    liquidity: Mapped[float] = mapped_column(Float, default=0.0)
    volume_24h: Mapped[float] = mapped_column(Float, default=0.0)
    snapshot_bucket: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    source_ts: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    message: Mapped[str] = mapped_column(String(1024), default="")
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), index=True)

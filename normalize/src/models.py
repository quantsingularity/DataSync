"""
DataSync Normalize
==================
Canonical Pydantic models for all market data events.
All sources (Alpaca, Polygon, mock) emit the same schema.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# Asset classes
class AssetClass(str, Enum):
    EQUITY = "equity"
    CRYPTO = "crypto"
    OPTION = "option"


# Data sources
class DataSource(str, Enum):
    ALPACA = "alpaca"
    POLYGON = "polygon"
    MOCK = "mock"


# Timeframes for OHLCV bars
class Timeframe(str, Enum):
    ONE_MIN = "1m"
    FIVE_MIN = "5m"
    FIFTEEN_MIN = "15m"
    ONE_HOUR = "1h"
    FOUR_HOUR = "4h"
    ONE_DAY = "1d"
    ONE_WEEK = "1w"


# ── Canonical Tick ────────────────────────────────────────────────────────────


class NormalizedTick(BaseModel):
    """
    Canonical tick event emitted to Kafka and stored in TimescaleDB.
    All feed adapters (Alpaca, Polygon, mock) produce this exact schema.
    """

    event_type: str = "tick"
    time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    symbol: str
    asset_class: AssetClass
    source: DataSource

    # Price fields
    price: Decimal
    size: Optional[Decimal] = None

    # Quote fields (optional - not all feeds provide both sides)
    bid: Optional[Decimal] = None
    ask: Optional[Decimal] = None
    bid_size: Optional[Decimal] = None
    ask_size: Optional[Decimal] = None

    # Metadata
    conditions: List[str] = []
    exchange: Optional[str] = None
    tape: Optional[str] = None
    extra: Dict[str, Any] = {}

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("time", mode="before")
    @classmethod
    def ensure_tz(cls, v) -> datetime:
        if isinstance(v, str):
            v = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if isinstance(v, datetime) and v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v

    def kafka_key(self) -> bytes:
        return self.symbol.encode()

    def to_kafka_value(self) -> Dict[str, Any]:
        return self.model_dump(mode="json")

    class Config:
        use_enum_values = True


# ── Canonical OHLCV Bar ───────────────────────────────────────────────────────


class NormalizedBar(BaseModel):
    """Canonical OHLCV (candlestick) bar."""

    event_type: str = "bar"
    time: datetime
    symbol: str
    asset_class: AssetClass
    timeframe: Timeframe
    source: DataSource

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Optional[Decimal] = None
    trade_count: Optional[int] = None
    vwap: Optional[Decimal] = None

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("time", mode="before")
    @classmethod
    def ensure_tz(cls, v) -> datetime:
        if isinstance(v, str):
            v = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if isinstance(v, datetime) and v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v

    class Config:
        use_enum_values = True


# ── Alt Data ─────────────────────────────────────────────────────────────────


class AltDataType(str, Enum):
    NEWS_SENTIMENT = "news_sentiment"
    SOCIAL_VOLUME = "social_volume"
    ONCHAIN = "onchain"


class NormalizedAltData(BaseModel):
    """
    Canonical alternative data event.
    Covers: news sentiment, social volume, on-chain metrics.
    """

    event_type: str = "alt_data"
    time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    data_type: AltDataType
    source: str
    symbol: Optional[str] = None

    # Sentiment score: -1.0 (very negative) to +1.0 (very positive)
    score: Optional[float] = None
    # Volume count (mentions, transactions, etc.)
    volume: Optional[float] = None

    # Full raw payload
    payload: Dict[str, Any] = {}

    @field_validator("time", mode="before")
    @classmethod
    def ensure_tz(cls, v) -> datetime:
        if isinstance(v, str):
            v = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if isinstance(v, datetime) and v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v

    class Config:
        use_enum_values = True


# ── Subscription ──────────────────────────────────────────────────────────────


class SubscriptionRequest(BaseModel):
    service_name: str
    symbol: str
    asset_class: AssetClass

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        return v.upper().strip()


class SubscriptionResponse(BaseModel):
    id: int
    service_name: str
    symbol: str
    asset_class: str
    is_active: bool
    subscribed_at: datetime

    class Config:
        from_attributes = True

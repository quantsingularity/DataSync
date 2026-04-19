"""
DataSync - Redis Cache Layer
Caches frequently accessed price data and bar series.
TTL is configurable per data type via environment variables.
"""

import json
import logging
import os
from decimal import Decimal
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis

logger = logging.getLogger("datasync.cache")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "60"))
BARS_CACHE_TTL = int(os.getenv("BARS_CACHE_TTL_SECONDS", "300"))

_redis: Optional[aioredis.Redis] = None


def _default(o: Any) -> Any:
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Not serializable: {type(o)}")


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis


# ── Price cache (latest tick per symbol) ─────────────────────────────────────


def _price_key(symbol: str) -> str:
    return f"datasync:price:{symbol.upper()}"


async def cache_price(
    symbol: str, price_data: Dict[str, Any], ttl: int = CACHE_TTL
) -> None:
    r = await get_redis()
    await r.setex(_price_key(symbol), ttl, json.dumps(price_data, default=_default))


async def get_cached_price(symbol: str) -> Optional[Dict[str, Any]]:
    r = await get_redis()
    raw = await r.get(_price_key(symbol))
    return json.loads(raw) if raw else None


# ── Bar series cache ──────────────────────────────────────────────────────────


def _bars_key(symbol: str, timeframe: str, from_ts: str, to_ts: str) -> str:
    return f"datasync:bars:{symbol.upper()}:{timeframe}:{from_ts}:{to_ts}"


async def cache_bars(
    symbol: str,
    timeframe: str,
    from_ts: str,
    to_ts: str,
    bars: List[Dict[str, Any]],
    ttl: int = BARS_CACHE_TTL,
) -> None:
    r = await get_redis()
    key = _bars_key(symbol, timeframe, from_ts, to_ts)
    await r.setex(key, ttl, json.dumps(bars, default=_default))


async def get_cached_bars(
    symbol: str, timeframe: str, from_ts: str, to_ts: str
) -> Optional[List[Dict[str, Any]]]:
    r = await get_redis()
    key = _bars_key(symbol, timeframe, from_ts, to_ts)
    raw = await r.get(key)
    return json.loads(raw) if raw else None


# ── Subscription cache ────────────────────────────────────────────────────────

SUBS_KEY = "datasync:subscriptions"


async def cache_subscriptions(subs: List[Dict[str, Any]]) -> None:
    r = await get_redis()
    await r.setex(SUBS_KEY, 60, json.dumps(subs, default=_default))


async def get_cached_subscriptions() -> Optional[List[Dict[str, Any]]]:
    r = await get_redis()
    raw = await r.get(SUBS_KEY)
    return json.loads(raw) if raw else None


# ── Latest tick streaming cache ───────────────────────────────────────────────


async def push_tick_to_stream(symbol: str, tick_data: Dict[str, Any]) -> None:
    """Push tick to a Redis stream for real-time consumers."""
    r = await get_redis()
    stream = f"datasync:stream:{symbol.upper()}"
    await r.xadd(
        stream,
        {k: str(v) for k, v in tick_data.items()},
        maxlen=1000,
        approximate=True,
    )


async def invalidate_bars_cache(symbol: str, timeframe: str = "*") -> None:
    """Invalidate all bar cache entries for a symbol."""
    r = await get_redis()
    pattern = f"datasync:bars:{symbol.upper()}:{timeframe}:*"
    keys = await r.keys(pattern)
    if keys:
        await r.delete(*keys)
        logger.debug(f"Invalidated {len(keys)} bar cache keys for {symbol}")

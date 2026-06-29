"""
DataSync REST API
=================
Endpoints:
  GET  /bars/{symbol}              - OHLCV bars with caching
  GET  /ticks/{symbol}             - Raw tick data
  GET  /price/{symbol}             - Latest price (Redis-first)
  GET  /alt-data/{symbol}          - Alternative data for symbol
  POST /subscriptions              - Register symbol subscription
  GET  /subscriptions              - List all subscriptions
  DELETE /subscriptions/{id}       - Remove subscription
  GET  /symbols                    - List all tradable symbols with data
  GET  /health                     - Health check
"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from api.src.subscriptions import (
    create_subscription,
    delete_subscription,
    list_subscriptions,
)
from normalize.src.models import SubscriptionRequest, SubscriptionResponse
from store.src.cache import (
    cache_bars,
    cache_price,
    cache_subscriptions,
    get_cached_bars,
    get_cached_price,
    get_cached_subscriptions,
)
from store.src.timescale import (
    close_pool,
    get_pool,
    query_bars,
    query_latest_price,
    query_ticks,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("datasync.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("DataSync API starting...")
    # Don't crash the whole service if the DB is briefly unavailable at boot;
    # /health reports "degraded" and pooled connections are retried lazily.
    try:
        await get_pool()
    except Exception as exc:
        logger.warning("DB pool not ready at startup (will retry lazily): %s", exc)
    yield
    logger.info("DataSync API stopping...")
    await close_pool()


app = FastAPI(
    title="DataSync Market Data API",
    description="Unified real-time and historical market data for equities, crypto, and options.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)
Instrumentator().instrument(app).expose(app)


# ── Bars ──────────────────────────────────────────────────────────────────────


@app.get("/bars/{symbol}", summary="Historical OHLCV bars")
async def get_bars(
    symbol: str,
    timeframe: str = Query("1d", description="1m | 5m | 15m | 1h | 1d"),
    from_: Optional[str] = Query(
        None, alias="from", description="ISO8601 start datetime"
    ),
    to: Optional[str] = Query(None, description="ISO8601 end datetime"),
    limit: int = Query(500, le=5000),
):
    """
    Fetch OHLCV bars for a symbol.
    Returns cached data when available. Cache TTL is configurable via BARS_CACHE_TTL_SECONDS.
    """
    from_dt = (
        datetime.fromisoformat(from_)
        if from_
        else (datetime.now(timezone.utc) - timedelta(days=30))
    )
    to_dt = datetime.fromisoformat(to) if to else datetime.now(timezone.utc)

    if from_dt.tzinfo is None:
        from_dt = from_dt.replace(tzinfo=timezone.utc)
    if to_dt.tzinfo is None:
        to_dt = to_dt.replace(tzinfo=timezone.utc)

    from_str = from_dt.isoformat()
    to_str = to_dt.isoformat()

    # Cache-first
    cached = await get_cached_bars(symbol, timeframe, from_str, to_str)
    if cached is not None:
        return {
            "symbol": symbol.upper(),
            "timeframe": timeframe,
            "bars": cached,
            "cached": True,
        }

    bars = await query_bars(symbol, timeframe, from_dt, to_dt, limit)
    if not bars:
        raise HTTPException(404, f"No bars found for {symbol} ({timeframe})")

    # Serialize for JSON and cache
    serialized = [
        {
            k: (
                float(v)
                if hasattr(v, "__float__")
                else str(v) if isinstance(v, datetime) else v
            )
            for k, v in row.items()
        }
        for row in bars
    ]
    await cache_bars(symbol, timeframe, from_str, to_str, serialized)

    return {
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "bars": serialized,
        "cached": False,
    }


# ── Ticks ─────────────────────────────────────────────────────────────────────


@app.get("/ticks/{symbol}", summary="Raw tick data")
async def get_ticks(
    symbol: str,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None),
    limit: int = Query(1000, le=10000),
):
    from_dt = (
        datetime.fromisoformat(from_)
        if from_
        else (datetime.now(timezone.utc) - timedelta(hours=1))
    )
    to_dt = datetime.fromisoformat(to) if to else datetime.now(timezone.utc)

    if from_dt.tzinfo is None:
        from_dt = from_dt.replace(tzinfo=timezone.utc)
    if to_dt.tzinfo is None:
        to_dt = to_dt.replace(tzinfo=timezone.utc)

    ticks = await query_ticks(symbol, from_dt, to_dt, limit)
    if not ticks:
        raise HTTPException(404, f"No ticks found for {symbol}")

    return {
        "symbol": symbol.upper(),
        "count": len(ticks),
        "ticks": [
            {
                k: (
                    float(v)
                    if hasattr(v, "__float__")
                    else str(v) if isinstance(v, datetime) else v
                )
                for k, v in row.items()
            }
            for row in ticks
        ],
    }


# ── Latest price ──────────────────────────────────────────────────────────────


@app.get("/price/{symbol}", summary="Latest price (Redis-first)")
async def get_price(symbol: str):
    """Returns the latest known price, checking Redis cache before TimescaleDB."""
    cached = await get_cached_price(symbol)
    if cached:
        return {"symbol": symbol.upper(), **cached, "cached": True}

    row = await query_latest_price(symbol)
    if not row:
        raise HTTPException(404, f"No price data found for {symbol}")

    result = {
        k: (
            float(v)
            if hasattr(v, "__float__")
            else str(v) if isinstance(v, datetime) else v
        )
        for k, v in row.items()
    }
    await cache_price(symbol, result)
    return {"symbol": symbol.upper(), **result, "cached": False}


# ── Alt data ──────────────────────────────────────────────────────────────────


@app.get("/alt-data/{symbol}", summary="Alternative data for symbol")
async def get_alt_data(
    symbol: str,
    data_type: Optional[str] = Query(
        None, description="news_sentiment | social_volume | onchain"
    ),
    limit: int = Query(20, le=200),
):
    pool = await get_pool()
    q = "SELECT * FROM alt_data WHERE symbol = $1 ORDER BY time DESC LIMIT $2"
    params = [symbol.upper(), limit]

    if data_type:
        q = "SELECT * FROM alt_data WHERE symbol=$1 AND data_type=$2 ORDER BY time DESC LIMIT $3"
        params = [symbol.upper(), data_type, limit]

    rows = await pool.fetch(q, *params)
    if not rows:
        raise HTTPException(404, f"No alt data for {symbol}")

    return {
        "symbol": symbol.upper(),
        "count": len(rows),
        "data": [dict(r) for r in rows],
    }


# ── Subscriptions ─────────────────────────────────────────────────────────────


@app.post("/subscriptions", response_model=SubscriptionResponse, status_code=201)
async def add_subscription(body: SubscriptionRequest):
    """Register a downstream service's interest in a symbol."""
    return await create_subscription(body)


@app.get("/subscriptions", response_model=List[SubscriptionResponse])
async def get_subscriptions(active_only: bool = True):
    """List all symbol subscriptions."""
    cached = await get_cached_subscriptions()
    if cached:
        return cached
    subs = await list_subscriptions(active_only)
    # list_subscriptions returns plain dicts; cache them directly.
    # (Previously this did `s.__dict__`, which raises AttributeError on dicts.)
    await cache_subscriptions(subs)
    return subs


@app.delete("/subscriptions/{sub_id}", status_code=204)
async def remove_subscription(sub_id: int):
    await delete_subscription(sub_id)


# ── Symbol list ───────────────────────────────────────────────────────────────


@app.get("/symbols", summary="All symbols with data in TimescaleDB")
async def list_symbols():
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT DISTINCT symbol, asset_class FROM ticks ORDER BY symbol"
    )
    return {"symbols": [dict(r) for r in rows]}


# ── Health ────────────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    try:
        pool = await get_pool()
        await pool.fetchval("SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False
    return {
        "status": "ok" if db_ok else "degraded",
        "db": db_ok,
        "service": "datasync-api",
    }

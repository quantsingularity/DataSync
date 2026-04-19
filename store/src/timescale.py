"""
DataSync - Store Module
Writes normalized ticks and bars to TimescaleDB.
Uses asyncpg for high-throughput batch inserts.
"""

import logging
import os
from datetime import datetime
from typing import List, Optional, Sequence

import asyncpg

from normalize.src.models import NormalizedAltData, NormalizedBar, NormalizedTick

logger = logging.getLogger("datasync.store")

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://datasync:datasync_secret@timescaledb:5432/datasync"
)
# asyncpg uses postgresql:// not postgresql+asyncpg://
_ASYNCPG_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            _ASYNCPG_URL,
            min_size=2,
            max_size=10,
            timeout=30,
        )
        logger.info("TimescaleDB connection pool created")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# ── Tick writes ───────────────────────────────────────────────────────────────


async def write_tick(tick: NormalizedTick) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO ticks
            (time, symbol, asset_class, source, price, size, bid, ask,
             bid_size, ask_size, conditions, exchange, tape, extra)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT DO NOTHING
        """,
        tick.time,
        tick.symbol,
        str(tick.asset_class),
        str(tick.source),
        tick.price,
        tick.size,
        tick.bid,
        tick.ask,
        tick.bid_size,
        tick.ask_size,
        tick.conditions or [],
        tick.exchange,
        tick.tape,
        tick.extra or {},
    )


async def write_ticks_batch(ticks: Sequence[NormalizedTick]) -> int:
    """Batch-insert ticks for efficiency. Returns number inserted."""
    if not ticks:
        return 0
    pool = await get_pool()
    rows = [
        (
            t.time,
            t.symbol,
            str(t.asset_class),
            str(t.source),
            t.price,
            t.size,
            t.bid,
            t.ask,
            t.bid_size,
            t.ask_size,
            t.conditions or [],
            t.exchange,
            t.tape,
            t.extra or {},
        )
        for t in ticks
    ]
    await pool.executemany(
        """
        INSERT INTO ticks
            (time, symbol, asset_class, source, price, size, bid, ask,
             bid_size, ask_size, conditions, exchange, tape, extra)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    return len(rows)


# ── Bar writes ────────────────────────────────────────────────────────────────


async def write_bar(bar: NormalizedBar) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO bars
            (time, symbol, asset_class, timeframe, source,
             open, high, low, close, volume, trade_count, vwap)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (symbol, timeframe, time) DO UPDATE
          SET open=EXCLUDED.open, high=EXCLUDED.high,
              low=EXCLUDED.low, close=EXCLUDED.close,
              volume=EXCLUDED.volume, trade_count=EXCLUDED.trade_count,
              vwap=EXCLUDED.vwap
        """,
        bar.time,
        bar.symbol,
        str(bar.asset_class),
        str(bar.timeframe),
        str(bar.source),
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.volume,
        bar.trade_count,
        bar.vwap,
    )


async def write_bars_batch(bars: Sequence[NormalizedBar]) -> int:
    if not bars:
        return 0
    pool = await get_pool()
    rows = [
        (
            b.time,
            b.symbol,
            str(b.asset_class),
            str(b.timeframe),
            str(b.source),
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume,
            b.trade_count,
            b.vwap,
        )
        for b in bars
    ]
    await pool.executemany(
        """
        INSERT INTO bars
            (time, symbol, asset_class, timeframe, source,
             open, high, low, close, volume, trade_count, vwap)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (symbol, timeframe, time) DO UPDATE
          SET close=EXCLUDED.close, volume=EXCLUDED.volume
        """,
        rows,
    )
    return len(rows)


# ── Alt data writes ────────────────────────────────────────────────────────────


async def write_alt_data(alt: NormalizedAltData) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO alt_data (time, symbol, data_type, source, score, volume, payload)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        """,
        alt.time,
        alt.symbol,
        str(alt.data_type),
        alt.source,
        alt.score,
        alt.volume,
        alt.payload,
    )


# ── Historical queries ────────────────────────────────────────────────────────


async def query_bars(
    symbol: str,
    timeframe: str,
    from_dt: datetime,
    to_dt: datetime,
    limit: int = 1000,
) -> List[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT time, symbol, asset_class, timeframe, source,
               open, high, low, close, volume, trade_count, vwap
        FROM bars
        WHERE symbol = $1 AND timeframe = $2
          AND time >= $3 AND time <= $4
        ORDER BY time ASC
        LIMIT $5
        """,
        symbol.upper(),
        timeframe,
        from_dt,
        to_dt,
        limit,
    )
    return [dict(r) for r in rows]


async def query_ticks(
    symbol: str,
    from_dt: datetime,
    to_dt: datetime,
    limit: int = 5000,
) -> List[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT time, symbol, asset_class, source, price, size,
               bid, ask, conditions, exchange
        FROM ticks
        WHERE symbol = $1
          AND time >= $2 AND time <= $3
        ORDER BY time ASC
        LIMIT $4
        """,
        symbol.upper(),
        from_dt,
        to_dt,
        limit,
    )
    return [dict(r) for r in rows]


async def query_latest_price(symbol: str) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        SELECT time, symbol, price, bid, ask, source
        FROM ticks
        WHERE symbol = $1
        ORDER BY time DESC
        LIMIT 1
        """,
        symbol.upper(),
    )
    return dict(row) if row else None

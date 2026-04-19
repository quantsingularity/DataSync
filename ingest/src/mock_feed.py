"""
DataSync - Mock WebSocket Feed
Generates realistic fake tick and bar data for development and testing.
No real API keys required when USE_MOCK_FEEDS=true.
"""

import asyncio
import logging
import os
import random
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, List

from normalize.src.models import (
    AssetClass,
    DataSource,
    NormalizedBar,
    NormalizedTick,
    Timeframe,
)

logger = logging.getLogger("datasync.ingest.mock")

MOCK_INTERVAL_MS = int(os.getenv("MOCK_FEED_INTERVAL_MS", "500"))

# Realistic starting prices
SEED_PRICES = {
    "AAPL": 185.00,
    "MSFT": 420.00,
    "GOOGL": 175.00,
    "AMZN": 195.00,
    "TSLA": 200.00,
    "SPY": 525.00,
    "QQQ": 445.00,
    "BTC/USD": 68000.00,
    "ETH/USD": 3500.00,
    "SOL/USD": 175.00,
}

EQUITY_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "SPY", "QQQ"]
CRYPTO_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]


def _random_walk(current: float, volatility: float = 0.001) -> float:
    """Apply a small random walk step."""
    change = current * volatility * random.gauss(0, 1)
    return max(current + change, 0.01)


def _make_tick(symbol: str, price: float, asset_class: AssetClass) -> NormalizedTick:
    spread = price * 0.0002
    return NormalizedTick(
        time=datetime.now(timezone.utc),
        symbol=symbol,
        asset_class=asset_class,
        source=DataSource.MOCK,
        price=Decimal(str(round(price, 4))),
        size=Decimal(str(random.randint(1, 500))),
        bid=Decimal(str(round(price - spread / 2, 4))),
        ask=Decimal(str(round(price + spread / 2, 4))),
        bid_size=Decimal(str(random.randint(100, 2000))),
        ask_size=Decimal(str(random.randint(100, 2000))),
        exchange="MOCK",
        conditions=[],
        extra={},
    )


async def run_mock_equity_feed(
    symbols: List[str],
    on_tick: Callable,
    stop_event: asyncio.Event,
) -> None:
    """Generate fake equity tick data at MOCK_INTERVAL_MS frequency."""
    prices = {s: SEED_PRICES.get(s, 100.0) for s in symbols}
    logger.info(f"Mock equity feed started: {symbols}")

    while not stop_event.is_set():
        for symbol in symbols:
            prices[symbol] = _random_walk(prices[symbol], volatility=0.0005)
            tick = _make_tick(symbol, prices[symbol], AssetClass.EQUITY)
            await on_tick(tick)

        await asyncio.sleep(MOCK_INTERVAL_MS / 1000)


async def run_mock_crypto_feed(
    symbols: List[str],
    on_tick: Callable,
    stop_event: asyncio.Event,
) -> None:
    """Generate fake crypto tick data - higher volatility than equities."""
    prices = {s: SEED_PRICES.get(s, 1000.0) for s in symbols}
    logger.info(f"Mock crypto feed started: {symbols}")

    while not stop_event.is_set():
        for symbol in symbols:
            prices[symbol] = _random_walk(prices[symbol], volatility=0.001)
            tick = _make_tick(symbol, prices[symbol], AssetClass.CRYPTO)
            await on_tick(tick)

        await asyncio.sleep(MOCK_INTERVAL_MS / 1000)


async def run_mock_bar_feed(
    symbols: List[str],
    asset_class: AssetClass,
    on_bar: Callable,
    stop_event: asyncio.Event,
    interval_s: int = 60,
) -> None:
    """Generate fake 1-minute OHLCV bars."""
    prices = {s: SEED_PRICES.get(s, 100.0) for s in symbols}
    logger.info(f"Mock bar feed started: {symbols} ({asset_class})")

    while not stop_event.is_set():
        for symbol in symbols:
            o = prices[symbol]
            c = _random_walk(o, volatility=0.002)
            h = max(o, c) * (1 + random.uniform(0, 0.001))
            l = min(o, c) * (1 - random.uniform(0, 0.001))
            prices[symbol] = c

            bar = NormalizedBar(
                time=datetime.now(timezone.utc),
                symbol=symbol,
                asset_class=asset_class,
                timeframe=Timeframe.ONE_MIN,
                source=DataSource.MOCK,
                open=Decimal(str(round(o, 4))),
                high=Decimal(str(round(h, 4))),
                low=Decimal(str(round(l, 4))),
                close=Decimal(str(round(c, 4))),
                volume=Decimal(str(random.randint(1000, 100000))),
                trade_count=random.randint(10, 500),
                vwap=Decimal(str(round((o + h + l + c) / 4, 4))),
            )
            await on_bar(bar)

        await asyncio.sleep(interval_s)

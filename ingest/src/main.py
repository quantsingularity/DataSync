"""
DataSync - Ingest Service Main
Orchestrates all WebSocket feed connections and historical data fetching.
Writes to TimescaleDB and publishes to Kafka.
"""

import asyncio
import logging
import os

from kafka_producer.src.producer import flush_and_stop, publish_bar, publish_tick

from normalize.src.models import AssetClass, NormalizedBar, NormalizedTick
from store.src.cache import cache_price, push_tick_to_stream
from store.src.timescale import close_pool, get_pool, write_bar, write_tick

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("datasync.ingest")

USE_MOCK = os.getenv("USE_MOCK_FEEDS", "true").lower() == "true"
EQUITY_SYMS = os.getenv("DEFAULT_EQUITY_SYMBOLS", "AAPL,MSFT,GOOGL,TSLA,SPY").split(",")
CRYPTO_SYMS = os.getenv("DEFAULT_CRYPTO_SYMBOLS", "BTC/USD,ETH/USD,SOL/USD").split(",")

_stop = asyncio.Event()


async def on_tick(tick: NormalizedTick) -> None:
    """Handle incoming normalized tick: publish to Kafka + write to DB + cache."""
    try:
        await asyncio.gather(
            publish_tick(tick),
            write_tick(tick),
            cache_price(
                tick.symbol,
                {
                    "price": float(tick.price),
                    "bid": float(tick.bid or 0),
                    "ask": float(tick.ask or 0),
                    "time": tick.time.isoformat(),
                    "source": str(tick.source),
                },
            ),
            push_tick_to_stream(
                tick.symbol,
                {
                    "price": str(tick.price),
                    "symbol": tick.symbol,
                    "time": tick.time.isoformat(),
                },
            ),
        )
    except Exception as e:
        logger.error(f"Error handling tick [{tick.symbol}]: {e}")


async def on_bar(bar: NormalizedBar) -> None:
    """Handle incoming normalized bar: publish to Kafka + write to DB."""
    try:
        await asyncio.gather(
            publish_bar(bar),
            write_bar(bar),
        )
    except Exception as e:
        logger.error(f"Error handling bar [{bar.symbol}]: {e}")


async def run_mock_feeds() -> None:
    from ingest.src.mock_feed import (
        run_mock_bar_feed,
        run_mock_crypto_feed,
        run_mock_equity_feed,
    )

    logger.info("Starting mock feeds (USE_MOCK_FEEDS=true)")
    await asyncio.gather(
        run_mock_equity_feed(EQUITY_SYMS, on_tick, _stop),
        run_mock_crypto_feed(CRYPTO_SYMS, on_tick, _stop),
        run_mock_bar_feed(EQUITY_SYMS, AssetClass.EQUITY, on_bar, _stop, interval_s=60),
        run_mock_bar_feed(CRYPTO_SYMS, AssetClass.CRYPTO, on_bar, _stop, interval_s=60),
    )


async def run_live_feeds() -> None:
    from ingest.src.alpaca_client import AlpacaStreamClient
    from ingest.src.historical import fetch_crypto_bars, fetch_equity_bars
    from ingest.src.polygon_client import PolygonStreamClient

    logger.info("Starting live feeds")

    # Run historical backfill on startup
    await asyncio.gather(
        fetch_equity_bars(EQUITY_SYMS),
        fetch_crypto_bars(CRYPTO_SYMS),
    )

    clients = [
        AlpacaStreamClient(EQUITY_SYMS, AssetClass.EQUITY, on_tick, on_bar, _stop),
        AlpacaStreamClient(CRYPTO_SYMS, AssetClass.CRYPTO, on_tick, on_bar, _stop),
        PolygonStreamClient(EQUITY_SYMS, on_tick, on_bar, _stop),
    ]
    await asyncio.gather(*[c.run() for c in clients])


async def main() -> None:
    logger.info("DataSync Ingest Service starting...")
    await get_pool()

    try:
        if USE_MOCK:
            await run_mock_feeds()
        else:
            await run_live_feeds()
    except asyncio.CancelledError:
        logger.info("Ingest service cancelled")
    finally:
        await flush_and_stop()
        await close_pool()
        logger.info("Ingest service stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted")

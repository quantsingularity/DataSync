"""
DataSync - Alpaca WebSocket Client
Connects to Alpaca's live data streams for equities and crypto.
Falls back gracefully when API keys are not set.
"""

import asyncio
import json
import logging
import os
from typing import Callable, List

import websockets

from normalize.src.alpaca_adapter import (
    alpaca_bar_to_bar,
    alpaca_quote_to_tick,
    alpaca_trade_to_tick,
)
from normalize.src.models import AssetClass, Timeframe

logger = logging.getLogger("datasync.ingest.alpaca")

ALPACA_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY", "")
EQUITY_STREAM_URL = os.getenv(
    "ALPACA_STREAM_URL", "wss://stream.data.alpaca.markets/v2"
)
CRYPTO_STREAM_URL = os.getenv(
    "ALPACA_CRYPTO_STREAM_URL", "wss://stream.data.alpaca.markets/v1beta3/crypto/us"
)


class AlpacaStreamClient:
    """
    Alpaca market data WebSocket client.
    Subscribes to trades, quotes, and bars for a list of symbols.
    All incoming messages are normalized and forwarded to the on_tick/on_bar callbacks.
    """

    def __init__(
        self,
        symbols: List[str],
        asset_class: AssetClass,
        on_tick: Callable,
        on_bar: Callable,
        stop_event: asyncio.Event,
    ):
        self.symbols = symbols
        self.asset_class = asset_class
        self.on_tick = on_tick
        self.on_bar = on_bar
        self.stop_event = stop_event

        if asset_class == AssetClass.CRYPTO:
            feed = "iex"
            self.stream_url = f"{CRYPTO_STREAM_URL}"
        else:
            feed = "iex"
            self.stream_url = f"{EQUITY_STREAM_URL}/{feed}"

    async def run(self) -> None:
        if not ALPACA_KEY or not ALPACA_SECRET:
            logger.warning("Alpaca API keys not set - skipping live feed")
            return

        while not self.stop_event.is_set():
            try:
                await self._connect()
            except Exception as e:
                logger.error(f"Alpaca stream error: {e}")
                if not self.stop_event.is_set():
                    logger.info("Reconnecting in 5s...")
                    await asyncio.sleep(5)

    async def _connect(self) -> None:
        logger.info(f"Connecting to Alpaca stream: {self.stream_url}")
        async with websockets.connect(self.stream_url, ping_interval=20) as ws:
            # Auth
            await ws.send(
                json.dumps(
                    {
                        "action": "auth",
                        "key": ALPACA_KEY,
                        "secret": ALPACA_SECRET,
                    }
                )
            )

            auth_resp = json.loads(await ws.recv())
            if not any(
                m.get("T") == "success"
                for m in (auth_resp if isinstance(auth_resp, list) else [auth_resp])
            ):
                raise ConnectionError(f"Alpaca auth failed: {auth_resp}")

            logger.info("Alpaca authenticated")

            # Subscribe
            await ws.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "trades": self.symbols,
                        "quotes": self.symbols,
                        "bars": self.symbols,
                    }
                )
            )

            # Message loop
            async for raw in ws:
                if self.stop_event.is_set():
                    break
                try:
                    messages = json.loads(raw)
                    if not isinstance(messages, list):
                        messages = [messages]
                    for msg in messages:
                        await self._handle(msg)
                except Exception as e:
                    logger.warning(f"Error handling Alpaca message: {e}")

    async def _handle(self, msg: dict) -> None:
        t = msg.get("T", "")
        if t == "t":
            tick = alpaca_trade_to_tick(msg, self.asset_class)
            await self.on_tick(tick)
        elif t == "q":
            tick = alpaca_quote_to_tick(msg, self.asset_class)
            await self.on_tick(tick)
        elif t in ("b", "cb"):
            bar = alpaca_bar_to_bar(msg, self.asset_class, Timeframe.ONE_MIN)
            await self.on_bar(bar)

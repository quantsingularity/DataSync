"""
DataSync - Polygon.io WebSocket Client
Connects to Polygon's stocks WebSocket feed for equities.
"""

import asyncio
import json
import logging
import os
from typing import Callable, List

import websockets

from normalize.src.models import Timeframe
from normalize.src.polygon_adapter import (
    polygon_agg_to_bar,
    polygon_quote_to_tick,
    polygon_trade_to_tick,
)

logger = logging.getLogger("datasync.ingest.polygon")

POLYGON_KEY = os.getenv("POLYGON_API_KEY", "")
POLYGON_STREAM_URL = os.getenv("POLYGON_STREAM_URL", "wss://socket.polygon.io/stocks")


class PolygonStreamClient:
    """
    Polygon.io WebSocket client for equities.
    Subscribes to trades (T), quotes (Q), and per-second aggregates (A).
    """

    def __init__(
        self,
        symbols: List[str],
        on_tick: Callable,
        on_bar: Callable,
        stop_event: asyncio.Event,
    ):
        self.symbols = symbols
        self.on_tick = on_tick
        self.on_bar = on_bar
        self.stop_event = stop_event

    async def run(self) -> None:
        if not POLYGON_KEY:
            logger.warning("Polygon API key not set - skipping live feed")
            return

        while not self.stop_event.is_set():
            try:
                await self._connect()
            except Exception as e:
                logger.error(f"Polygon stream error: {e}")
                if not self.stop_event.is_set():
                    logger.info("Reconnecting in 5s...")
                    await asyncio.sleep(5)

    async def _connect(self) -> None:
        logger.info(f"Connecting to Polygon stream: {POLYGON_STREAM_URL}")
        async with websockets.connect(POLYGON_STREAM_URL, ping_interval=20) as ws:
            # Auth
            await ws.send(json.dumps({"action": "auth", "params": POLYGON_KEY}))
            await ws.recv()  # connected msg
            await ws.recv()  # auth status

            # Subscribe: T.* (trades), Q.* (quotes), A.* (second aggregates)
            subs = []
            for sym in self.symbols:
                subs.extend([f"T.{sym}", f"Q.{sym}", f"AM.{sym}"])

            await ws.send(json.dumps({"action": "subscribe", "params": ",".join(subs)}))
            logger.info(f"Polygon subscribed to {len(self.symbols)} symbols")

            async for raw in ws:
                if self.stop_event.is_set():
                    break
                try:
                    messages = json.loads(raw)
                    for msg in (messages if isinstance(messages, list) else [messages]):
                        await self._handle(msg)
                except Exception as e:
                    logger.warning(f"Error handling Polygon message: {e}")

    async def _handle(self, msg: dict) -> None:
        ev = msg.get("ev", "")
        if ev == "T":
            await self.on_tick(polygon_trade_to_tick(msg))
        elif ev == "Q":
            await self.on_tick(polygon_quote_to_tick(msg))
        elif ev in ("A", "AM"):
            tf = Timeframe.ONE_MIN if ev == "AM" else Timeframe.ONE_MIN
            await self.on_bar(polygon_agg_to_bar(msg, tf))

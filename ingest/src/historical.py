"""
DataSync - Historical Data Fetcher
Pulls OHLCV bars from Alpaca REST API and stores them in TimescaleDB.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import httpx

from normalize.src.alpaca_adapter import alpaca_bar_to_bar
from normalize.src.models import AssetClass, Timeframe
from store.src.timescale import write_bars_batch

logger = logging.getLogger("datasync.ingest.historical")

ALPACA_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY", "")
ALPACA_BASE = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
DATA_BASE = "https://data.alpaca.markets"
LOOKBACK_DAYS = int(os.getenv("HISTORICAL_LOOKBACK_DAYS", "365"))

TIMEFRAME_MAP = {
    Timeframe.ONE_MIN: "1Min",
    Timeframe.FIVE_MIN: "5Min",
    Timeframe.FIFTEEN_MIN: "15Min",
    Timeframe.ONE_HOUR: "1Hour",
    Timeframe.ONE_DAY: "1Day",
}


async def fetch_equity_bars(
    symbols: List[str],
    timeframe: Timeframe = Timeframe.ONE_DAY,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> int:
    """
    Fetch historical equity bars from Alpaca and write to TimescaleDB.
    Returns total bars written.
    """
    if not ALPACA_KEY or not ALPACA_SECRET:
        logger.warning("Alpaca keys not set - skipping historical fetch")
        return 0

    start = start or (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS))
    end = end or datetime.now(timezone.utc)
    tf = TIMEFRAME_MAP.get(timeframe, "1Day")

    total = 0
    headers = {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}

    async with httpx.AsyncClient(timeout=60) as client:
        for symbol in symbols:
            bars = []
            cursor = None

            while True:
                params = {
                    "symbols": symbol,
                    "timeframe": tf,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "limit": 1000,
                    "feed": "iex",
                }
                if cursor:
                    params["page_token"] = cursor

                try:
                    resp = await client.get(
                        f"{DATA_BASE}/v2/stocks/bars",
                        headers=headers,
                        params=params,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    raw = data.get("bars", {}).get(symbol, [])
                    cursor = data.get("next_page_token")

                    for b in raw:
                        b["S"] = symbol
                        bars.append(alpaca_bar_to_bar(b, AssetClass.EQUITY, timeframe))

                    if not cursor:
                        break
                except httpx.HTTPStatusError as e:
                    logger.error(f"Alpaca API error for {symbol}: {e}")
                    break

            if bars:
                written = await write_bars_batch(bars)
                total += written
                logger.info(f"Stored {written} {tf} bars for {symbol}")

    return total


async def fetch_crypto_bars(
    symbols: List[str],
    timeframe: Timeframe = Timeframe.ONE_DAY,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> int:
    """Fetch historical crypto bars from Alpaca."""
    if not ALPACA_KEY or not ALPACA_SECRET:
        return 0

    start = start or (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS))
    end = end or datetime.now(timezone.utc)
    tf = TIMEFRAME_MAP.get(timeframe, "1Day")

    total = 0
    headers = {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}

    async with httpx.AsyncClient(timeout=60) as client:
        for symbol in symbols:
            bars = []
            symbol.replace("/", "")
            cursor = None

            while True:
                params = {
                    "symbols": symbol,
                    "timeframe": tf,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "limit": 1000,
                }
                if cursor:
                    params["page_token"] = cursor

                try:
                    resp = await client.get(
                        f"{DATA_BASE}/v1beta3/crypto/us/bars",
                        headers=headers,
                        params=params,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    raw = data.get("bars", {}).get(symbol, [])
                    cursor = data.get("next_page_token")

                    for b in raw:
                        b["S"] = symbol
                        bars.append(alpaca_bar_to_bar(b, AssetClass.CRYPTO, timeframe))

                    if not cursor:
                        break
                except Exception as e:
                    logger.error(f"Alpaca crypto API error for {symbol}: {e}")
                    break

            if bars:
                written = await write_bars_batch(bars)
                total += written
                logger.info(f"Stored {written} {tf} crypto bars for {symbol}")

    return total

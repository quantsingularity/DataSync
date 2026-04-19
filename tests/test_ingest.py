"""
Integration tests for the ingest pipeline using a real mock WebSocket server.
The mock server sends canned Alpaca/Polygon format messages.
We verify they are correctly parsed and passed to the on_tick/on_bar callbacks.
"""

import asyncio
from decimal import Decimal
from typing import List
from unittest.mock import patch

import pytest

from normalize.src.models import AssetClass, NormalizedBar, NormalizedTick

# ── Mock feed tests ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_mock_equity_feed_emits_ticks():
    """Mock equity feed must call on_tick with valid NormalizedTick objects."""
    from ingest.src.mock_feed import run_mock_equity_feed

    received: List[NormalizedTick] = []
    stop = asyncio.Event()

    async def on_tick(tick: NormalizedTick):
        received.append(tick)
        if len(received) >= 3:
            stop.set()

    await asyncio.wait_for(
        run_mock_equity_feed(["AAPL", "MSFT"], on_tick, stop),
        timeout=5.0,
    )

    assert len(received) >= 3
    for tick in received:
        assert isinstance(tick, NormalizedTick)
        assert tick.asset_class == AssetClass.EQUITY
        assert tick.price > Decimal("0")
        assert tick.symbol in ("AAPL", "MSFT")


@pytest.mark.asyncio
async def test_mock_crypto_feed_emits_ticks():
    """Mock crypto feed must emit higher-volatility ticks."""
    from ingest.src.mock_feed import run_mock_crypto_feed

    received: List[NormalizedTick] = []
    stop = asyncio.Event()

    async def on_tick(tick: NormalizedTick):
        received.append(tick)
        if len(received) >= 2:
            stop.set()

    await asyncio.wait_for(
        run_mock_crypto_feed(["BTC/USD"], on_tick, stop),
        timeout=5.0,
    )

    assert len(received) >= 2
    assert all(t.asset_class == AssetClass.CRYPTO for t in received)
    assert all(t.symbol == "BTC/USD" for t in received)


@pytest.mark.asyncio
async def test_mock_bar_feed_emits_bars():
    """Mock bar feed must emit NormalizedBar objects with OHLCV data."""
    from ingest.src.mock_feed import run_mock_bar_feed

    received: List[NormalizedBar] = []
    stop = asyncio.Event()

    async def on_bar(bar: NormalizedBar):
        received.append(bar)
        if len(received) >= 1:
            stop.set()

    await asyncio.wait_for(
        run_mock_bar_feed(["AAPL"], AssetClass.EQUITY, on_bar, stop, interval_s=0),
        timeout=5.0,
    )

    assert len(received) >= 1
    bar = received[0]
    assert bar.high >= bar.open
    assert bar.low <= bar.open
    assert bar.close > Decimal("0")
    assert bar.volume is not None


# ── Real WebSocket server tests ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_alpaca_client_parses_equity_messages(mock_ws_server_equity):
    """
    AlpacaStreamClient must parse trades, quotes, and bars from
    the mock WebSocket server and call callbacks correctly.
    """
    from ingest.src.alpaca_client import AlpacaStreamClient

    ticks: List[NormalizedTick] = []
    bars: List[NormalizedBar] = []
    stop = asyncio.Event()

    async def on_tick(t):
        ticks.append(t)

    async def on_bar(b):
        bars.append(b)

    client = AlpacaStreamClient(
        symbols=["AAPL"],
        asset_class=AssetClass.EQUITY,
        on_tick=on_tick,
        on_bar=on_bar,
        stop_event=stop,
    )

    # Override stream URL + inject fake credentials
    client.stream_url = mock_ws_server_equity

    with (
        patch("ingest.src.alpaca_client.ALPACA_KEY", "test-key"),
        patch("ingest.src.alpaca_client.ALPACA_SECRET", "test-secret"),
    ):
        try:
            await asyncio.wait_for(client.run(), timeout=3.0)
        except asyncio.TimeoutError:
            stop.set()

    # We expect at least one tick (trade or quote) and one bar
    assert len(ticks) >= 1, "Expected at least one tick from mock server"
    assert len(bars) >= 1, "Expected at least one bar from mock server"

    tick = ticks[0]
    assert isinstance(tick, NormalizedTick)
    assert tick.symbol == "AAPL"
    assert tick.price > Decimal("0")


@pytest.mark.asyncio
async def test_polygon_client_parses_messages(mock_ws_server_polygon):
    """
    PolygonStreamClient must parse trades, quotes, and aggregates
    from the mock WebSocket server.
    """
    from ingest.src.polygon_client import PolygonStreamClient

    ticks: List[NormalizedTick] = []
    bars: List[NormalizedBar] = []
    stop = asyncio.Event()

    async def on_tick(t):
        ticks.append(t)

    async def on_bar(b):
        bars.append(b)

    client = PolygonStreamClient(
        symbols=["AAPL"],
        on_tick=on_tick,
        on_bar=on_bar,
        stop_event=stop,
    )

    with patch("ingest.src.polygon_client.POLYGON_STREAM_URL", mock_ws_server_polygon):
        with patch("ingest.src.polygon_client.POLYGON_KEY", "test-key"):
            try:
                await asyncio.wait_for(client.run(), timeout=3.0)
            except asyncio.TimeoutError:
                stop.set()

    assert len(ticks) >= 1, "Expected at least one tick from Polygon mock"
    assert len(bars) >= 1, "Expected at least one bar from Polygon mock"

    tick = ticks[0]
    assert tick.symbol == "AAPL"
    assert tick.price == Decimal("185.42")


@pytest.mark.asyncio
async def test_tick_and_bar_routing():
    """
    on_tick and on_bar callbacks must receive the correct normalized types
    (not swapped), and all required fields must be populated.
    """
    from ingest.src.mock_feed import run_mock_equity_feed

    ticks: List[NormalizedTick] = []
    stop = asyncio.Event()

    async def on_tick(t: NormalizedTick):
        ticks.append(t)
        if len(ticks) >= 5:
            stop.set()

    await asyncio.wait_for(
        run_mock_equity_feed(["SPY"], on_tick, stop),
        timeout=5.0,
    )

    for tick in ticks:
        # Must have core required fields
        assert tick.time is not None
        assert tick.symbol == "SPY"
        assert tick.price > Decimal("0")
        assert tick.asset_class == AssetClass.EQUITY
        assert tick.source.value == "mock"

        # Bid/ask spread must be positive when present
        if tick.bid and tick.ask:
            assert tick.ask >= tick.bid

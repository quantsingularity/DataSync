"""
DataSync Test Configuration
============================
Provides:
  - mock_ws_server  : a real asyncio WebSocket server serving canned market data
  - mock_kafka      : async mock that captures published messages
  - mock_redis      : in-process dict-based cache mock
  - api_client      : httpx AsyncClient against the FastAPI app
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

import pytest
import pytest_asyncio
import websockets

# Make all datasync modules importable
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# ── Mock Kafka ────────────────────────────────────────────────────────────────


class MockKafkaProducer:
    """Captures published messages for assertion."""

    def __init__(self):
        self.published = []

    async def send(self, topic, key=None, value=None):
        self.published.append({"topic": topic, "key": key, "value": value})

    async def flush(self):
        pass

    async def stop(self):
        pass


@pytest.fixture
def mock_kafka():
    producer = MockKafkaProducer()
    return producer


# ── Mock Redis ────────────────────────────────────────────────────────────────


class MockRedis:
    """In-memory dict-backed Redis mock."""

    def __init__(self):
        self._store = {}
        self._streams = {}

    async def setex(self, key, ttl, value):
        self._store[key] = value

    async def get(self, key):
        return self._store.get(key)

    async def keys(self, pattern="*"):
        return list(self._store.keys())

    async def delete(self, *keys):
        for k in keys:
            self._store.pop(k, None)

    async def xadd(self, stream, data, maxlen=None, approximate=None):
        self._streams.setdefault(stream, []).append(data)

    async def incr(self, key):
        self._store[key] = str(int(self._store.get(key, "0")) + 1)
        return int(self._store[key])

    async def expire(self, key, ttl):
        pass

    async def ping(self):
        return True


@pytest.fixture
def mock_redis():
    return MockRedis()


# ── Mock WebSocket server ─────────────────────────────────────────────────────

MOCK_EQUITY_MESSAGES = [
    [{"T": "success", "msg": "connected"}],
    [{"T": "success", "msg": "authenticated"}],
    [{"T": "subscription", "trades": ["AAPL"], "quotes": ["AAPL"], "bars": ["AAPL"]}],
    # Trade
    [
        {
            "T": "t",
            "S": "AAPL",
            "p": 185.42,
            "s": 100,
            "t": "2024-01-15T14:30:00Z",
            "x": "C",
            "z": "C",
            "c": ["@", "I"],
            "i": 1,
        }
    ],
    # Quote
    [
        {
            "T": "q",
            "S": "AAPL",
            "bp": 185.40,
            "bs": 200,
            "ap": 185.44,
            "as": 150,
            "t": "2024-01-15T14:30:01Z",
            "ax": "C",
            "bx": "C",
            "c": ["R"],
        }
    ],
    # Bar
    [
        {
            "T": "b",
            "S": "AAPL",
            "o": 185.00,
            "h": 185.80,
            "l": 184.90,
            "c": 185.42,
            "v": 52000,
            "t": "2024-01-15T14:30:00Z",
            "n": 340,
            "vw": 185.35,
        }
    ],
]

MOCK_POLYGON_MESSAGES = [
    [{"ev": "status", "status": "connected"}],
    [{"ev": "status", "status": "auth_success"}],
    [{"ev": "status", "status": "success"}],
    # Trade
    [
        {
            "ev": "T",
            "sym": "AAPL",
            "p": 185.42,
            "s": 100,
            "t": 1705329000000,
            "x": 4,
            "z": 3,
            "c": [14, 41],
        }
    ],
    # Quote
    [
        {
            "ev": "Q",
            "sym": "AAPL",
            "bp": 185.40,
            "bs": 200,
            "ap": 185.44,
            "as": 150,
            "t": 1705329001000,
            "bx": 4,
            "ax": 12,
            "c": [1],
        }
    ],
    # Aggregate
    [
        {
            "ev": "AM",
            "sym": "AAPL",
            "o": 185.00,
            "h": 185.80,
            "l": 184.90,
            "c": 185.42,
            "v": 52000,
            "av": 185.35,
            "s": 1705329000000,
            "e": 1705329060000,
        }
    ],
]


async def _ws_handler(websocket, path=None, messages=None):
    """Send pre-canned messages then wait for close."""
    for msg in messages or []:
        await websocket.send(json.dumps(msg))
        await asyncio.sleep(0.05)
    # Keep connection open until client disconnects
    try:
        await websocket.wait_closed()
    except Exception:
        pass


@pytest_asyncio.fixture
async def mock_ws_server_equity():
    """Serve mock Alpaca-style equity WebSocket messages on a random local port."""
    msgs = MOCK_EQUITY_MESSAGES

    async def handler(ws):
        await _ws_handler(ws, messages=msgs)

    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    yield f"ws://127.0.0.1:{port}"
    server.close()
    await server.wait_closed()


@pytest_asyncio.fixture
async def mock_ws_server_polygon():
    """Serve mock Polygon-style WebSocket messages on a random local port."""
    msgs = MOCK_POLYGON_MESSAGES

    async def handler(ws):
        await _ws_handler(ws, messages=msgs)

    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    yield f"ws://127.0.0.1:{port}"
    server.close()
    await server.wait_closed()


# ── API client ────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def api_client():
    """httpx AsyncClient pointing at the DataSync API with mocked dependencies."""
    from unittest.mock import AsyncMock, patch

    from httpx import ASGITransport, AsyncClient

    async def fake_query_bars(*a, **kw):
        return [
            {
                "time": "2024-01-15T14:30:00+00:00",
                "symbol": "AAPL",
                "asset_class": "equity",
                "timeframe": "1d",
                "source": "mock",
                "open": 183.5,
                "high": 186.2,
                "low": 183.1,
                "close": 185.4,
                "volume": 52000000,
                "trade_count": 340000,
                "vwap": 185.1,
            }
        ]

    async def fake_query_ticks(*a, **kw):
        return [
            {
                "time": "2024-01-15T14:30:00+00:00",
                "symbol": "AAPL",
                "asset_class": "equity",
                "source": "mock",
                "price": 185.42,
                "size": 100,
                "bid": 185.40,
                "ask": 185.44,
                "conditions": [],
                "exchange": "C",
            }
        ]

    async def fake_latest_price(*a, **kw):
        return {
            "time": "2024-01-15T14:30:00+00:00",
            "symbol": "AAPL",
            "price": 185.42,
            "bid": 185.40,
            "ask": 185.44,
            "source": "mock",
        }

    async def fake_get_cached_bars(*a, **kw):
        return None

    async def fake_get_cached_price(*a, **kw):
        return None

    async def fake_cache_bars(*a, **kw):
        pass

    async def fake_cache_price(*a, **kw):
        pass

    async def fake_get_cached_subs(*a, **kw):
        return None

    async def fake_cache_subs(*a, **kw):
        pass

    async def fake_list_subs(active_only=True):
        return [
            {
                "id": 1,
                "service_name": "test",
                "symbol": "AAPL",
                "asset_class": "equity",
                "is_active": True,
                "subscribed_at": datetime.now(timezone.utc),
            }
        ]

    async def fake_create_sub(body):
        return {
            "id": 2,
            "service_name": body.service_name,
            "symbol": body.symbol,
            "asset_class": str(body.asset_class),
            "is_active": True,
            "subscribed_at": datetime.now(timezone.utc),
        }

    async def fake_delete_sub(sid):
        pass

    async def fake_init_pool():
        pass

    async def fake_close_pool():
        pass

    with (
        patch("api.src.main.query_bars", fake_query_bars),
        patch("api.src.main.query_ticks", fake_query_ticks),
        patch("api.src.main.query_latest_price", fake_latest_price),
        patch("api.src.main.get_cached_bars", fake_get_cached_bars),
        patch("api.src.main.get_cached_price", fake_get_cached_price),
        patch("api.src.main.cache_bars", fake_cache_bars),
        patch("api.src.main.cache_price", fake_cache_price),
        patch("api.src.main.get_cached_subscriptions", fake_get_cached_subs),
        patch("api.src.main.cache_subscriptions", fake_cache_subs),
        patch("api.src.main.list_subscriptions", fake_list_subs),
        patch("api.src.main.create_subscription", fake_create_sub),
        patch("api.src.main.delete_subscription", fake_delete_sub),
        patch(
            "api.src.main.get_pool",
            AsyncMock(
                return_value=AsyncMock(
                    fetchval=AsyncMock(return_value=1),
                    fetch=AsyncMock(return_value=[]),
                )
            ),
        ),
        patch("api.src.main.close_pool", fake_close_pool),
    ):
        from api.src.main import app

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client

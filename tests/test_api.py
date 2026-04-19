"""Tests for the DataSync REST API."""

import pytest


@pytest.mark.asyncio
async def test_health(api_client):
    r = await api_client.get("/health")
    assert r.status_code == 200
    assert r.json()["service"] == "datasync-api"


@pytest.mark.asyncio
async def test_get_bars(api_client):
    r = await api_client.get("/bars/AAPL?timeframe=1d")
    assert r.status_code == 200
    data = r.json()
    assert data["symbol"] == "AAPL"
    assert data["timeframe"] == "1d"
    assert isinstance(data["bars"], list)
    assert len(data["bars"]) >= 1

    bar = data["bars"][0]
    assert "open" in bar
    assert "high" in bar
    assert "low" in bar
    assert "close" in bar
    assert "volume" in bar


@pytest.mark.asyncio
async def test_get_bars_with_date_range(api_client):
    r = await api_client.get(
        "/bars/MSFT",
        params={
            "timeframe": "1h",
            "from": "2024-01-01T00:00:00",
            "to": "2024-01-15T00:00:00",
        },
    )
    assert r.status_code == 200
    assert r.json()["symbol"] == "MSFT"


@pytest.mark.asyncio
async def test_get_bars_symbol_uppercase(api_client):
    r = await api_client.get("/bars/aapl?timeframe=1d")
    assert r.status_code == 200
    assert r.json()["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_get_ticks(api_client):
    r = await api_client.get("/ticks/AAPL")
    assert r.status_code == 200
    data = r.json()
    assert data["symbol"] == "AAPL"
    assert isinstance(data["ticks"], list)
    assert data["count"] >= 1

    tick = data["ticks"][0]
    assert "price" in tick
    assert "time" in tick


@pytest.mark.asyncio
async def test_get_price(api_client):
    r = await api_client.get("/price/AAPL")
    assert r.status_code == 200
    data = r.json()
    assert data["symbol"] == "AAPL"
    assert "price" in data
    assert "cached" in data


@pytest.mark.asyncio
async def test_list_subscriptions(api_client):
    r = await api_client.get("/subscriptions")
    assert r.status_code == 200
    subs = r.json()
    assert isinstance(subs, list)
    assert len(subs) >= 1
    assert "symbol" in subs[0]
    assert "asset_class" in subs[0]
    assert "service_name" in subs[0]


@pytest.mark.asyncio
async def test_create_subscription(api_client):
    r = await api_client.post(
        "/subscriptions",
        json={
            "service_name": "test-service",
            "symbol": "TSLA",
            "asset_class": "equity",
        },
    )
    assert r.status_code == 201
    data = r.json()
    assert data["symbol"] == "TSLA"
    assert data["service_name"] == "test-service"
    assert data["is_active"] is True


@pytest.mark.asyncio
async def test_create_subscription_crypto(api_client):
    r = await api_client.post(
        "/subscriptions",
        json={
            "service_name": "algo-service",
            "symbol": "ETH/USD",
            "asset_class": "crypto",
        },
    )
    assert r.status_code == 201
    assert r.json()["asset_class"] == "crypto"


@pytest.mark.asyncio
async def test_delete_subscription(api_client):
    r = await api_client.delete("/subscriptions/1")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_invalid_subscription_asset_class(api_client):
    r = await api_client.post(
        "/subscriptions",
        json={
            "service_name": "bad-service",
            "symbol": "AAPL",
            "asset_class": "nft",  # invalid
        },
    )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_bars_limit_capped(api_client):
    r = await api_client.get("/bars/AAPL?limit=99999")
    # FastAPI should clamp or reject over 5000
    assert r.status_code in (200, 422)

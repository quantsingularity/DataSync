"""Tests for normalization models and feed adapters."""

from datetime import datetime, timezone
from decimal import Decimal

from normalize.src.alpaca_adapter import (
    alpaca_bar_to_bar,
    alpaca_quote_to_tick,
    alpaca_trade_to_tick,
)
from normalize.src.models import (
    AltDataType,
    AssetClass,
    DataSource,
    NormalizedAltData,
    NormalizedBar,
    NormalizedTick,
    Timeframe,
)
from normalize.src.polygon_adapter import (
    polygon_agg_to_bar,
    polygon_quote_to_tick,
    polygon_trade_to_tick,
)

# ── NormalizedTick ────────────────────────────────────────────────────────────


def test_tick_creation():
    tick = NormalizedTick(
        symbol="aapl",  # should be uppercased
        asset_class=AssetClass.EQUITY,
        source=DataSource.MOCK,
        price=Decimal("185.42"),
        size=Decimal("100"),
    )
    assert tick.symbol == "AAPL"
    assert tick.price == Decimal("185.42")
    assert tick.asset_class == AssetClass.EQUITY
    assert tick.extra == {}
    assert tick.conditions == []


def test_tick_timezone_coercion():
    tick = NormalizedTick(
        symbol="MSFT",
        asset_class=AssetClass.EQUITY,
        source=DataSource.ALPACA,
        price=Decimal("420.00"),
        time="2024-01-15T14:30:00Z",  # string input
    )
    assert tick.time.tzinfo is not None
    assert tick.time.tzinfo.utcoffset(None).seconds == 0


def test_tick_kafka_key():
    tick = NormalizedTick(
        symbol="BTC/USD",
        asset_class=AssetClass.CRYPTO,
        source=DataSource.MOCK,
        price=Decimal("68000"),
    )
    assert tick.kafka_key() == b"BTC/USD"


def test_tick_to_kafka_value():
    tick = NormalizedTick(
        symbol="AAPL",
        asset_class=AssetClass.EQUITY,
        source=DataSource.MOCK,
        price=Decimal("185.42"),
    )
    val = tick.to_kafka_value()
    assert val["symbol"] == "AAPL"
    assert val["event_type"] == "tick"
    assert "price" in val


# ── NormalizedBar ─────────────────────────────────────────────────────────────


def test_bar_creation():
    bar = NormalizedBar(
        time=datetime.now(timezone.utc),
        symbol="spy",
        asset_class=AssetClass.EQUITY,
        timeframe=Timeframe.ONE_DAY,
        source=DataSource.ALPACA,
        open=Decimal("524.50"),
        high=Decimal("526.80"),
        low=Decimal("523.90"),
        close=Decimal("525.70"),
        volume=Decimal("85000000"),
    )
    assert bar.symbol == "SPY"
    assert bar.high >= bar.low
    assert bar.timeframe == Timeframe.ONE_DAY


def test_bar_symbol_uppercased():
    bar = NormalizedBar(
        time=datetime.now(timezone.utc),
        symbol="tsla",
        asset_class=AssetClass.EQUITY,
        timeframe=Timeframe.ONE_HOUR,
        source=DataSource.MOCK,
        open=Decimal("200"),
        high=Decimal("205"),
        low=Decimal("199"),
        close=Decimal("202"),
    )
    assert bar.symbol == "TSLA"


# ── NormalizedAltData ─────────────────────────────────────────────────────────


def test_alt_data_sentiment():
    alt = NormalizedAltData(
        data_type=AltDataType.NEWS_SENTIMENT,
        source="newsapi",
        symbol="AAPL",
        score=0.72,
        volume=15.0,
        payload={"articles": 15, "top_headline": "Apple hits record"},
    )
    assert alt.data_type == AltDataType.NEWS_SENTIMENT
    assert alt.score == 0.72
    assert alt.symbol == "AAPL"


def test_alt_data_onchain():
    alt = NormalizedAltData(
        data_type=AltDataType.ONCHAIN,
        source="coingecko",
        symbol="BTC/USD",
        volume=45_000_000_000.0,
        payload={"market_cap": 1_300_000_000_000, "market_cap_rank": 1},
    )
    assert alt.data_type == AltDataType.ONCHAIN
    assert alt.score is None


# ── Alpaca Adapter ────────────────────────────────────────────────────────────


def test_alpaca_trade_adapter():
    msg = {
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
    tick = alpaca_trade_to_tick(msg, AssetClass.EQUITY)
    assert tick.symbol == "AAPL"
    assert tick.price == Decimal("185.42")
    assert tick.size == Decimal("100")
    assert tick.source == DataSource.ALPACA
    assert tick.asset_class == AssetClass.EQUITY
    assert "@" in tick.conditions


def test_alpaca_quote_adapter():
    msg = {
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
    tick = alpaca_quote_to_tick(msg, AssetClass.EQUITY)
    assert tick.bid == Decimal("185.40")
    assert tick.ask == Decimal("185.44")
    # Mid price should be between bid and ask
    assert tick.bid <= tick.price <= tick.ask


def test_alpaca_bar_adapter():
    msg = {
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
    bar = alpaca_bar_to_bar(msg, AssetClass.EQUITY, Timeframe.ONE_MIN)
    assert bar.open == Decimal("185.00")
    assert bar.high >= bar.low
    assert bar.volume == Decimal("52000")
    assert bar.vwap == Decimal("185.35")
    assert bar.source == DataSource.ALPACA


def test_alpaca_crypto_tick():
    msg = {
        "T": "t",
        "S": "BTC/USD",
        "p": 68423.50,
        "s": 0.025,
        "t": "2024-01-15T14:30:00Z",
    }
    tick = alpaca_trade_to_tick(msg, AssetClass.CRYPTO)
    assert tick.asset_class == AssetClass.CRYPTO
    assert tick.symbol == "BTC/USD"


# ── Polygon Adapter ───────────────────────────────────────────────────────────


def test_polygon_trade_adapter():
    msg = {
        "ev": "T",
        "sym": "AAPL",
        "p": 185.42,
        "s": 100,
        "t": 1705329000000,
        "x": 4,
        "z": 3,
        "c": [14, 41],
        "q": 9999,
    }
    tick = polygon_trade_to_tick(msg)
    assert tick.symbol == "AAPL"
    assert tick.price == Decimal("185.42")
    assert tick.source == DataSource.POLYGON
    assert tick.asset_class == AssetClass.EQUITY


def test_polygon_quote_adapter():
    msg = {
        "ev": "Q",
        "sym": "MSFT",
        "bp": 420.10,
        "bs": 300,
        "ap": 420.15,
        "as": 200,
        "t": 1705329001000,
        "bx": 4,
        "ax": 12,
        "c": [1],
    }
    tick = polygon_quote_to_tick(msg)
    assert tick.bid == Decimal("420.10")
    assert tick.ask == Decimal("420.15")
    assert tick.bid <= tick.price <= tick.ask


def test_polygon_agg_adapter():
    msg = {
        "ev": "AM",
        "sym": "SPY",
        "o": 524.50,
        "h": 526.80,
        "l": 523.90,
        "c": 525.70,
        "v": 3500000,
        "av": 525.10,
        "s": 1705329000000,
        "e": 1705329060000,
    }
    bar = polygon_agg_to_bar(msg, Timeframe.ONE_MIN)
    assert bar.open == Decimal("524.50")
    assert bar.close == Decimal("525.70")
    assert bar.volume == Decimal("3500000")
    assert bar.source == DataSource.POLYGON


def test_polygon_timestamp_conversion():
    msg = {
        "ev": "T",
        "sym": "GOOGL",
        "p": 175.00,
        "s": 50,
        "t": 1705329000000,
        "x": 4,
        "z": 3,
        "c": [],
    }
    tick = polygon_trade_to_tick(msg)
    assert tick.time.tzinfo is not None
    assert tick.time.year == 2024

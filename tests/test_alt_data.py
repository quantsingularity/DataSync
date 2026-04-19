"""Tests for the alternative data pipeline."""

import pytest

from normalize.src.models import AltDataType, NormalizedAltData


@pytest.mark.asyncio
async def test_fetch_news_sentiment_mock():
    """News sentiment fetch uses mock data when NEWS_API_KEY is not set."""
    from alt_data.src.main import fetch_news_sentiment

    results = await fetch_news_sentiment(["AAPL", "MSFT"])
    assert len(results) == 2

    for item in results:
        assert isinstance(item, NormalizedAltData)
        assert item.data_type == AltDataType.NEWS_SENTIMENT
        assert item.symbol in ("AAPL", "MSFT")
        assert -1.0 <= (item.score or 0) <= 1.0
        assert item.time.tzinfo is not None


@pytest.mark.asyncio
async def test_fetch_social_volume_mock():
    """Social volume returns mock data with volume and sentiment score."""
    from alt_data.src.main import fetch_social_volume

    results = await fetch_social_volume(["AAPL", "BTC/USD"])
    assert len(results) == 2

    for item in results:
        assert item.data_type == AltDataType.SOCIAL_VOLUME
        assert item.volume is not None
        assert item.volume > 0
        assert -1.0 <= (item.score or 0) <= 1.0
        assert "mentions" in item.payload


@pytest.mark.asyncio
async def test_simple_sentiment_positive():
    from alt_data.src.main import _simple_sentiment

    score = _simple_sentiment(
        "Apple surges to record high as earnings beat expectations"
    )
    assert score > 0


@pytest.mark.asyncio
async def test_simple_sentiment_negative():
    from alt_data.src.main import _simple_sentiment

    score = _simple_sentiment("Stock plunges and crashes on weak earnings loss")
    assert score < 0


@pytest.mark.asyncio
async def test_simple_sentiment_neutral():
    from alt_data.src.main import _simple_sentiment

    score = _simple_sentiment("The company reported quarterly results on Tuesday")
    assert score == 0.0


def test_alt_data_model_serialization():
    alt = NormalizedAltData(
        data_type=AltDataType.ONCHAIN,
        source="coingecko",
        symbol="BTC/USD",
        volume=45_000_000_000.0,
        payload={"market_cap": 1_300_000_000_000},
    )
    d = alt.model_dump(mode="json")
    assert d["data_type"] == "onchain"
    assert d["source"] == "coingecko"
    assert "payload" in d


def test_alt_data_null_score():
    """On-chain data has no sentiment score - score field must accept None."""
    alt = NormalizedAltData(
        data_type=AltDataType.ONCHAIN,
        source="coingecko",
        symbol="ETH/USD",
        score=None,
        payload={},
    )
    assert alt.score is None


@pytest.mark.asyncio
async def test_news_single_symbol():
    from alt_data.src.main import fetch_news_sentiment

    results = await fetch_news_sentiment(["TSLA"])
    assert len(results) == 1
    assert results[0].symbol == "TSLA"


@pytest.mark.asyncio
async def test_social_volume_includes_crypto():
    from alt_data.src.main import fetch_social_volume

    results = await fetch_social_volume(["SOL/USD"])
    assert len(results) == 1
    assert results[0].symbol == "SOL/USD"

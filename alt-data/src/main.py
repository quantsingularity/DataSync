"""
DataSync - Alternative Data Pipeline
Ingests: news sentiment (NewsAPI), social volume (mock), on-chain metrics (CoinGecko).
All data normalized to NormalizedAltData and published to Kafka + TimescaleDB.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import List

import httpx
from kafka_producer.src.producer import publish_alt_data

from normalize.src.models import AltDataType, NormalizedAltData
from store.src.timescale import write_alt_data

logger = logging.getLogger("datasync.alt-data")

NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
NEWS_INTERVAL_MINS = int(os.getenv("NEWS_FETCH_INTERVAL_MINUTES", "15"))
COINGECKO_BASE = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
COINGECKO_INTERVAL = int(os.getenv("COINGECKO_FETCH_INTERVAL_MINUTES", "5"))

EQUITY_SYMBOLS = os.getenv("DEFAULT_EQUITY_SYMBOLS", "AAPL,MSFT,GOOGL,TSLA").split(",")
CRYPTO_SYMBOLS = os.getenv("DEFAULT_CRYPTO_SYMBOLS", "BTC/USD,ETH/USD,SOL/USD").split(
    ","
)

# CoinGecko coin IDs for the crypto symbols
COINGECKO_COINS = {
    "BTC/USD": "bitcoin",
    "ETH/USD": "ethereum",
    "SOL/USD": "solana",
}

_stop = asyncio.Event()


# ── Simple sentiment scorer ───────────────────────────────────────────────────

POSITIVE_WORDS = {
    "surge",
    "gain",
    "rise",
    "soar",
    "rally",
    "beat",
    "profit",
    "growth",
    "bullish",
    "upgrade",
    "record",
    "strong",
    "positive",
}
NEGATIVE_WORDS = {
    "fall",
    "drop",
    "plunge",
    "crash",
    "loss",
    "miss",
    "weak",
    "bearish",
    "downgrade",
    "cut",
    "decline",
    "negative",
    "risk",
}


def _simple_sentiment(text: str) -> float:
    """Score text sentiment from -1.0 to +1.0 using keyword matching."""
    words = text.lower().split()
    pos = sum(1 for w in words if w in POSITIVE_WORDS)
    neg = sum(1 for w in words if w in NEGATIVE_WORDS)
    total = pos + neg
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 4)


# ── News sentiment ────────────────────────────────────────────────────────────


async def fetch_news_sentiment(symbols: List[str]) -> List[NormalizedAltData]:
    """
    Fetch news for equity symbols from NewsAPI and score sentiment.
    Falls back to mock data if NEWS_API_KEY is not set.
    """
    results = []

    if not NEWS_API_KEY:
        # Mock news data
        for symbol in symbols:
            import random

            score = round(random.uniform(-0.5, 0.8), 4)
            alt = NormalizedAltData(
                time=datetime.now(timezone.utc),
                data_type=AltDataType.NEWS_SENTIMENT,
                source="newsapi-mock",
                symbol=symbol,
                score=score,
                payload={
                    "articles": 2,
                    "top_headline": f"Mock news for {symbol}",
                    "sentiment": score,
                },
            )
            results.append(alt)
        return results

    async with httpx.AsyncClient(timeout=30) as client:
        for symbol in symbols:
            try:
                resp = await client.get(
                    "https://newsapi.org/v2/everything",
                    params={
                        "q": symbol,
                        "language": "en",
                        "sortBy": "publishedAt",
                        "pageSize": 10,
                        "apiKey": NEWS_API_KEY,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                articles = data.get("articles", [])

                if not articles:
                    continue

                # Score all headlines and descriptions
                texts = [
                    f"{a.get('title', '')} {a.get('description', '')}" for a in articles
                ]
                scores = [_simple_sentiment(t) for t in texts]
                avg_score = sum(scores) / len(scores) if scores else 0.0

                alt = NormalizedAltData(
                    time=datetime.now(timezone.utc),
                    data_type=AltDataType.NEWS_SENTIMENT,
                    source="newsapi",
                    symbol=symbol,
                    score=round(avg_score, 4),
                    volume=float(len(articles)),
                    payload={
                        "articles": len(articles),
                        "top_headline": articles[0].get("title", ""),
                        "avg_score": round(avg_score, 4),
                        "scores": scores[:5],
                    },
                )
                results.append(alt)

            except Exception as e:
                logger.error(f"NewsAPI error for {symbol}: {e}")

    return results


# ── Social volume (mock) ──────────────────────────────────────────────────────


async def fetch_social_volume(symbols: List[str]) -> List[NormalizedAltData]:
    """
    Mock social volume endpoint.
    In production, replace with LunarCrush, Santiment, or Twitter API.
    """
    import random

    results = []
    for symbol in symbols:
        volume = random.randint(100, 50000)
        score = round(random.uniform(-0.3, 0.9), 4)
        alt = NormalizedAltData(
            time=datetime.now(timezone.utc),
            data_type=AltDataType.SOCIAL_VOLUME,
            source="social-mock",
            symbol=symbol,
            score=score,
            volume=float(volume),
            payload={
                "mentions": volume,
                "sentiment": score,
                "platform": "twitter+reddit",
                "change_24h": round(random.uniform(-0.3, 0.5), 4),
            },
        )
        results.append(alt)
    return results


# ── On-chain metrics (CoinGecko free tier) ────────────────────────────────────


async def fetch_onchain_metrics(symbols: List[str]) -> List[NormalizedAltData]:
    """Fetch on-chain metrics from CoinGecko free API."""
    results = []
    coin_ids = [COINGECKO_COINS[s] for s in symbols if s in COINGECKO_COINS]

    if not coin_ids:
        return results

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "ids": ",".join(coin_ids),
                    "order": "market_cap_desc",
                    "per_page": len(coin_ids),
                    "page": 1,
                    "sparkline": "false",
                    "price_change_percentage": "24h,7d",
                },
            )
            resp.raise_for_status()
            coins = resp.json()

            for coin in coins:
                symbol = next(
                    (s for s, cid in COINGECKO_COINS.items() if cid == coin["id"]), None
                )
                if not symbol:
                    continue

                alt = NormalizedAltData(
                    time=datetime.now(timezone.utc),
                    data_type=AltDataType.ONCHAIN,
                    source="coingecko",
                    symbol=symbol,
                    score=None,
                    volume=coin.get("total_volume"),
                    payload={
                        "market_cap": coin.get("market_cap"),
                        "total_volume": coin.get("total_volume"),
                        "price_change_24h": coin.get("price_change_percentage_24h"),
                        "price_change_7d": coin.get(
                            "price_change_percentage_7d_in_currency"
                        ),
                        "circulating_supply": coin.get("circulating_supply"),
                        "max_supply": coin.get("max_supply"),
                        "market_cap_rank": coin.get("market_cap_rank"),
                        "ath": coin.get("ath"),
                        "ath_change_percentage": coin.get("ath_change_percentage"),
                    },
                )
                results.append(alt)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("CoinGecko rate limit hit - skipping cycle")
            else:
                logger.error(f"CoinGecko error: {e}")
        except Exception as e:
            logger.error(f"CoinGecko fetch error: {e}")

    return results


# ── Pipeline runner ───────────────────────────────────────────────────────────


async def _process(items: List[NormalizedAltData]) -> None:
    for alt in items:
        try:
            await asyncio.gather(
                publish_alt_data(alt),
                write_alt_data(alt),
            )
        except Exception as e:
            logger.error(f"Error persisting alt data [{alt.data_type}]: {e}")


async def run_news_pipeline() -> None:
    while not _stop.is_set():
        try:
            items = await fetch_news_sentiment(EQUITY_SYMBOLS)
            await _process(items)
            logger.info(f"News pipeline: processed {len(items)} items")
        except Exception as e:
            logger.error(f"News pipeline error: {e}")
        await asyncio.sleep(NEWS_INTERVAL_MINS * 60)


async def run_social_pipeline() -> None:
    while not _stop.is_set():
        try:
            items = await fetch_social_volume(EQUITY_SYMBOLS + CRYPTO_SYMBOLS)
            await _process(items)
            logger.info(f"Social pipeline: processed {len(items)} items")
        except Exception as e:
            logger.error(f"Social pipeline error: {e}")
        await asyncio.sleep(300)  # every 5 minutes


async def run_onchain_pipeline() -> None:
    while not _stop.is_set():
        try:
            items = await fetch_onchain_metrics(CRYPTO_SYMBOLS)
            await _process(items)
            logger.info(f"On-chain pipeline: processed {len(items)} items")
        except Exception as e:
            logger.error(f"On-chain pipeline error: {e}")
        await asyncio.sleep(COINGECKO_INTERVAL * 60)


async def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    logger.info("DataSync Alt-Data Pipeline starting...")
    try:
        await asyncio.gather(
            run_news_pipeline(),
            run_social_pipeline(),
            run_onchain_pipeline(),
        )
    except asyncio.CancelledError:
        logger.info("Alt-data pipeline cancelled")
    finally:
        from kafka_producer.src.producer import flush_and_stop

        from store.src.timescale import close_pool

        await flush_and_stop()
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())

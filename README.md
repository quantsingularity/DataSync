# DataSync

## Market Data Layer

Unified real-time and historical market data service for equities, crypto, and options.
Streams normalized tick events to Kafka, stores OHLCV bars in TimescaleDB,
and exposes a REST API with Redis caching.

---

## Architecture

```
  Alpaca WebSocket ---+
                      |---> Normalize ---> Kafka Producer ---> market.ticks.equities
  Polygon WebSocket --+                                    ---> market.ticks.crypto
                      |                                    ---> market.ticks.options
  Mock Feed ----------+
                      |
                      +--> TimescaleDB (ticks, bars, alt_data)
                      |
                      +--> Redis (latest price cache, bar series cache)

  NewsAPI ------+
  Social Mock --+--> Alt-data Pipeline --> Kafka (datasync.alt-data) --> TimescaleDB
  CoinGecko ----+

  TimescaleDB + Redis <---- REST API (FastAPI :8000)
```

---

## Quick Start

```bash
git clone https://github.com/quantsingularity/DataSync.git
cd DataSync
cp .env.example .env
# Leave USE_MOCK_FEEDS=true for development (no real API keys needed)
make up
```

| Service            | URL                        |
| ------------------ | -------------------------- |
| API docs (Swagger) | http://localhost:8000/docs |
| Prometheus         | http://localhost:9090      |
| TimescaleDB        | localhost:5432             |
| Kafka              | localhost:9092             |
| Redis              | localhost:6379             |

---

## API Reference

### Historical Bars

```
GET /bars/{symbol}?timeframe=1d&from=2024-01-01&to=2024-01-31&limit=500
```

| Parameter | Default     | Options             |
| --------- | ----------- | ------------------- |
| timeframe | 1d          | 1m, 5m, 15m, 1h, 1d |
| from      | 30 days ago | ISO8601 datetime    |
| to        | now         | ISO8601 datetime    |
| limit     | 500         | max 5000            |

Response is Redis-cached. TTL set via `BARS_CACHE_TTL_SECONDS` (default 300s).

```bash
curl "http://localhost:8000/bars/AAPL?timeframe=1d&from=2024-01-01"
curl "http://localhost:8000/bars/BTC%2FUSD?timeframe=1h"
```

### Raw Ticks

```
GET /ticks/{symbol}?from=&to=&limit=1000
```

Returns raw tick data from TimescaleDB (up to 10,000 per request).

### Latest Price

```
GET /price/{symbol}
```

Redis-first lookup. Falls back to TimescaleDB latest tick.

### Alternative Data

```
GET /alt-data/{symbol}?data_type=news_sentiment&limit=20
```

| data_type      | Source                              |
| -------------- | ----------------------------------- |
| news_sentiment | NewsAPI (keyword scoring)           |
| social_volume  | Mock (plug in LunarCrush/Santiment) |
| onchain        | CoinGecko free tier                 |

### Subscription Manager

```
POST /subscriptions      - register a symbol
GET  /subscriptions      - list all active subscriptions
DELETE /subscriptions/1  - deactivate subscription
```

```bash
# Register a downstream service's interest in a symbol
curl -X POST http://localhost:8000/subscriptions \
  -H "Content-Type: application/json" \
  -d '{"service_name": "algo-trader", "symbol": "NVDA", "asset_class": "equity"}'
```

### Symbol List

```
GET /symbols
```

Returns all symbols currently present in TimescaleDB.

---

## Kafka Topics

| Topic                   | Content                       | Partitions |
| ----------------------- | ----------------------------- | ---------- |
| `market.ticks.equities` | Equity trades and quotes      | 4          |
| `market.ticks.crypto`   | Crypto trades and quotes      | 4          |
| `market.ticks.options`  | Options ticks                 | 2          |
| `datasync.alt-data`     | News, social, on-chain events | 2          |

### Message schema (NormalizedTick)

```json
{
  "event_type": "tick",
  "time": "2024-01-15T14:30:00.123456+00:00",
  "symbol": "AAPL",
  "asset_class": "equity",
  "source": "alpaca",
  "price": "185.42",
  "size": "100",
  "bid": "185.40",
  "ask": "185.44",
  "bid_size": "200",
  "ask_size": "150",
  "conditions": ["@", "I"],
  "exchange": "C",
  "tape": "C",
  "extra": {}
}
```

Kafka key is always the symbol (e.g. `AAPL` or `BTC/USD`).

---

## Data Sources

### Alpaca Markets

- Equities: trades, quotes, 1-minute bars
- Crypto: trades, quotes
- Historical: REST API backfill on startup
- Free IEX feed available; paid SIP feed for full NBBO

Set `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` in `.env`.

### Polygon.io

- Equities: trades, quotes, per-second aggregates
- Free tier: end-of-day bars only; paid tier: real-time

Set `POLYGON_API_KEY` in `.env`.

### Mock Feed (`USE_MOCK_FEEDS=true`)

- Generates realistic random-walk price data
- No API keys required
- Controlled via `MOCK_FEED_INTERVAL_MS`

### Alternative Data Sources

| Source      | Data                              | Auth                  |
| ----------- | --------------------------------- | --------------------- |
| NewsAPI     | News sentiment for equity symbols | `NEWS_API_KEY`        |
| Social mock | Mention volume + sentiment        | No key (pluggable)    |
| CoinGecko   | On-chain metrics for crypto       | No key (rate limited) |

---

## Normalization Schema

All sources emit the same canonical Pydantic models regardless of origin.

```python
from normalize.src.models import NormalizedTick, NormalizedBar, NormalizedAltData

# All adapters produce NormalizedTick with identical fields
tick = alpaca_trade_to_tick(raw_msg, AssetClass.EQUITY)
tick = polygon_trade_to_tick(raw_msg)
# Same schema, different source field
```

---

## Redis Caching

| Key pattern                               | TTL                             | Content                   |
| ----------------------------------------- | ------------------------------- | ------------------------- |
| `datasync:price:{SYMBOL}`                 | `CACHE_TTL_SECONDS` (60s)       | Latest tick price         |
| `datasync:bars:{SYMBOL}:{tf}:{from}:{to}` | `BARS_CACHE_TTL_SECONDS` (300s) | Bar series                |
| `datasync:subscriptions`                  | 60s                             | Active subscriptions list |
| `datasync:stream:{SYMBOL}`                | 1000 entries                    | Redis Stream (real-time)  |

---

## Folder Structure

```
DataSync/
├── Makefile
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── pytest.ini
│
├── normalize/               - canonical Pydantic models + feed adapters
│   └── src/
│       ├── models.py        - NormalizedTick, NormalizedBar, NormalizedAltData
│       ├── alpaca_adapter.py
│       └── polygon_adapter.py
│
├── kafka-producer/          - async Kafka publisher
│   └── src/producer.py
│
├── store/                   - persistence layer
│   └── src/
│       ├── timescale.py     - TimescaleDB reads and writes (asyncpg)
│       └── cache.py         - Redis caching layer
│
├── ingest/                  - WebSocket feed connections
│   ├── Dockerfile
│   └── src/
│       ├── main.py          - service orchestrator
│       ├── mock_feed.py     - realistic mock feed (no API keys)
│       ├── alpaca_client.py - Alpaca WebSocket client
│       ├── polygon_client.py - Polygon WebSocket client
│       └── historical.py    - Alpaca REST historical backfill
│
├── alt-data/                - alternative data pipeline
│   ├── Dockerfile
│   └── src/main.py          - NewsAPI, social mock, CoinGecko
│
├── api/                     - REST API
│   ├── Dockerfile
│   └── src/
│       ├── main.py          - FastAPI app + all endpoints
│       └── subscriptions.py - subscription manager
│
├── infra/
│   ├── timescaledb/init.sql - hypertables + continuous aggregates
│   └── prometheus/prometheus.yml
│
└── tests/
    ├── conftest.py          - fixtures including mock WebSocket server
    ├── test_normalize.py    - model + adapter unit tests
    ├── test_ingest.py       - WebSocket integration tests
    ├── test_api.py          - REST API endpoint tests
    └── test_alt_data.py     - alt-data pipeline tests
```

---

## Running Tests

```bash
make test
# or:
pip install -r requirements.txt
pytest tests/ -v
```

Test coverage:

- `test_normalize.py` - 16 tests: tick/bar/alt_data models, Alpaca adapter, Polygon adapter
- `test_ingest.py` - 5 tests: mock equity feed, mock crypto feed, mock bar feed, Alpaca WebSocket integration, Polygon WebSocket integration
- `test_api.py` - 11 tests: all REST endpoints, validation, symbol casing
- `test_alt_data.py` - 8 tests: news mock, social mock, sentiment scoring, serialization

---

## Connecting to Other Suite Services

DataSync feeds the following Nexon (0.1) and downstream services:

```python
# From Nexon feature-store: subscribe to equity symbols
import httpx

async def register_with_datasync(symbols: list, service: str):
    async with httpx.AsyncClient() as c:
        for sym in symbols:
            await c.post("http://datasync-api:8000/subscriptions", json={
                "service_name": service,
                "symbol":       sym,
                "asset_class":  "equity",
            })

# From a trading algorithm: consume Kafka ticks
from aiokafka import AIOKafkaConsumer
import json

consumer = AIOKafkaConsumer(
    "market.ticks.equities",
    bootstrap_servers = "kafka:29092",
    group_id          = "my-algo",
)
await consumer.start()
async for msg in consumer:
    tick = json.loads(msg.value)
    print(f"{tick['symbol']} @ {tick['price']}")
```

---

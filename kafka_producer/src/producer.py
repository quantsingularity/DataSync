"""
DataSync - Kafka Producer
Publishes NormalizedTick and NormalizedBar events to the correct Kafka topic.
Topics:
  market.ticks.equities
  market.ticks.crypto
  market.ticks.options
  datasync.alt-data
"""

import json
import logging
import os
from typing import Optional

from aiokafka import AIOKafkaProducer

from normalize.src.models import (
    AssetClass,
    NormalizedAltData,
    NormalizedBar,
    NormalizedTick,
)

logger = logging.getLogger("datasync.kafka-producer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_EQUITIES = os.getenv("KAFKA_TOPIC_EQUITIES", "market.ticks.equities")
TOPIC_CRYPTO = os.getenv("KAFKA_TOPIC_CRYPTO", "market.ticks.crypto")
TOPIC_OPTIONS = os.getenv("KAFKA_TOPIC_OPTIONS", "market.ticks.options")
TOPIC_ALTDATA = os.getenv("KAFKA_TOPIC_ALTDATA", "datasync.alt-data")

ASSET_TOPIC_MAP = {
    AssetClass.EQUITY: TOPIC_EQUITIES,
    AssetClass.CRYPTO: TOPIC_CRYPTO,
    AssetClass.OPTION: TOPIC_OPTIONS,
}

_producer: Optional[AIOKafkaProducer] = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: (
                k if isinstance(k, bytes) else str(k).encode("utf-8")
            ),
            compression_type="gzip",
            acks="all",
            max_batch_size=16384,
            linger_ms=5,
        )
        await _producer.start()
        logger.info(f"Kafka producer started - bootstrap: {KAFKA_BOOTSTRAP}")
    return _producer


async def publish_tick(tick: NormalizedTick) -> None:
    """Publish a normalized tick to the appropriate Kafka topic."""
    topic = ASSET_TOPIC_MAP.get(tick.asset_class, TOPIC_EQUITIES)
    producer = await get_producer()
    try:
        await producer.send(
            topic,
            key=tick.kafka_key(),
            value=tick.to_kafka_value(),
        )
    except Exception as e:
        logger.error(f"Failed to publish tick for {tick.symbol}: {e}")
        raise


async def publish_bar(bar: NormalizedBar) -> None:
    """Publish a normalized bar to the appropriate Kafka topic."""
    topic = ASSET_TOPIC_MAP.get(bar.asset_class, TOPIC_EQUITIES)
    producer = await get_producer()
    try:
        await producer.send(
            topic,
            key=bar.symbol.encode(),
            value=bar.model_dump(mode="json"),
        )
    except Exception as e:
        logger.error(f"Failed to publish bar for {bar.symbol}: {e}")
        raise


async def publish_alt_data(alt: NormalizedAltData) -> None:
    """Publish alternative data to the alt-data topic."""
    producer = await get_producer()
    try:
        await producer.send(
            TOPIC_ALTDATA,
            key=(alt.symbol or alt.data_type).encode(),
            value=alt.model_dump(mode="json"),
        )
    except Exception as e:
        logger.error(f"Failed to publish alt data [{alt.data_type}]: {e}")
        raise


async def flush_and_stop() -> None:
    global _producer
    if _producer:
        await _producer.flush()
        await _producer.stop()
        _producer = None
        logger.info("Kafka producer stopped")

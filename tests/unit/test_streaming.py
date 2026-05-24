from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from quantstream.schemas.envelope import EventEnvelope
from quantstream.schemas.serialization import model_to_bytes
from quantstream.schemas.trade import TradeEvent
from quantstream.streaming.config import ConsumerConfig
from quantstream.streaming.pipeline import (
    SparkStreamingPipeline,
    TRADE_EVENT_SCHEMA,
    ENRICHED_TRADE_SCHEMA,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-quantstream")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def pipeline():
    consumer_config = ConsumerConfig(bootstrap_servers="localhost:9092")
    return SparkStreamingPipeline(
        consumer_config=consumer_config,
        checkpoint_dir="/tmp/checkpoints/",
        minio_endpoint="",
    )


class TestSparkStreamingPipeline:
    def test_pipeline_creates_spark_session(self, pipeline):
        assert pipeline.spark is not None
        assert pipeline.app_name == "QuantStreamPipeline"

    def test_kafka_read_options(self, pipeline):
        df = pipeline.read_from_redpanda("raw.trades")
        assert df.isStreaming is True

    def test_deserialize_events(self, spark, pipeline):
        trade = TradeEvent(
            trade_id=uuid.uuid4(),
            exchange="binance",
            symbol="BTCUSDT",
            price=Decimal("68452.12"),
            quantity=Decimal("0.035"),
            side="BUY",
            event_time=datetime.now(timezone.utc),
            ingest_time=datetime.now(timezone.utc),
        )
        envelope = EventEnvelope.wrap_trade(trade)
        value = model_to_bytes(envelope)

        df = spark.createDataFrame(
            [{"key": b"BTCUSDT", "value": value}]
        )

        result = pipeline.deserialize_events(df)
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].symbol == "BTCUSDT"

    def test_deserialize_invalid_json_returns_empty(self, spark, pipeline):
        df = spark.createDataFrame(
            [{"key": b"BAD", "value": b"not valid json"}]
        )

        result = pipeline.deserialize_events(df)
        rows = result.collect()
        assert len(rows) == 0

    def test_apply_watermark(self, spark, pipeline):
        trade = TradeEvent(
            trade_id=uuid.uuid4(),
            exchange="binance",
            symbol="BTCUSDT",
            price=Decimal("68452"),
            quantity=Decimal("0.1"),
            side="BUY",
            event_time=datetime.now(timezone.utc),
            ingest_time=datetime.now(timezone.utc),
        )
        envelope = EventEnvelope.wrap_trade(trade)
        value = model_to_bytes(envelope)

        df = spark.createDataFrame(
            [{"key": b"BTCUSDT", "value": value}]
        )
        events_df = pipeline.deserialize_events(df)
        watermarked = pipeline.apply_watermark(events_df, 60)
        assert watermarked.isStreaming is True

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from quantstream.ingestion.client import RedpandaProducer
from quantstream.schemas.trade import TradeEvent
from quantstream.streaming.config import ConsumerConfig, ProducerConfig
from quantstream.streaming.pipeline import SparkStreamingPipeline


@pytest.mark.integration
class TestStreamingIntegration:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("test-integration-streaming")
            .getOrCreate()
        )
        yield
        self.spark.stop()

    def test_streaming_deserialization_end_to_end(self):
        producer_config = ProducerConfig(bootstrap_servers="localhost:19092")
        producer = RedpandaProducer(producer_config)

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

        producer.publish_trade(trade)
        producer.flush(timeout=10.0)

        consumer_config = ConsumerConfig(
            bootstrap_servers="localhost:19092",
            group_id="test-streaming-integration",
        )

        kafka_options = consumer_config.to_spark_kafka_options()
        kafka_options["subscribe"] = "raw.trades"
        kafka_options["startingOffsets"] = "earliest"

        df = (
            self.spark.read
            .format("kafka")
            .options(**kafka_options)
            .load()
        )

        assert df.count() >= 1

        from quantstream.streaming.pipeline import SparkStreamingPipeline
        events_df = SparkStreamingPipeline.deserialize_events(df)
        rows = events_df.collect()
        assert len(rows) >= 1

        btc_events = [r for r in rows if r.symbol == "BTCUSDT"]
        assert len(btc_events) >= 1
        assert btc_events[0].side == "BUY"

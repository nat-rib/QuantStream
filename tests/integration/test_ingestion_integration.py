from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from quantstream.ingestion.client import RedpandaProducer
from quantstream.schemas.trade import TradeEvent
from quantstream.streaming.config import ConsumerConfig, ProducerConfig


@pytest.mark.integration
class TestIngestionIntegration:
    def test_producer_to_redpanda(self):
        config = ProducerConfig(bootstrap_servers="localhost:19092")
        producer = RedpandaProducer(config)

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
            bootstrap_servers="localhost:19092", group_id="test-group"
        )
        from confluent_kafka import Consumer

        consumer = Consumer(consumer_config.to_confluent_config())
        consumer.subscribe(["raw.trades"])

        msg = consumer.poll(10.0)
        consumer.close()

        assert msg is not None
        assert msg.error() is None

        data = json.loads(msg.value().decode("utf-8"))
        assert data["eventType"] == "trade"
        assert "payload" in data
        assert data["payload"]["symbol"] == "BTCUSDT"

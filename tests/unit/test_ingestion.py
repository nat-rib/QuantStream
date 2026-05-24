from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from quantstream.ingestion.client import BinanceWebSocketClient, RedpandaProducer
from quantstream.schemas.trade import TradeEvent
from quantstream.streaming.config import ProducerConfig


class TestBinanceWebSocketClient:
    def test_init_with_symbols(self):
        client = BinanceWebSocketClient(["btcusdt", "ethusdt"])
        assert client.symbols == ["btcusdt", "ethusdt"]

    def test_symbols_lowercased(self):
        client = BinanceWebSocketClient(["BTCUSDT", "ETHUSDT"])
        assert client.symbols == ["btcusdt", "ethusdt"]

    @pytest.mark.asyncio
    async def test_handle_message_parses_trade(self):
        client = BinanceWebSocketClient(["btcusdt"])
        producer = MagicMock(spec=RedpandaProducer)
        client.set_producer(producer)

        message = json.dumps({
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "E": 1716561600000,
                "s": "BTCUSDT",
                "t": 12345,
                "p": "68452.12",
                "q": "0.035",
                "T": 1716561600000,
                "m": False,
            },
        })

        await client._handle_message(message)
        producer.publish_trade.assert_called_once()
        trade = producer.publish_trade.call_args[0][0]
        assert trade.symbol == "BTCUSDT"
        assert trade.price == Decimal("68452.12")

    @pytest.mark.asyncio
    async def test_handle_message_skips_non_data(self):
        client = BinanceWebSocketClient(["btcusdt"])
        producer = MagicMock(spec=RedpandaProducer)
        client.set_producer(producer)

        message = json.dumps({"result": None, "id": 1})
        await client._handle_message(message)
        producer.publish_trade.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_message_buy_side_detection(self):
        client = BinanceWebSocketClient(["btcusdt"])
        producer = MagicMock(spec=RedpandaProducer)
        client.set_producer(producer)

        message = json.dumps({
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "E": 1716561600000,
                "s": "BTCUSDT",
                "p": "68452.12",
                "q": "0.035",
                "T": 1716561600000,
                "m": False,
            },
        })

        await client._handle_message(message)
        trade = producer.publish_trade.call_args[0][0]
        assert trade.side == "BUY"

    @pytest.mark.asyncio
    async def test_handle_message_sell_side_detection(self):
        client = BinanceWebSocketClient(["btcusdt"])
        producer = MagicMock(spec=RedpandaProducer)
        client.set_producer(producer)

        message = json.dumps({
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "E": 1716561600000,
                "s": "BTCUSDT",
                "p": "68452.12",
                "q": "0.035",
                "T": 1716561600000,
                "m": True,
            },
        })

        await client._handle_message(message)
        trade = producer.publish_trade.call_args[0][0]
        assert trade.side == "SELL"


class TestRedpandaProducer:
    def test_init(self):
        config = ProducerConfig(bootstrap_servers="localhost:9092")
        producer = RedpandaProducer(config)
        assert producer.topic == "raw.trades"

    @patch("quantstream.ingestion.client.Producer")
    def test_publish_trade(self, mock_kafka_producer):
        config = ProducerConfig(bootstrap_servers="localhost:9092")
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
        producer._producer.produce.assert_called_once()
        call_kwargs = producer._producer.produce.call_args[1]
        assert call_kwargs["topic"] == "raw.trades"
        assert call_kwargs["key"] == b"BTCUSDT"

    @patch("quantstream.ingestion.client.Producer")
    def test_flush_calls_underlying_producer(self, mock_kafka_producer):
        config = ProducerConfig(bootstrap_servers="localhost:9092")
        producer = RedpandaProducer(config)
        producer.flush(timeout=10.0)
        producer._producer.flush.assert_called_once_with(10.0)

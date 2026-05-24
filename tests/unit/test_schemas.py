from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from quantstream.schemas.envelope import EventEnvelope
from quantstream.schemas.models import EnrichedTradeEvent, OHLCCandle, TradeStats, VolumeProfile
from quantstream.schemas.registry import SchemaRegistry, SchemaVersionError, get_schema_registry
from quantstream.schemas.serialization import (
    bytes_to_model,
    dict_to_model,
    json_to_model,
    model_to_bytes,
    model_to_json,
)
from quantstream.schemas.trade import TradeEvent


def _valid_trade_dict():
    return {
        "tradeId": str(uuid.uuid4()),
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "price": 68452.12,
        "quantity": 0.035,
        "side": "BUY",
        "eventTime": datetime.now(timezone.utc),
        "schemaVersion": 1,
    }


def _valid_trade_snake_dict():
    return {
        "trade_id": uuid.uuid4(),
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "price": Decimal("68452.12"),
        "quantity": Decimal("0.035"),
        "side": "BUY",
        "event_time": datetime.now(timezone.utc),
        "schema_version": 1,
    }


class TestTradeEvent:
    def test_valid_trade_from_camel_json(self):
        data = _valid_trade_dict()
        event = TradeEvent.model_validate(data)
        assert event.symbol == "BTCUSDT"
        assert event.price == Decimal("68452.12")
        assert event.side == "BUY"

    def test_valid_trade_from_snake_case(self):
        data = _valid_trade_snake_dict()
        event = TradeEvent.model_validate(data)
        assert event.symbol == "BTCUSDT"

    def test_missing_required_field_raises_error(self):
        data = _valid_trade_dict()
        del data["price"]
        with pytest.raises(ValidationError):
            TradeEvent.model_validate(data)

    def test_invalid_side_raises_error(self):
        data = _valid_trade_dict()
        data["side"] = "INVALID"
        with pytest.raises(ValidationError):
            TradeEvent.model_validate(data)

    def test_negative_price_raises_error(self):
        data = _valid_trade_dict()
        data["price"] = -10.0
        with pytest.raises(ValidationError):
            TradeEvent.model_validate(data)

    def test_zero_quantity_raises_error(self):
        data = _valid_trade_snake_dict()
        data["quantity"] = Decimal("0")
        with pytest.raises(ValidationError):
            TradeEvent.model_validate(data)

    def test_json_deserialize(self):
        data = _valid_trade_dict()
        event = TradeEvent.model_validate(data)
        json_str = model_to_json(event)
        parsed = json_to_model(json_str, TradeEvent)
        assert parsed.trade_id == event.trade_id
        assert parsed.price == event.price

    def test_bytes_serialization_roundtrip(self):
        data = _valid_trade_dict()
        event = TradeEvent.model_validate(data)
        binary = model_to_bytes(event)
        parsed = bytes_to_model(binary, TradeEvent)
        assert parsed.trade_id == event.trade_id
        assert parsed.symbol == event.symbol

    def test_symbol_uppercased(self):
        data = _valid_trade_dict()
        data["symbol"] = "btcusdt"
        data["tradeId"] = str(uuid.uuid4())
        event = TradeEvent.model_validate(data)
        assert event.symbol == "BTCUSDT"

    def test_ingest_time_defaults_to_utc_now(self):
        data = _valid_trade_dict()
        data["tradeId"] = str(uuid.uuid4())
        event = TradeEvent.model_validate(data)
        assert event.ingest_time.tzinfo == timezone.utc

    def test_event_time_without_timezone_gets_utc(self):
        data = _valid_trade_snake_dict()
        data["event_time"] = datetime(2026, 5, 24, 14, 33, 52)
        event = TradeEvent.model_validate(data)
        assert event.event_time.tzinfo == timezone.utc

    def test_camel_case_alias_output(self):
        data = _valid_trade_snake_dict()
        event = TradeEvent.model_validate(data)
        json_str = model_to_json(event)
        assert "tradeId" in json_str
        assert "eventTime" in json_str
        assert "schemaVersion" in json_str


class TestEnrichedTradeEvent:
    def test_enriched_includes_processing_fields(self):
        data = _valid_trade_snake_dict()
        enriched = EnrichedTradeEvent.model_validate(data)
        assert enriched.processing_timestamp.tzinfo == timezone.utc
        assert enriched.pipeline_version == "0.1.0"
        assert enriched.partition_date == ""

    def test_enriched_inherits_trade_validators(self):
        data = _valid_trade_snake_dict()
        data["quantity"] = Decimal("-1")
        with pytest.raises(ValidationError):
            EnrichedTradeEvent.model_validate(data)


class TestEventEnvelope:
    def test_wrap_trade_creates_envelope(self):
        data = _valid_trade_snake_dict()
        trade = TradeEvent.model_validate(data)
        envelope = EventEnvelope.wrap_trade(trade)
        assert envelope.payload.trade_id == trade.trade_id
        assert envelope.partition_key == trade.symbol
        assert envelope.source == trade.exchange
        assert envelope.event_type == "trade"

    def test_envelope_serialization_roundtrip(self):
        data = _valid_trade_snake_dict()
        trade = TradeEvent.model_validate(data)
        envelope = EventEnvelope.wrap_trade(trade)
        json_str = model_to_json(envelope)
        assert "eventId" in json_str
        assert "partitionKey" in json_str


class TestSchemaRegistry:
    def test_default_registry_has_version_1(self):
        registry = SchemaRegistry()
        model = registry.get_model(1)
        assert model == TradeEvent

    def test_unknown_version_raises_error(self):
        registry = SchemaRegistry()
        with pytest.raises(SchemaVersionError) as exc:
            registry.get_model(99)
        assert exc.value.version == 99

    def test_register_new_version(self):
        registry = SchemaRegistry()

        class V2Model(TradeEvent):
            commission: float = 0.0

        registry.register(2, V2Model)
        assert registry.get_model(2) == V2Model
        assert registry.get_latest_version() == 2

    def test_duplicate_registration_raises_error(self):
        registry = SchemaRegistry()
        with pytest.raises(ValueError, match="already registered"):
            registry.register(1, TradeEvent)

    def test_get_latest_model(self):
        registry = SchemaRegistry()
        assert registry.get_latest_model() == TradeEvent

    def test_global_registry_is_singleton(self):
        r1 = get_schema_registry()
        r2 = get_schema_registry()
        assert r1 is r2


class TestGoldModels:
    def test_ohlc_candle_validation(self):
        candle = OHLCCandle(
            symbol="BTCUSDT",
            window_start=datetime.now(timezone.utc),
            window_end=datetime.now(timezone.utc),
            open_price=Decimal("68000"),
            high_price=Decimal("68500"),
            low_price=Decimal("67900"),
            close_price=Decimal("68400"),
            volume=Decimal("100.5"),
            trade_count=50,
        )
        assert candle.symbol == "BTCUSDT"

    def test_volume_profile_validation(self):
        vp = VolumeProfile(
            symbol="BTCUSDT",
            window_start=datetime.now(timezone.utc),
            window_end=datetime.now(timezone.utc),
            total_volume=Decimal("200"),
            buy_volume=Decimal("120"),
            sell_volume=Decimal("80"),
            buy_count=30,
            sell_count=20,
            vwap=Decimal("68300"),
        )
        assert vp.buy_volume + vp.sell_volume == vp.total_volume

    def test_trade_stats_validation(self):
        stats = TradeStats(
            symbol="BTCUSDT",
            window_start=datetime.now(timezone.utc),
            window_end=datetime.now(timezone.utc),
            trade_count=100,
            avg_trade_size=Decimal("0.5"),
            max_trade_size=Decimal("5.0"),
            trades_per_second=1.67,
        )
        assert stats.trade_count == 100

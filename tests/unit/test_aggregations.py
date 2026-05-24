from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from quantstream.streaming.aggregations import (
    aggregate_ohlc,
    aggregate_volume_profile,
    aggregate_trade_stats,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-aggregations")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestOHLCAggregation:
    def test_ohlc_computes_correct_values(self, spark):
        base_time = datetime(2026, 5, 24, 10, 0, 0, tzinfo=timezone.utc)
        data = [
            {"symbol": "BTCUSDT", "price": 68000.0, "quantity": 0.1, "side": "BUY",
             "event_time": base_time, "trade_id": "1"},
            {"symbol": "BTCUSDT", "price": 68500.0, "quantity": 0.2, "side": "BUY",
             "event_time": base_time, "trade_id": "2"},
            {"symbol": "BTCUSDT", "price": 67000.0, "quantity": 0.1, "side": "SELL",
             "event_time": base_time, "trade_id": "3"},
            {"symbol": "BTCUSDT", "price": 68400.0, "quantity": 0.3, "side": "BUY",
             "event_time": base_time, "trade_id": "4"},
        ]

        df = spark.createDataFrame(data)
        result = aggregate_ohlc(df, window_duration="1 minute")

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].symbol == "BTCUSDT"
        assert rows[0].high_price == 68500.0
        assert rows[0].low_price == 67000.0
        assert rows[0].trade_count == 4

    def test_ohlc_multiple_symbols(self, spark):
        base_time = datetime(2026, 5, 24, 10, 0, 0, tzinfo=timezone.utc)
        data = [
            {"symbol": "BTCUSDT", "price": 68000.0, "quantity": 0.1, "side": "BUY",
             "event_time": base_time, "trade_id": "1"},
            {"symbol": "ETHUSDT", "price": 3100.0, "quantity": 1.0, "side": "BUY",
             "event_time": base_time, "trade_id": "2"},
        ]

        df = spark.createDataFrame(data)
        result = aggregate_ohlc(df, window_duration="1 minute")
        rows = result.collect()
        symbols = {r.symbol for r in rows}
        assert symbols == {"BTCUSDT", "ETHUSDT"}


class TestVolumeProfile:
    def test_volume_profile_computes_correctly(self, spark):
        base_time = datetime(2026, 5, 24, 10, 0, 0, tzinfo=timezone.utc)
        data = [
            {"symbol": "BTCUSDT", "price": 68000.0, "quantity": 0.5, "side": "BUY",
             "event_time": base_time, "trade_id": "1"},
            {"symbol": "BTCUSDT", "price": 68000.0, "quantity": 0.3, "side": "SELL",
             "event_time": base_time, "trade_id": "2"},
        ]

        df = spark.createDataFrame(data)
        result = aggregate_volume_profile(df, window_duration="1 minute")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].total_volume == 0.8
        assert rows[0].buy_volume == 0.5
        assert rows[0].sell_volume == 0.3
        assert rows[0].buy_count == 1
        assert rows[0].sell_count == 1


class TestTradeStats:
    def test_trade_stats_computes_correctly(self, spark):
        base_time = datetime(2026, 5, 24, 10, 0, 0, tzinfo=timezone.utc)
        data = [
            {"symbol": "BTCUSDT", "price": 68000.0, "quantity": 0.1, "side": "BUY",
             "event_time": base_time, "trade_id": "1"},
            {"symbol": "BTCUSDT", "price": 68000.0, "quantity": 0.5, "side": "BUY",
             "event_time": base_time, "trade_id": "2"},
        ]

        df = spark.createDataFrame(data)
        result = aggregate_trade_stats(df, window_duration="1 minute")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].trade_count == 2
        assert rows[0].max_trade_size == 0.5
        assert rows[0].trades_per_second == 2.0 / 60.0

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from quantstream.streaming.transforms import (
    deduplicate_by_trade_id,
    enrich_with_processing_metadata,
    filter_invalid_events,
    normalize_fields,
    validate_business_rules,
    count_duplicates_dropped,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-transforms")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestNormalization:
    def test_symbol_uppercased(self, spark):
        df = spark.createDataFrame([
            {"symbol": "btcusdt", "exchange": "BINANCE", "side": "buy"},
        ])
        result = normalize_fields(df)
        row = result.collect()[0]
        assert row.symbol == "BTCUSDT"
        assert row.exchange == "binance"
        assert row.side == "BUY"


class TestBusinessValidation:
    def test_negative_price_filtered(self, spark):
        df = spark.createDataFrame([
            {"trade_id": "1", "price": -10.0, "quantity": 0.1, "side": "BUY",
             "event_time": datetime.now(timezone.utc), "ingest_time": datetime.now(timezone.utc)},
            {"trade_id": "2", "price": 100.0, "quantity": 0.1, "side": "BUY",
             "event_time": datetime.now(timezone.utc), "ingest_time": datetime.now(timezone.utc)},
        ])
        valid = validate_business_rules(df)
        rows = valid.collect()
        assert len(rows) == 1
        assert rows[0].trade_id == "2"

    def test_invalid_side_filtered(self, spark):
        df = spark.createDataFrame([
            {"trade_id": "1", "price": 100.0, "quantity": 0.1, "side": "INVALID",
             "event_time": datetime.now(timezone.utc), "ingest_time": datetime.now(timezone.utc)},
            {"trade_id": "2", "price": 100.0, "quantity": 0.1, "side": "SELL",
             "event_time": datetime.now(timezone.utc), "ingest_time": datetime.now(timezone.utc)},
        ])
        valid = validate_business_rules(df)
        valid_rows = valid.collect()
        assert len(valid_rows) == 1
        assert valid_rows[0].trade_id == "2"

    def test_filter_invalid_events(self, spark):
        df = spark.createDataFrame([
            {"trade_id": "1", "price": 100.0, "quantity": 0.1, "side": "BUY",
             "event_time": datetime.now(timezone.utc), "ingest_time": datetime.now(timezone.utc)},
            {"trade_id": "2", "price": -50.0, "quantity": 0.1, "side": "BUY",
             "event_time": datetime.now(timezone.utc), "ingest_time": datetime.now(timezone.utc)},
        ])
        valid, invalid = filter_invalid_events(df)
        assert valid.count() == 1
        assert invalid.count() == 1
        assert invalid.collect()[0].trade_id == "2"


class TestDeduplication:
    def test_keep_latest_ingest_time(self, spark):
        early = datetime(2026, 5, 24, 10, 0, 0, tzinfo=timezone.utc)
        late = datetime(2026, 5, 24, 11, 0, 0, tzinfo=timezone.utc)

        df = spark.createDataFrame([
            {"trade_id": "abc", "price": 100.0, "ingest_time": early},
            {"trade_id": "abc", "price": 101.0, "ingest_time": late},
        ])
        result = deduplicate_by_trade_id(df)
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].price == 101.0

    def test_different_trade_ids_not_deduplicated(self, spark):
        t = datetime.now(timezone.utc)
        df = spark.createDataFrame([
            {"trade_id": "abc", "price": 100.0, "ingest_time": t},
            {"trade_id": "def", "price": 200.0, "ingest_time": t},
        ])
        result = deduplicate_by_trade_id(df)
        assert result.count() == 2

    def test_count_duplicates_dropped(self, spark):
        t1 = datetime(2026, 5, 24, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 5, 24, 11, 0, 0, tzinfo=timezone.utc)

        df = spark.createDataFrame([
            {"trade_id": "abc", "price": 100.0, "ingest_time": t1},
            {"trade_id": "abc", "price": 101.0, "ingest_time": t2},
            {"trade_id": "def", "price": 200.0, "ingest_time": t1},
        ])
        deduped = deduplicate_by_trade_id(df)
        assert count_duplicates_dropped(df, deduped) == 1


class TestEnrichment:
    def test_enrich_adds_processing_fields(self, spark):
        df = spark.createDataFrame([
            {"trade_id": "1", "price": 100.0, "quantity": 0.1, "side": "BUY",
             "symbol": "BTCUSDT", "exchange": "binance",
             "event_time": datetime.now(timezone.utc),
             "ingest_time": datetime.now(timezone.utc),
             "schema_version": "1"},
        ])
        enriched = enrich_with_processing_metadata(df)
        row = enriched.collect()[0]
        assert row.processing_timestamp is not None
        assert row.pipeline_version == "0.1.0"
        assert row.partition_date is not None

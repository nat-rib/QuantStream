from __future__ import annotations

import tempfile
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import datetime as dt
from pyspark.sql import SparkSession

from quantstream.schemas.trade import TradeEvent
from quantstream.storage.writer import StorageWriter, read_bronze


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-storage")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestStorageWriter:
    def test_write_and_read_bronze(self, spark):
        trades_data = [
            {
                "trade_id": str(uuid.uuid4()),
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "price": Decimal("68452.12"),
                "quantity": Decimal("0.035"),
                "side": "BUY",
                "event_time": datetime.now(timezone.utc),
                "ingest_time": datetime.now(timezone.utc),
                "schema_version": "1",
            },
            {
                "trade_id": str(uuid.uuid4()),
                "exchange": "binance",
                "symbol": "ETHUSDT",
                "price": Decimal("3100.50"),
                "quantity": Decimal("1.5"),
                "side": "SELL",
                "event_time": datetime.now(timezone.utc),
                "ingest_time": datetime.now(timezone.utc),
                "schema_version": "1",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            df = spark.createDataFrame(trades_data)
            df.write.mode("overwrite").parquet(tmpdir)

            result = spark.read.parquet(tmpdir)
            assert result.count() == 2
            symbols = [r.symbol for r in result.collect()]
            assert "BTCUSDT" in symbols
            assert "ETHUSDT" in symbols

    def test_bronze_partitioning(self, spark):
        trades_data = [
            {
                "trade_id": str(uuid.uuid4()),
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "price": Decimal("68452.12"),
                "quantity": Decimal("0.035"),
                "side": "BUY",
                "event_time": datetime(2026, 5, 24, 14, 0, 0, tzinfo=timezone.utc),
                "ingest_time": datetime.now(timezone.utc),
                "schema_version": "1",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            from pyspark.sql import functions as F

            df = spark.createDataFrame(trades_data)
            df.withColumn(
                "partition_date", F.date_format(F.col("event_time"), "yyyy-MM-dd")
            ).write.mode("overwrite").partitionBy("partition_date").parquet(tmpdir)

            result = spark.read.parquet(tmpdir)
            partitions = result.select("partition_date").distinct().collect()
            assert len(partitions) == 1
            assert partitions[0].partition_date == dt.date(2026, 5, 24)

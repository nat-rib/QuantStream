from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from quantstream.streaming.pipeline import TRADE_EVENT_SCHEMA


BRONZE_EXPECTED_SCHEMA = TRADE_EVENT_SCHEMA


@pytest.mark.contract
class TestBronzeContract:
    def test_bronze_schema_fields(self):
        required_fields = {
            "trade_id",
            "exchange",
            "symbol",
            "price",
            "quantity",
            "side",
            "event_time",
            "ingest_time",
            "schema_version",
        }
        actual_fields = {f.name for f in BRONZE_EXPECTED_SCHEMA.fields}
        assert actual_fields == required_fields

    def test_bronze_schema_types(self):
        field_types = {f.name: type(f.dataType) for f in BRONZE_EXPECTED_SCHEMA.fields}
        assert field_types["trade_id"] == StringType
        assert field_types["price"] == DecimalType
        assert field_types["event_time"] == TimestampType
        assert field_types["side"] == StringType

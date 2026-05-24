from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def normalize_fields(df: DataFrame) -> DataFrame:
    return df.withColumn("symbol", F.upper(F.trim(F.col("symbol")))) \
             .withColumn("side", F.upper(F.trim(F.col("side")))) \
             .withColumn("exchange", F.lower(F.trim(F.col("exchange"))))


def validate_business_rules(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("price") > 0) &
        (F.col("quantity") > 0) &
        (F.col("side").isin("BUY", "SELL")) &
        (F.col("event_time") <= F.current_timestamp() + F.expr("INTERVAL 5 SECONDS"))
    )


def filter_invalid_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = validate_business_rules(df)
    invalid = df.join(valid, on="trade_id", how="left_anti")
    return valid, invalid


def deduplicate_by_trade_id(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("trade_id").orderBy(F.col("ingest_time").desc())

    return df.withColumn("_row_num", F.row_number().over(window_spec)) \
             .filter(F.col("_row_num") == 1) \
             .drop("_row_num")


def enrich_with_processing_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "processing_timestamp", F.current_timestamp()
    ).withColumn(
        "pipeline_version", F.lit("0.1.0")
    ).withColumn(
        "partition_date", F.date_format(F.col("event_time"), "yyyy-MM-dd")
    )


def count_invalid_events(df: DataFrame) -> int:
    return df.count()


def count_duplicates_dropped(original: DataFrame, deduplicated: DataFrame) -> int:
    return original.count() - deduplicated.count()

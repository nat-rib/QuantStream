from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_ohlc(
    df: DataFrame,
    window_duration: str = "1 minute",
    watermark_seconds: int = 60,
) -> DataFrame:
    return (
        df.withWatermark("event_time", f"{watermark_seconds} seconds")
        .groupBy(
            F.col("symbol"),
            F.window("event_time", window_duration),
        )
        .agg(
            F.first("price", ignorenulls=True).alias("open_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.last("price", ignorenulls=True).alias("close_price"),
            F.sum("quantity").alias("volume"),
            F.count("trade_id").alias("trade_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("window_duration", F.lit(window_duration))
        .drop("window")
    )


def aggregate_volume_profile(
    df: DataFrame,
    window_duration: str = "1 minute",
    watermark_seconds: int = 60,
) -> DataFrame:
    return (
        df.withWatermark("event_time", f"{watermark_seconds} seconds")
        .groupBy(
            F.col("symbol"),
            F.window("event_time", window_duration),
        )
        .agg(
            F.sum("quantity").alias("total_volume"),
            F.sum(F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(0)).alias("buy_volume"),
            F.sum(F.when(F.col("side") == "SELL", F.col("quantity")).otherwise(0)).alias("sell_volume"),
            F.count(F.when(F.col("side") == "BUY", F.lit(1))).alias("buy_count"),
            F.count(F.when(F.col("side") == "SELL", F.lit(1))).alias("sell_count"),
            F.sum(F.col("price") * F.col("quantity")).alias("volume_price_product"),
        )
        .withColumn("vwap", F.col("volume_price_product") / F.col("total_volume"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("window_duration", F.lit(window_duration))
        .drop("window", "volume_price_product")
    )


def aggregate_trade_stats(
    df: DataFrame,
    window_duration: str = "1 minute",
    watermark_seconds: int = 60,
) -> DataFrame:
    return (
        df.withWatermark("event_time", f"{watermark_seconds} seconds")
        .groupBy(
            F.col("symbol"),
            F.window("event_time", window_duration),
        )
        .agg(
            F.count("trade_id").alias("trade_count"),
            F.avg("quantity").alias("avg_trade_size"),
            F.max("quantity").alias("max_trade_size"),
        )
        .withColumn(
            "trades_per_second",
            F.col("trade_count") / F.lit(60.0),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("window_duration", F.lit(window_duration))
        .drop("window")
    )

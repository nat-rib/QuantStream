from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class StorageWriter:
    def __init__(self, spark: SparkSession, checkpoint_base: str) -> None:
        self.spark = spark
        self.checkpoint_base = checkpoint_base

    def write_bronze(
        self,
        df: DataFrame,
        path: str = "s3a://quantstream/bronze/trades/",
        trigger_seconds: int = 30,
    ) -> None:
        bronzed = df.withColumn(
            "partition_date", F.date_format(F.col("event_time"), "yyyy-MM-dd")
        )

        query = (
            bronzed.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", path)
            .option("checkpointLocation", f"{self.checkpoint_base}bronze")
            .partitionBy("partition_date")
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .start()
        )
        logger.info("Bronze writer started", extra={"path": path})
        return query

    def write_silver(
        self,
        df: DataFrame,
        path: str = "s3a://quantstream/silver/trades/",
        trigger_seconds: int = 30,
    ) -> None:
        enriched = df.withColumn(
            "processing_timestamp", F.current_timestamp()
        ).withColumn(
            "pipeline_version", F.lit("0.1.0")
        ).withColumn(
            "partition_date", F.date_format(F.col("event_time"), "yyyy-MM-dd")
        )

        query = (
            enriched.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", path)
            .option("checkpointLocation", f"{self.checkpoint_base}silver")
            .partitionBy("partition_date", "symbol")
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .start()
        )
        logger.info("Silver writer started", extra={"path": path})
        return query

    def write_gold(
        self,
        df: DataFrame,
        dataset: str,
        path_template: str = "s3a://quantstream/gold/{dataset}/",
        trigger_seconds: int = 60,
    ) -> None:
        output_path = path_template.format(dataset=dataset)
        golden = df.withColumn(
            "partition_date", F.date_format(F.col("window_end"), "yyyy-MM-dd")
        )

        query = (
            golden.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", f"{self.checkpoint_base}gold/{dataset}")
            .partitionBy("partition_date")
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .start()
        )
        logger.info("Gold writer started", extra={"dataset": dataset, "path": output_path})
        return query


def read_bronze(spark: SparkSession, path: str = "s3a://quantstream/bronze/trades/") -> DataFrame:
    return spark.read.parquet(path)


def read_silver(spark: SparkSession, path: str = "s3a://quantstream/silver/trades/") -> DataFrame:
    return spark.read.parquet(path)


def read_gold(spark: SparkSession, dataset: str, path_template: str = "s3a://quantstream/gold/{dataset}/") -> DataFrame:
    return spark.read.parquet(path_template.format(dataset=dataset))

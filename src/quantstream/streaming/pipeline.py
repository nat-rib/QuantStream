from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from quantstream.schemas.serialization import bytes_to_model, json_to_model
from quantstream.schemas.trade import TradeEvent
from quantstream.streaming.config import ConsumerConfig, ProducerConfig

logger = logging.getLogger(__name__)


TRADE_EVENT_SCHEMA = StructType([
    StructField("trade_id", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DecimalType(18, 8), True),
    StructField("quantity", DecimalType(18, 8), True),
    StructField("side", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("ingest_time", TimestampType(), True),
    StructField("schema_version", StringType(), True),
])

ENRICHED_TRADE_SCHEMA = StructType([
    StructField("trade_id", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DecimalType(18, 8), True),
    StructField("quantity", DecimalType(18, 8), True),
    StructField("side", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("ingest_time", TimestampType(), True),
    StructField("schema_version", StringType(), True),
    StructField("processing_timestamp", TimestampType(), True),
    StructField("pipeline_version", StringType(), True),
    StructField("partition_date", StringType(), True),
])


class SparkStreamingPipeline:
    def __init__(
        self,
        consumer_config: ConsumerConfig,
        producer_config: Optional[ProducerConfig] = None,
        checkpoint_dir: str = "s3a://quantstream/checkpoints/",
        minio_endpoint: str = "http://minio:9000",
        minio_access_key: str = "minioadmin",
        minio_secret_key: str = "minioadmin",
        watermark_seconds: int = 60,
        app_name: str = "QuantStreamPipeline",
    ) -> None:
        self.consumer_config = consumer_config
        self.producer_config = producer_config
        self.checkpoint_dir = checkpoint_dir
        self.watermark_seconds = watermark_seconds
        self.app_name = app_name

        builder = SparkSession.builder.appName(app_name)

        if minio_endpoint:
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
                .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
                .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            )

        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def read_from_redpanda(self, topic: str = "raw.trades") -> DataFrame:
        kafka_options = self.consumer_config.to_spark_kafka_options()
        kafka_options["subscribe"] = topic

        return (
            self.spark.readStream
            .format("kafka")
            .options(**kafka_options)
            .load()
        )

    @staticmethod
    def deserialize_events(df: DataFrame) -> DataFrame:
        """Deserialize Kafka value bytes into structured trade event columns."""

        def parse_trade_event(value: bytes) -> dict[str, Any] | None:
            try:
                envelope_data = json.loads(value.decode("utf-8"))
                payload = envelope_data.get("payload", {})

                event_time_str = payload.get("eventTime", "")
                ingest_time_str = payload.get("ingestTime", "")

                return {
                    "trade_id": payload.get("tradeId", ""),
                    "exchange": payload.get("exchange", ""),
                    "symbol": payload.get("symbol", ""),
                    "price": float(payload.get("price", 0)),
                    "quantity": float(payload.get("quantity", 0)),
                    "side": payload.get("side", ""),
                    "event_time": datetime.fromisoformat(event_time_str) if event_time_str else None,
                    "ingest_time": datetime.fromisoformat(ingest_time_str) if ingest_time_str else None,
                    "schema_version": str(payload.get("schemaVersion", 1)),
                }
            except Exception as e:
                logger.warning("Failed to parse trade event", extra={"error": str(e)})
                return None

        parse_udf = F.udf(parse_trade_event, TRADE_EVENT_SCHEMA)
        return df.select(parse_udf(F.col("value")).alias("data")).select("data.*").filter(
            F.col("event_time").isNotNull()
        )

    @staticmethod
    def apply_watermark(df: DataFrame, watermark_seconds: int = 60) -> DataFrame:
        return df.withWatermark("event_time", f"{watermark_seconds} seconds")

    def route_dead_letter(
        self, df: DataFrame, dlq_topic: str = "dead-letter.trades"
    ) -> None:
        """Identify unparseable events and route to dead-letter topic."""
        pass

    def write_bronze(
        self, df: DataFrame, output_path: str = "s3a://quantstream/bronze/trades/"
    ) -> None:
        df.withColumn("partition_date", F.date_format(F.col("event_time"), "yyyy-MM-dd")) \
          .writeStream \
          .outputMode("append") \
          .format("parquet") \
          .option("path", output_path) \
          .option("checkpointLocation", f"{self.checkpoint_dir}bronze") \
          .partitionBy("partition_date") \
          .trigger(processingTime="30 seconds") \
          .start()

    def write_silver(
        self, df: DataFrame, output_path: str = "s3a://quantstream/silver/trades/"
    ) -> None:
        enriched = df.withColumn(
            "processing_timestamp", F.current_timestamp()
        ).withColumn(
            "pipeline_version", F.lit("0.1.0")
        ).withColumn(
            "partition_date", F.date_format(F.col("event_time"), "yyyy-MM-dd")
        )

        enriched.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{self.checkpoint_dir}silver") \
            .partitionBy("partition_date", "symbol") \
            .trigger(processingTime="30 seconds") \
            .start()

    def run(self, topic: str = "raw.trades") -> None:
        logger.info("Starting streaming pipeline", extra={"topic": topic})

        raw_df = self.read_from_redpanda(topic)
        events_df = self.deserialize_events(raw_df)
        watermarked_df = self.apply_watermark(events_df, self.watermark_seconds)

        self.write_bronze(watermarked_df)
        self.write_silver(watermarked_df)

        self.spark.streams.awaitAnyTermination()

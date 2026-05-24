from __future__ import annotations

import os

import click

from quantstream.streaming.config import ConsumerConfig, ProducerConfig
from quantstream.streaming.pipeline import SparkStreamingPipeline


@click.command()
@click.option(
    "--bootstrap-servers",
    default=os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:19092"),
    help="Redpanda bootstrap servers",
)
@click.option(
    "--topic",
    default="raw.trades",
    help="Redpanda topic to consume from",
)
@click.option(
    "--checkpoint-dir",
    default=os.getenv("SPARK_CHECKPOINT_DIR", "s3a://quantstream/checkpoints/"),
    help="Spark checkpoint location",
)
@click.option(
    "--watermark-seconds",
    default=60,
    type=int,
    help="Event-time watermark in seconds",
)
def main(
    bootstrap_servers: str,
    topic: str,
    checkpoint_dir: str,
    watermark_seconds: int,
) -> None:
    consumer_config = ConsumerConfig(
        bootstrap_servers=bootstrap_servers,
        group_id="quantstream-spark-consumer",
    )

    pipeline = SparkStreamingPipeline(
        consumer_config=consumer_config,
        checkpoint_dir=checkpoint_dir,
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        watermark_seconds=watermark_seconds,
    )

    pipeline.run(topic=topic)


if __name__ == "__main__":
    main()

from __future__ import annotations

import click
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


@click.command()
@click.option("--input-path", default="s3a://quantstream/bronze/trades/", help="Path to bronze partition")
@click.option("--output-path", default="s3a://quantstream/bronze/compacted/", help="Output path for compacted files")
@click.option("--target-file-size-mb", default=128, help="Target file size in MB after compaction")
@click.option("--minio-endpoint", default="http://localhost:9000", help="MinIO endpoint")
@click.option("--minio-access-key", default="minioadmin", help="MinIO access key")
@click.option("--minio-secret-key", default="minioadmin", help="MinIO secret key")
def compact(
    input_path: str,
    output_path: str,
    target_file_size_mb: int,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> None:
    spark = (
        SparkSession.builder
        .appName("QuantStream-Compaction")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(input_path)
        row_count = df.count()
        logger.info("Read rows from bronze", extra={"rows": row_count, "path": input_path})

        approx_file_size = target_file_size_mb * 1024 * 1024
        num_partitions = max(1, int(row_count * 500 / approx_file_size))

        df.coalesce(num_partitions) \
          .write \
          .mode("overwrite") \
          .parquet(output_path)

        logger.info("Compaction complete", extra={
            "input_path": input_path,
            "output_path": output_path,
            "rows": row_count,
            "partitions": num_partitions,
        })
    finally:
        spark.stop()


if __name__ == "__main__":
    compact()

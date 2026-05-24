from __future__ import annotations

import click
import json
import logging
from pyspark.sql import SparkSession

from quantstream.data_quality.suites import (
    create_silver_trades_suite,
    create_gold_ohlc_suite,
)

logger = logging.getLogger(__name__)


@click.command()
@click.option("--suite", required=True, help="Validation suite name (silver.trades, gold.ohlc)")
@click.option("--path", required=True, help="Path to Parquet data to validate")
@click.option("--output", default="-", help="Output file for results (default: stdout)")
def validate(suite: str, path: str, output: str) -> None:
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName(f"QuantStream-DQ-{suite}")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(path)
        rows = [row.asDict() for row in df.collect()]

        if suite == "silver.trades":
            runner = create_silver_trades_suite()
        elif suite == "gold.ohlc":
            runner = create_gold_ohlc_suite()
        else:
            raise ValueError(f"Unknown suite: {suite}")

        result = runner.validate(suite, rows)

        if output == "-":
            click.echo(json.dumps(result, default=str, indent=2))
        else:
            with open(output, "w") as f:
                json.dump(result, f, default=str, indent=2)

        logger.info("Validation complete", extra={"suite": suite, "success": result["success"]})

        if not result["success"]:
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    validate()

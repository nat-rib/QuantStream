from __future__ import annotations

import asyncio
import os

import click

from quantstream.ingestion.client import run_ingestion


@click.command()
@click.option(
    "--symbols",
    default=os.getenv("BINANCE_SYMBOLS", "btcusdt,ethusdt"),
    help="Comma-separated list of trading symbols",
)
@click.option(
    "--bootstrap-servers",
    default=os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:19092"),
    help="Redpanda bootstrap servers",
)
def main(symbols: str, bootstrap_servers: str) -> None:
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    asyncio.run(run_ingestion(symbol_list, bootstrap_servers))


if __name__ == "__main__":
    main()

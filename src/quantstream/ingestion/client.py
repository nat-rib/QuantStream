from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import websockets
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

from quantstream.schemas.envelope import EventEnvelope
from quantstream.schemas.serialization import model_to_bytes
from quantstream.schemas.trade import TradeEvent
from quantstream.streaming.config import ProducerConfig

logger = logging.getLogger(__name__)

METRICS_PORT = 8001

events_total = Counter(
    "quantstream_ingestion_events_total",
    "Total trade events ingested",
    ["symbol", "exchange"],
)
errors_total = Counter(
    "quantstream_ingestion_errors_total",
    "Total ingestion errors",
    ["error_type"],
)
connection_status = Gauge(
    "quantstream_ingestion_connection_status",
    "WebSocket connection status (1=connected, 0=disconnected)",
    ["exchange"],
)

MAX_RETRIES = 10
BASE_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0


class BinanceWebSocketClient:
    def __init__(self, symbols: list[str], ws_url: str = "wss://stream.binance.com:9443/ws") -> None:
        self.symbols = [s.lower() for s in symbols]
        self.ws_url = ws_url
        self._running = False
        self._producer: RedpandaProducer | None = None

    def set_producer(self, producer: RedpandaProducer) -> None:
        self._producer = producer

    async def connect(self) -> None:
        self._running = True
        streams = "/".join(f"{s}@trade" for s in self.symbols)
        url = f"{self.ws_url}/{streams}"

        retries = 0
        while self._running and retries < MAX_RETRIES:
            try:
                connection_status.labels(exchange="binance").set(1)
                logger.info("Connecting to Binance WebSocket", extra={"url": url, "symbols": self.symbols})
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    retries = 0
                    logger.info("Connected to Binance WebSocket")
                    connection_status.labels(exchange="binance").set(1)
                    await self._receive_loop(ws)
            except websockets.ConnectionClosed as e:
                connection_status.labels(exchange="binance").set(0)
                logger.warning("WebSocket connection closed", extra={"code": e.code, "reason": str(e.reason)})
            except Exception as e:
                connection_status.labels(exchange="binance").set(0)
                logger.error("WebSocket connection error", extra={"error": str(e)})
                errors_total.labels(error_type="connection_error").inc()

            if self._running:
                retries += 1
                delay = min(BASE_BACKOFF_SECONDS * (2 ** (retries - 1)), MAX_BACKOFF_SECONDS)
                jitter = delay * 0.1 * random.random()
                wait = delay + jitter
                logger.info("Reconnecting", extra={"retry": retries, "delay_seconds": wait})
                await asyncio.sleep(wait)

        if retries >= MAX_RETRIES:
            logger.critical("Max connection retries exhausted")
            connection_status.labels(exchange="binance").set(0)

    async def _receive_loop(self, ws: websockets.WebSocketClientProtocol) -> None:
        async for message in ws:
            if not self._running:
                break
            try:
                await self._handle_message(message)
            except Exception as e:
                logger.error("Error handling message", extra={"error": str(e)})
                errors_total.labels(error_type="message_handling").inc()

    async def _handle_message(self, raw_message: str | bytes) -> None:
        data: dict[str, Any] = json.loads(raw_message)
        if "data" not in data:
            return

        trade_data = data["data"]
        trade_event = TradeEvent(
            trade_id=uuid.uuid4(),
            exchange="binance",
            symbol=trade_data.get("s", "UNKNOWN"),
            price=Decimal(str(trade_data.get("p", 0))),
            quantity=Decimal(str(trade_data.get("q", 0))),
            side="BUY" if trade_data.get("m", True) is False else "SELL",
            event_time=datetime.fromtimestamp(
                trade_data.get("T", 0) / 1000, tz=timezone.utc
            ),
            ingest_time=datetime.now(timezone.utc),
            schema_version=1,
        )

        events_total.labels(symbol=trade_event.symbol, exchange=trade_event.exchange).inc()

        if self._producer:
            self._producer.publish_trade(trade_event)

    async def shutdown(self) -> None:
        logger.info("Shutting down ingestion client")
        self._running = False
        if self._producer:
            self._producer.flush()


class RedpandaProducer:
    def __init__(self, config: ProducerConfig) -> None:
        self.config = config
        self.topic = "raw.trades"
        self._producer = Producer(config.to_confluent_config())

    def publish_trade(self, trade: TradeEvent) -> None:
        envelope = EventEnvelope.wrap_trade(trade)
        key = trade.symbol.encode("utf-8")
        value = model_to_bytes(envelope)

        self._producer.produce(
            topic=self.topic,
            key=key,
            value=value,
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> None:
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("Producer flush incomplete", extra={"remaining_messages": remaining})

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            logger.error("Message delivery failed", extra={"error": str(err)})
            errors_total.labels(error_type="delivery_failure").inc()
        else:
            logger.debug("Message delivered", extra={"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()})


async def run_ingestion(symbols: list[str], bootstrap_servers: str) -> None:
    logger.info("Starting ingestion producer", extra={"symbols": symbols, "brokers": bootstrap_servers})

    start_http_server(METRICS_PORT)
    producer = RedpandaProducer(ProducerConfig(bootstrap_servers=bootstrap_servers))
    client = BinanceWebSocketClient(symbols=symbols)
    client.set_producer(producer)

    loop = asyncio.get_running_loop()

    def shutdown_handler() -> None:
        logger.info("Received shutdown signal")
        asyncio.ensure_future(client.shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown_handler)
        except NotImplementedError:
            pass

    try:
        await client.connect()
    except asyncio.CancelledError:
        pass
    finally:
        producer.flush()
        logger.info("Ingestion producer stopped")

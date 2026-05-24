from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    client_id: str = "quantstream"


@dataclass
class ProducerConfig(KafkaConfig):
    acks: str = "all"
    enable_idempotence: bool = True
    compression_type: str = "snappy"
    max_in_flight_requests_per_connection: int = 5
    retries: int = 3
    request_timeout_ms: int = 30000

    def to_confluent_config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": self.acks,
            "enable.idempotence": self.enable_idempotence,
            "compression.type": self.compression_type,
            "max.in.flight.requests.per.connection": self.max_in_flight_requests_per_connection,
            "retries": self.retries,
            "request.timeout.ms": self.request_timeout_ms,
        }


@dataclass
class ConsumerConfig(KafkaConfig):
    group_id: str = "quantstream-spark-consumer"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    isolation_level: str = "read_committed"
    max_poll_interval_ms: int = 600000

    def to_confluent_config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
            "isolation.level": self.isolation_level,
            "max.poll.interval.ms": self.max_poll_interval_ms,
        }

    def to_spark_kafka_options(self) -> dict[str, str]:
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": "",
            "kafka.group.id": self.group_id,
            "startingOffsets": self.auto_offset_reset,
            "failOnDataLoss": "false",
            "kafka.isolation.level": self.isolation_level,
            "kafka.max.poll.interval.ms": str(self.max_poll_interval_ms),
        }

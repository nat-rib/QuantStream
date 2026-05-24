from __future__ import annotations

from quantstream.streaming.config import ConsumerConfig, KafkaConfig, ProducerConfig


class TestKafkaConfig:
    def test_defaults(self):
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        assert config.bootstrap_servers == "localhost:9092"
        assert config.client_id == "quantstream"


class TestProducerConfig:
    def test_defaults(self):
        config = ProducerConfig(bootstrap_servers="localhost:9092")
        assert config.bootstrap_servers == "localhost:9092"
        assert config.enable_idempotence is True
        assert config.acks == "all"
        assert config.compression_type == "snappy"

    def test_to_confluent_config(self):
        config = ProducerConfig(bootstrap_servers="redpanda:9092")
        result = config.to_confluent_config()
        assert result["bootstrap.servers"] == "redpanda:9092"
        assert result["enable.idempotence"] is True
        assert result["acks"] == "all"
        assert result["retries"] == 3

    def test_custom_config(self):
        config = ProducerConfig(
            bootstrap_servers="custom:9092",
            client_id="test-producer",
            retries=5,
            request_timeout_ms=60000,
        )
        result = config.to_confluent_config()
        assert result["bootstrap.servers"] == "custom:9092"
        assert result["client.id"] == "test-producer"
        assert result["retries"] == 5
        assert result["request.timeout.ms"] == 60000


class TestConsumerConfig:
    def test_defaults(self):
        config = ConsumerConfig(bootstrap_servers="localhost:9092")
        assert config.group_id == "quantstream-spark-consumer"
        assert config.auto_offset_reset == "earliest"
        assert config.enable_auto_commit is False

    def test_to_confluent_config(self):
        config = ConsumerConfig(bootstrap_servers="redpanda:9092")
        result = config.to_confluent_config()
        assert result["group.id"] == "quantstream-spark-consumer"
        assert result["isolation.level"] == "read_committed"

    def test_to_spark_kafka_options(self):
        config = ConsumerConfig(bootstrap_servers="redpanda:9092")
        result = config.to_spark_kafka_options()
        assert result["kafka.bootstrap.servers"] == "redpanda:9092"
        assert result["kafka.group.id"] == "quantstream-spark-consumer"
        assert result["failOnDataLoss"] == "false"
        assert "kafka.isolation.level" in result

    def test_custom_group_id(self):
        config = ConsumerConfig(
            bootstrap_servers="localhost:9092", group_id="custom-group"
        )
        assert config.group_id == "custom-group"
        result = config.to_spark_kafka_options()
        assert result["kafka.group.id"] == "custom-group"

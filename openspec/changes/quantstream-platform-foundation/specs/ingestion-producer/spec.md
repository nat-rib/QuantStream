## ADDED Requirements

### Requirement: Binance WebSocket connection
The system SHALL establish a persistent WebSocket connection to Binance's public trade stream endpoint for a configured list of trading symbols.

#### Scenario: Successful connection to trade stream
- **WHEN** the ingestion producer starts with a valid symbol list (e.g. ["btcusdt", "ethusdt"])
- **THEN** it SHALL connect to `wss://stream.binance.com:9443/ws/<symbol>@trade` for each symbol and begin receiving trade events

### Requirement: Reconnection with exponential backoff
The system SHALL automatically reconnect on WebSocket disconnection using exponential backoff with jitter, with a maximum retry cap.

#### Scenario: WebSocket disconnects and reconnects
- **WHEN** the WebSocket connection drops unexpectedly
- **THEN** the client SHALL wait for an exponentially increasing interval (with jitter) before attempting reconnection, up to a maximum of 60 seconds between attempts

#### Scenario: Maximum retries exhausted
- **WHEN** reconnection attempts exceed the configured maximum retry count (default 10)
- **THEN** the client SHALL log a critical error, increment a failure metric, and exit with a non-zero status code

### Requirement: Event publishing to Redpanda
The system SHALL publish each received trade event as a message to a Redpanda topic named `raw.trades`, using the trading symbol as the partition key for ordering guarantees.

#### Scenario: Trade event published successfully
- **WHEN** a valid trade event is received from the WebSocket
- **THEN** it SHALL be serialized, wrapped in an EventEnvelope, and published to the `raw.trades` Redpanda topic with acknowledgment required

#### Scenario: Publish failure is retried
- **WHEN** a publish to Redpanda fails with a transient error
- **THEN** the producer SHALL retry the publish up to 3 times with linear backoff before logging a permanent failure

### Requirement: Idempotent producer configuration
The Redpanda producer SHALL be configured with `enable.idempotence=true` to prevent duplicate messages during retry scenarios.

#### Scenario: Producer retry does not create duplicates
- **WHEN** a producer retries a message due to a network timeout
- **THEN** Redpanda SHALL ensure the message is stored exactly once in the topic partition

### Requirement: Throughput and health metrics
The ingestion producer SHALL expose Prometheus metrics for events ingested per second, connection status, error counts by type, and producer latency.

#### Scenario: Metrics endpoint is scraped
- **WHEN** Prometheus scrapes the producer's metrics endpoint
- **THEN** it SHALL receive `quantstream_ingestion_events_total`, `quantstream_ingestion_errors_total`, and `quantstream_ingestion_connection_status` metrics

### Requirement: Graceful shutdown
The ingestion producer SHALL handle SIGTERM and SIGINT signals by closing the WebSocket connection, flushing pending messages to Redpanda, and exiting cleanly.

#### Scenario: Producer receives SIGTERM
- **WHEN** the producer process receives a SIGTERM signal
- **THEN** it SHALL stop accepting new events, flush the producer buffer, close the WebSocket, and exit with code 0

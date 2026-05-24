## ADDED Requirements

### Requirement: Spark Structured Streaming read from Redpanda
The system SHALL configure a Spark Structured Streaming job that reads from the `raw.trades` Redpanda topic using the Kafka connector, with `failOnDataLoss=false` and a starting offset of `earliest` on first run.

#### Scenario: Streaming query reads events from Redpanda
- **WHEN** the Spark job starts and events exist in the `raw.trades` topic
- **THEN** the streaming DataFrame SHALL contain rows with columns matching the trade event schema

### Requirement: Event-time watermarking
The system SHALL use `event_time` as the event-time column with a watermark of 60 seconds, enabling windowed aggregations and late-event handling.

#### Scenario: Events within watermark are processed
- **WHEN** a trade event arrives with `event_time` within the 60-second watermark window
- **THEN** it SHALL be processed normally through the pipeline

#### Scenario: Events beyond watermark are dropped
- **WHEN** a trade event arrives with `event_time` more than 60 seconds behind the current watermark
- **THEN** it SHALL be dropped from the stream and counted in the `quantstream_streaming_late_events_total` metric

### Requirement: Checkpointing with exactly-once semantics
The system SHALL configure checkpointing to a durable location (MinIO bucket `checkpoints/`) at 30-second intervals, enabling exactly-once processing across restarts.

#### Scenario: Consumer recovers from checkpoint after crash
- **WHEN** the Spark job crashes and restarts
- **THEN** it SHALL read the checkpoint from MinIO, resume from the last committed offset, and not duplicate already-processed events in the bronze storage

### Requirement: Dead-letter routing for unparseable events
The system SHALL catch deserialization and validation errors during stream processing and route the raw message bytes to the `dead-letter.trades` topic with failure metadata in message headers.

#### Scenario: Malformed event is routed to dead-letter
- **WHEN** a message in `raw.trades` contains non-JSON bytes that cannot be parsed
- **THEN** the raw bytes SHALL be published to `dead-letter.trades` with a header `failure_reason: deserialization_error`, and the pipeline SHALL continue processing subsequent messages

### Requirement: Throughput and lag metrics
The Spark consumer SHALL expose metrics for events processed per second, consumer lag per topic partition, late-event count, and dead-letter event count, all prefixed with `quantstream_streaming_`.

#### Scenario: Consumer lag is measurable
- **WHEN** Prometheus scrapes the Spark metrics endpoint
- **THEN** `quantstream_streaming_lag_seconds` SHALL reflect the time delta between the latest Redpanda message timestamp and the latest processed event timestamp

### Requirement: Pipeline output contract
The Spark streaming pipeline SHALL output processed events as DataFrames with a well-defined schema to downstream bronze and silver writers, enabling independent evolution of output stages.

#### Scenario: Downstream writer reads processed DataFrame
- **WHEN** the streaming pipeline processes a batch of events
- **THEN** the output DataFrame SHALL have columns matching the EnrichedTradeEvent schema (all trade fields plus processing_timestamp, pipeline_version, partition_date)

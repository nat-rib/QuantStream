## ADDED Requirements

### Requirement: Redpanda broker in Docker Compose
The system SHALL define a Redpanda service in Docker Compose with single-broker configuration, exposed Kafka API port (9092), schema registry port (8081), and admin API port (9644).

#### Scenario: Redpanda starts successfully
- **WHEN** Docker Compose starts the Redpanda service
- **THEN** the broker SHALL be reachable at `localhost:9092` and the admin API SHALL report healthy status at `localhost:9644`

### Requirement: Topic auto-creation script
The system SHALL provide a script or init container that creates required Redpanda topics on first boot with appropriate partition counts and retention policies.

#### Scenario: Topics are created on startup
- **WHEN** the platform starts for the first time
- **THEN** topics `raw.trades`, `dead-letter.trades` SHALL exist with configured partitions and retention settings

### Requirement: Topic partitioning strategy
The `raw.trades` topic SHALL be partitioned by trading symbol (e.g., `BTCUSDT`, `ETHUSDT`) with 3 partitions default, ensuring events for the same symbol arrive in order.

#### Scenario: Events for same symbol are ordered
- **WHEN** events for `BTCUSDT` are produced with symbol as the partition key
- **THEN** all `BTCUSDT` events SHALL be routed to the same partition, preserving insertion order

### Requirement: Retention policies
The `raw.trades` topic SHALL retain messages for 7 days, and the `dead-letter.trades` topic SHALL retain messages for 30 days.

#### Scenario: Old messages are cleaned up
- **WHEN** messages in `raw.trades` exceed 7 days of age
- **THEN** Redpanda SHALL delete them based on the configured retention policy

### Requirement: Dead-letter topic
The system SHALL define a `dead-letter.trades` topic with the same partition count as `raw.trades`, used to store events that fail processing after all retries.

#### Scenario: Unprocessable event routed to dead-letter
- **WHEN** a Spark consumer cannot parse or validate an event after retries
- **THEN** the raw event bytes SHALL be published to `dead-letter.trades` with headers describing the failure reason

### Requirement: Consumer group management
The Spark consumer SHALL use a stable consumer group ID (`quantstream-spark-consumer`) to enable offset tracking and checkpoint recovery across restarts.

#### Scenario: Consumer resumes from last committed offset
- **WHEN** the Spark consumer restarts after a crash
- **THEN** it SHALL read from the last committed offset for its consumer group, not from the beginning of the topic

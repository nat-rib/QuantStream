## ADDED Requirements

### Requirement: Schema validation at silver boundary
The system SHALL validate every event entering the silver layer against its declared schema version using Pydantic model validation. Events that fail validation SHALL be routed to the dead-letter topic.

#### Scenario: Valid event passes to silver
- **WHEN** a bronze event with valid schema is processed by the silver transformation
- **THEN** it SHALL pass validation and proceed to normalization

#### Scenario: Invalid event is routed to dead-letter
- **WHEN** an event fails Pydantic validation (e.g., negative price)
- **THEN** the raw event SHALL be published to `dead-letter.trades` with `failure_reason: schema_validation_error` and the silver pipeline SHALL continue processing

### Requirement: Field normalization
The system SHALL normalize event fields to a consistent format: `symbol` in uppercase, `price` and `quantity` as Decimal types with fixed precision, `event_time` and `ingest_time` as UTC datetime, and all string fields trimmed of whitespace.

#### Scenario: Symbol is uppercased
- **WHEN** an event arrives with `symbol` = "btcusdt"
- **THEN** the normalized event SHALL have `symbol` = "BTCUSDT"

### Requirement: Deduplication by trade_id
The system SHALL deduplicate events by `trade_id` within the silver partition, keeping the latest event by `event_time` when duplicates are detected.

#### Scenario: Duplicate trade_id is deduplicated
- **WHEN** two events with the same `trade_id` arrive (e.g., due to at-least-once delivery)
- **THEN** only the event with the later `ingest_time` SHALL survive to silver storage, and the deduplication SHALL be counted in a metric

### Requirement: Business rule validation
The system SHALL validate business rules: `price > 0`, `quantity > 0`, `side` is one of `["BUY", "SELL"]`, `event_time` is not in the future (within clock skew tolerance of 5 seconds).

#### Scenario: Negative price event is rejected
- **WHEN** an event with `price <= 0` is processed
- **THEN** the event SHALL be routed to dead-letter with `failure_reason: business_rule_violation` and specific field detail

### Requirement: Enriched event schema
Silver events SHALL include additional processing metadata fields: `processing_timestamp` (when silver processing occurred), `pipeline_version` (semantic version of the silver pipeline), and `partition_date` (date used for silver partitioning).

#### Scenario: Silver event includes processing metadata
- **WHEN** an event is written to silver storage
- **THEN** the Parquet row SHALL include `processing_timestamp`, `pipeline_version`, and `partition_date` in addition to all normalized trade fields

### Requirement: Silver partition strategy
Silver storage SHALL partition data by `event_date=<YYYY-MM-DD>/symbol=<SYMBOL>/`, enabling efficient symbol-scoped queries.

#### Scenario: Silver partition directory structure
- **WHEN** silver events are written for `BTCUSDT` on `2026-05-24`
- **THEN** they SHALL be stored at `s3a://quantstream/silver/trades/event_date=2026-05-24/symbol=BTCUSDT/`

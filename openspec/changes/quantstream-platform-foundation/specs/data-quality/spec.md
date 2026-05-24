## ADDED Requirements

### Requirement: Great Expectations context initialization
The system SHALL include a Great Expectations project initialized with a filesystem-based data context, configured to validate data at the silver and gold pipeline boundaries.

#### Scenario: GX context loads successfully
- **WHEN** the Great Expectations context is initialized from the project's `great_expectations/` directory
- **THEN** it SHALL load without errors and be ready to run validations

### Requirement: Silver trade event expectations
The system SHALL define an Expectation Suite for silver trade events validating: schema column set and types, `trade_id` uniqueness within a batch, `price` between 0 and 1,000,000, `quantity > 0`, `side` in `["BUY", "SELL"]`, `event_time` not null, and `partition_date` matching the batch date.

#### Scenario: Valid silver batch passes all expectations
- **WHEN** a checkpoint runs the silver trade event suite against a valid Parquet batch
- **THEN** all expectations SHALL pass and the validation result SHALL have `success: true`

#### Scenario: Silver batch with duplicate trade_ids fails
- **WHEN** a checkpoint runs against a silver batch containing duplicate `trade_id` values
- **THEN** the uniqueness expectation SHALL fail and the validation result SHALL report the failure

### Requirement: Gold OHLC expectations
The system SHALL define an Expectation Suite for gold OHLC data validating: `high_price >= low_price`, `open_price` and `close_price` between `low_price` and `high_price`, `volume >= 0`, `trade_count >= 0`, and `window_start` before `window_end`.

#### Scenario: Valid gold OHLC batch passes
- **WHEN** a checkpoint runs the gold OHLC suite against a valid Parquet batch
- **THEN** all expectations SHALL pass

#### Scenario: OHLC with high < low fails
- **WHEN** an OHLC row has `high_price < low_price`
- **THEN** the `high_price >= low_price` expectation SHALL fail with row-level details

### Requirement: Data freshness checks
The system SHALL define freshness expectations ensuring that the latest `event_time` in a partition is within the expected recency window, defaulting to 120 seconds for silver data.

#### Scenario: Stale data triggers freshness alert
- **WHEN** the latest event in a silver partition has `event_time` more than 120 seconds in the past
- **THEN** the freshness expectation SHALL fail with details on the lag duration

### Requirement: Pipeline integration for validation
The system SHALL provide a Python validation runner that reads Parquet batches from silver or gold storage and executes the corresponding GX checkpoint, emitting validation results as Prometheus metrics and structured logs.

#### Scenario: Validation runner succeeds on clean data
- **WHEN** the validation runner is invoked for `silver.trades` partition `event_date=2026-05-24`
- **THEN** it SHALL execute the checkpoint, log the result, and emit `quantstream_data_quality_checks_total{status="pass"}` metric

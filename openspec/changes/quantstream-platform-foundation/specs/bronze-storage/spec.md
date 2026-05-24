## ADDED Requirements

### Requirement: Append-only Parquet writes to MinIO
The system SHALL write raw trade events to MinIO in Apache Parquet format under the path `s3a://quantstream/bronze/trades/`, with append-only semantics — once written, data is never modified or deleted.

#### Scenario: Raw events are written to MinIO
- **WHEN** the Spark streaming pipeline processes a micro-batch of trade events
- **THEN** the events SHALL be written as Parquet files to the bronze storage path in MinIO

### Requirement: Date-based partitioning
Bronze storage SHALL partition data by event date using the convention `event_date=<YYYY-MM-DD>/`, enabling efficient time-range queries and retention management.

#### Scenario: Events from different dates go to different partitions
- **WHEN** a micro-batch contains events with `event_time` spanning two different dates
- **THEN** the events SHALL be written to separate partition directories (`event_date=2026-05-24/`, `event_date=2026-05-25/`)

### Requirement: Immutable storage contract
Once written, bronze partition files SHALL NOT be modified or overwritten. Any reprocessing SHALL write new partition files or use a separate output path.

#### Scenario: Reprocessing does not overwrite bronze data
- **WHEN** a consumer replay is triggered from an earlier offset
- **THEN** the reprocessing SHALL either write to a new output location or append to bronze with deduplication handled downstream

### Requirement: Partition compaction utility
The system SHALL provide a compaction utility that merges small Parquet files within a bronze partition into larger files for query efficiency, without changing the logical data content.

#### Scenario: Small files in a partition are compacted
- **WHEN** the compaction utility runs on a bronze partition containing many small Parquet files from frequent micro-batches
- **THEN** the utility SHALL produce fewer, larger Parquet files with identical row content

### Requirement: MinIO bucket and access configuration
The system SHALL define MinIO service configuration in Docker Compose with a `quantstream` bucket, access key, secret key, and S3-compatible endpoint for Spark's S3A filesystem connector.

#### Scenario: Spark can read and write to MinIO
- **WHEN** the Spark job attempts to write Parquet to `s3a://quantstream/bronze/trades/`
- **THEN** the write SHALL succeed, and the Parquet files SHALL be readable via the MinIO console or S3 API

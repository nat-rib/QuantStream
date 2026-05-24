# Architecture Decision Records

## ADR-001: Redpanda over Apache Kafka

**Status**: Accepted  
**Date**: 2026-05-24

### Context
The platform needs a streaming message broker to decouple ingestion from processing. Apache Kafka is the industry standard, but its operational complexity (Zookeeper, multiple containers, complex networking) adds friction for a local-first platform.

### Decision
Use Redpanda, a Kafka-compatible broker with a single-binary deployment and no Zookeeper dependency.

### Consequences
- **Positive**: Single-container deployment, lower memory footprint, same Kafka API
- **Positive**: Spark Kafka connector works without modification
- **Positive**: `rpk` CLI provides better developer experience than Kafka scripts
- **Negative**: Smaller community than Kafka (but compatible API mitigates this)

---

## ADR-002: Spark Structured Streaming over Flink/Faust/Bytewax

**Status**: Accepted  
**Date**: 2026-05-24

### Context
The platform needs a stream processor capable of exactly-once semantics, watermarking, and dead-letter handling.

### Decision
Use Apache Spark Structured Streaming with PySpark.

### Consequences
- **Positive**: Built-in Kafka connector, checkpointing, watermarking, windowing
- **Positive**: DataFrame API integrates naturally with Parquet reads/writes
- **Positive**: Well-known technology — demonstrates transferable skills
- **Negative**: PySpark introduces JVM overhead and serialization cost (acceptable at Phase 1 throughput)
- **Negative**: Local mode has resource constraints (adequate for single exchange stream)

---

## ADR-003: MinIO over local filesystem

**Status**: Accepted  
**Date**: 2026-05-24

### Context
Processed data must be stored in a durable, queryable format. Local filesystem storage works but doesn't demonstrate production patterns.

### Decision
Use MinIO, an S3-compatible object store in a single container.

### Consequences
- **Positive**: Demonstrates cloud object storage patterns (standard in DE)
- **Positive**: Separates compute (Spark) from storage (MinIO)
- **Positive**: Enables independent storage access from multiple consumers
- **Negative**: Adds one additional container to the Docker Compose stack

---

## ADR-004: DuckDB for analytical queries

**Status**: Accepted  
**Date**: 2026-05-24

### Context
The platform needs an analytical query engine to serve API requests. Options: PostgreSQL, ClickHouse, DuckDB.

### Decision
Use DuckDB, an in-process OLAP database.

### Consequences
- **Positive**: Zero-config deployment — no separate server process
- **Positive**: Direct Parquet reading with zero-copy where possible
- **Positive**: Excellent Python integration and FastAPI compatibility
- **Positive**: Columnar execution engine optimized for analytical workloads
- **Negative**: Not designed for high-concurrency workloads (acceptable for portfolio use)

---

## ADR-005: dbt for analytical transformations

**Status**: Accepted  
**Date**: 2026-05-24

### Context
Gold storage produces aggregated Parquet datasets. These need further transformation for serving layer consumption. Options: pure Spark SQL, dbt, hand-written SQL.

### Decision
Use dbt with the dbt-duckdb adapter.

### Consequences
- **Positive**: SQL-based modeling with testing, documentation, and lineage
- **Positive**: dbt is the industry standard for data transformation — critical DE skill
- **Positive**: Clear separation between streaming transforms (Spark) and analytical transforms (dbt)
- **Negative**: Adds a separate tool to learn and configure

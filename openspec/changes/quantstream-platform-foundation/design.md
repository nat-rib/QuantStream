## Context

QuantStream is a greenfield project — no existing codebase, no legacy constraints. The platform must run entirely on a developer's local machine using Docker Compose, yet demonstrate production-grade engineering practices. Phase 1 is scoped to real-time crypto trade event ingestion from Binance, processed through a medallion architecture (bronze/silver/gold), with a serving API and full observability.

## Goals / Non-Goals

**Goals:**
- Ingest real-time trade events from Binance WebSocket streams
- Process events with exactly-once semantics through a streaming pipeline
- Store data in a layered medallion architecture (bronze/silver/gold) using Parquet on MinIO
- Serve analytical queries through DuckDB exposed via FastAPI
- Provide full observability (metrics, dashboards, health checks)
- Enforce data quality with schema validation and business rule checks
- Demonstrate senior-level engineering: checkpointing, late-event handling, dead-letter routing, idempotency, replay
- Be fully reproducible locally with a single `docker compose up`

**Non-Goals:**
- Multi-exchange support (Binance only in Phase 1)
- Order book snapshots or ticker streams (trade events only)
- Kubernetes, cloud deployment, Terraform, Airflow, Flink
- Real trading execution or strategy engine
- Multi-region or production cloud deployment
- Backfill of historical data (streaming only for Phase 1)

## Decisions

### Redpanda over Apache Kafka
Redpanda is a Kafka-compatible broker with a single-binary deployment, no Zookeeper dependency, and significantly lower operational overhead. It uses the same Kafka protocol, so existing Spark Kafka connector works without modification. For a local-first platform, Redpanda's Docker experience is dramatically simpler — one container vs. Kafka+Zookeeper requiring multiple containers with complex networking.

### Spark Structured Streaming over Flink / Faust / Bytewax
Spark Structured Streaming was chosen as the primary stream processor because:
1. It provides the Kafka (Redpanda) connector out of the box
2. Exactly-once semantics are built-in via checkpointing
3. Watermarking and event-time windowing are first-class features
4. The DataFrame API integrates naturally with Parquet reads/writes
5. It's a well-known technology that demonstrates transferable skills

Alternatives considered: Flink (excellent but complex to run locally, heavier resource footprint); Faust (Python-native but less mature, limited connector ecosystem); Bytewax (promising but newer ecosystem).

### MinIO over local filesystem
MinIO provides an S3-compatible object store that runs in a single container. Using S3-compatible storage:
1. Demonstrates production patterns (cloud object storage is the norm)
2. Enables separation of compute (Spark) and storage (MinIO)
3. Makes the storage layer independently addressable (other consumers can read Parquet directly)
4. Parquet format with S3 path conventions follows industry standards

### DuckDB for analytics
DuckDB is an in-process OLAP database that excels at analytical queries over Parquet files. It was chosen because:
1. Direct Parquet file reading with zero ETL — no separate data loading step
2. Columnar execution engine optimized for analytical workloads
3. Excellent Python integration and FastAPI compatibility
4. No separate server process needed (unlike PostgreSQL, ClickHouse)
5. Well-suited for local-first architectures

### dbt with DuckDB adapter
dbt provides the transformation layer (T in ELT) with:
1. SQL-based modeling, documentation, and lineage
2. Testing framework for data quality assertions
3. Incremental materialization patterns
4. The dbt-duckdb adapter enables direct DuckDB execution

dbt is the industry standard for data transformation and demonstrates a key data engineering skill.

### Python (not Java/Scala) for Spark
While Spark's native languages are JVM-based, using PySpark demonstrates that the engineer understands Spark's architecture while keeping the entire stack in a single language. For a portfolio project, a unified Python codebase reduces cognitive load and is more accessible. PySpark performance is adequate for the throughput of a single exchange's trade stream.

### Medallion Architecture (Bronze/Silver/Gold)
The three-tier storage pattern provides clear separation of concerns:
- **Bronze**: Raw, immutable, append-only — acts as the source of truth and replay source
- **Silver**: Validated, normalized, deduplicated — the trusted operational layer
- **Gold**: Aggregated, business-level datasets — ready for serving

This separation enables replay from bronze, data quality enforcement at silver, and independent evolution of gold analytics.

### Schema Versioning
Trade events carry a `schema_version` field. When schemas evolve:
1. New events are produced with incremented version
2. Silver layer's validation adapts per schema version
3. Schema versions are registered in a lightweight schema registry (simple Python dictionary of version → Pydantic model)
4. Forward compatibility: old consumers ignore unknown fields

### Partition Strategy
- Bronze: `event_date=<YYYY-MM-DD>/` — date-based partitioning for retention management
- Silver: `event_date=<YYYY-MM-DD>/symbol=<SYMBOL>/` — adds symbol for query pruning
- Gold: `dataset=<name>/event_date=<YYYY-MM-DD>/` — dataset-level organization

### Error Handling and Resilience

| Component | Failure Mode | Mitigation |
|---|---|---|
| WebSocket client | Disconnect | Exponential backoff reconnection with jitter |
| WebSocket client | Malformed message | Log, increment error counter, skip — do not crash |
| Redpanda | Broker restart | Producer retry with idempotency (enable.idempotence=true) |
| Spark consumer | Crash | Checkpoint recovery, resume from last committed offset |
| Spark consumer | Unparseable event | Route to dead-letter topic, continue processing |
| Spark consumer | Late event (>watermark) | Drop with metrics counter, log for audit |
| Spark consumer | Duplicate trade_id | Deduplicate in silver layer, keep latest by event_time |
| MinIO | Write failure | Spark task retry, checkpoint prevents data loss |
| FastAPI | DuckDB query failure | Graceful error response, Prometheus error counter |

### Metric Naming Convention
```
quantstream_ingestion_events_total{symbol,exchange}
quantstream_ingestion_errors_total{error_type}
quantstream_streaming_lag_seconds{consumer_group,topic}
quantstream_streaming_events_processed_total{stage}
quantstream_streaming_late_events_total{symbol}
quantstream_streaming_dlq_events_total{reason}
quantstream_api_request_duration_seconds{endpoint,method}
quantstream_service_health{component}
```

## Risks / Trade-offs

- **Risk**: Spark's local mode may have performance limitations under sustained high-volume streams → **Mitigation**: Binance trade streams produce moderate volume (~10-100 events/sec per symbol); Spark local mode handles this comfortably
- **Risk**: Schema evolution creates complexity in the silver layer → **Mitigation**: Version-aware validation functions, schema registry, and explicit evolution tests
- **Risk**: Docker Compose resource footprint may be heavy for some developer machines → **Mitigation**: Resource limits in compose file, optional component profiles, documentation of minimum specs
- **Risk**: PySpark introduces serialization overhead vs. Scala Spark → **Mitigation**: Acceptable trade-off for unified Python codebase; not a bottleneck at Phase 1 throughput
- **Risk**: DuckDB may not handle concurrent connections well → **Mitigation**: DuckDB supports concurrent reads; single-writer limitation is acceptable for a serving layer with read-heavy workload
- **Trade-off**: Using dbt (another system) vs. pure Spark transformations → **Decision**: dbt is the industry standard for analytical transformations and demonstrates a critical skill; Spark handles streaming transforms, dbt handles batch analytical models
- **Trade-off**: MinIO adds operational complexity vs. local filesystem → **Decision**: S3-compatible storage patterns are a standard expectation in data engineering portfolios; the abstraction is worth the small complexity cost

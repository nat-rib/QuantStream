## Why

QuantStream is a production-grade crypto market data engineering platform designed to demonstrate senior-level data engineering expertise. It addresses the gap between toy ETL projects and cloud-only platforms by providing a realistic, local-first streaming data platform that is portfolio-worthy, interview-defensible, and incrementally expandable. The platform ingests real-time crypto trade events from Binance, processes them through a medallion architecture (bronze/silver/gold), and serves analytical results via API — all runnable locally with Docker Compose.

## What Changes

- New project: QuantStream real-time crypto market data engineering platform
- Streaming ingestion from Binance WebSocket API producing trade events to Redpanda
- Spark Structured Streaming consumer processing events with event-time semantics
- Bronze storage: immutable raw Parquet events in MinIO
- Silver storage: validated, normalized, deduplicated events with schema enforcement
- Gold storage: aggregated analytical datasets via Spark and dbt
- DuckDB for interactive analytical queries
- FastAPI serving layer exposing operational and analytical endpoints
- Prometheus + Grafana observability stack with throughput, lag, and health metrics
- Great Expectations data quality validation suites
- Full test pyramid: unit, integration, smoke, and contract tests
- CI/CD via GitHub Actions with linting (ruff/black/mypy) and testing
- Comprehensive documentation: ADRs, runbooks, architecture docs

## Capabilities

### New Capabilities

- `repository-foundation`: Project scaffolding, Python package structure, dependency management (poetry), Docker Compose base configuration, development tooling (ruff, black, mypy, pre-commit), and environment setup automation
- `event-contracts`: Explicit event schemas (trade, future event types), schema versioning strategy, schema registry patterns, data contract definitions using Pydantic models, and serialization/deserialization utilities
- `ingestion-producer`: Binance WebSocket client with reconnection handling, backpressure-aware event publishing to Redpanda, idempotent producer configuration, and health/throughput instrumentation
- `messaging-infrastructure`: Redpanda broker configuration (Docker Compose), topic creation and partitioning strategy, retention policies, dead-letter topic setup, and consumer group management
- `streaming-consumer`: Spark Structured Streaming pipeline reading from Redpanda, watermarking and event-time windowing, checkpointing and exactly-once semantics, late event handling policy, and dead-letter routing for unparseable events
- `bronze-storage`: Immutable raw event storage in MinIO using Apache Parquet, partition scheme by event date, append-only writes from Spark, and basic partition compaction utilities
- `silver-transformations`: Schema validation at ingestion boundary, normalization (field name casing, timestamp standardization), deduplication by trade_id, business rule validation, and enriched event output to silver Parquet storage
- `gold-analytics`: Aggregated analytical datasets (OHLC candles, volume profiles, trade statistics), Spark-based aggregation windows, materialized views in Parquet, and incremental refresh patterns
- `dbt-transforms`: dbt project for DuckDB, SQL-based transformation models reading from gold Parquet storage, documentation and lineage, testing macros, and data marts for serving layer consumption
- `serving-api`: FastAPI application with endpoint categories (health, operational, analytical), DuckDB-backed query layer, Prometheus metrics endpoint, OpenAPI documentation, and request validation
- `observability`: Prometheus metrics export from all components, Grafana dashboards (throughput, consumer lag, error rates, system health), alert rule definitions, and centralized logging structure
- `data-quality`: Great Expectations suites for schema validation, business rule checks, no-duplicate assertions, freshness checks, and integration with silver/gold pipeline stages
- `ci-cd`: GitHub Actions workflows for linting, type checking, unit tests, integration tests, container image builds, and pre-merge validation gates

### Modified Capabilities

None — this is a new project with no existing capabilities.

## Impact

- New repository with Python project structure
- Docker Compose stack: Redpanda, Spark, MinIO, DuckDB, FastAPI, Prometheus, Grafana
- No existing code, APIs, or systems affected
- Development dependencies: Docker, Python 3.11+, poetry
- Future impact surface: all subsequent QuantStream phases will extend these foundations

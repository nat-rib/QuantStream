## 1. Repository Foundation

- [x] 1.1 Scaffold project structure: `src/`, `tests/`, `config/`, `docker/`, `docs/` directories
- [x] 1.2 Create `pyproject.toml` with Poetry configuration, Python 3.11+ requirement, and all dependencies (pydantic, fastapi, duckdb, pyspark, confluent-kafka, prometheus-client, great-expectations, dbt-duckdb)
- [x] 1.3 Add dev dependencies: pytest, pytest-asyncio, pytest-mock, ruff, black, mypy, pre-commit
- [x] 1.4 Configure ruff (pyproject.toml), black (pyproject.toml), mypy (pyproject.toml) with strict settings
- [x] 1.5 Create `.pre-commit-config.yaml` with ruff, black, mypy, and basic checks (trailing-whitespace, end-of-file-fixer)
- [x] 1.6 Create `.env.example` with all required environment variables and sensible local defaults
- [x] 1.7 Create `Makefile` with targets: up, down, build, test, lint, format, typecheck, clean
- [x] 1.8 Create `.gitignore` covering Python artifacts, environment files, dbt targets, Spark checkpoints, and IDE files
- [x] 1.9 Create `README.md` with project overview, prerequisites, quick-start guide, and architecture diagram

## 2. Event Contracts and Schemas

- [x] 2.1 Define `TradeEvent` Pydantic model with all fields, validators, and camelCase alias generator in `src/quantstream/schemas/`
- [x] 2.2 Define `EnrichedTradeEvent` model extending trade fields with processing metadata
- [x] 2.3 Implement `EventEnvelope` wrapper model with event routing metadata in `src/quantstream/schemas/envelope.py`
- [x] 2.4 Implement schema version registry mapping `schema_version` → Pydantic model class in `src/quantstream/schemas/registry.py`
- [x] 2.5 Implement bidirectional JSON serialization utilities (model ↔ JSON bytes) with consistent field naming
- [x] 2.6 Write unit tests for schema validation (valid events, invalid events, edge cases) and version registry

## 3. Messaging Infrastructure

- [x] 3.1 Add Redpanda service to `docker-compose.yml` with single-broker config, exposed ports (9092, 8081, 9644), and health check
- [x] 3.2 Create topic creation script `scripts/create_topics.sh` that creates `raw.trades` (3 partitions, 7-day retention) and `dead-letter.trades` (3 partitions, 30-day retention) using `rpk`
- [x] 3.3 Create Docker init container or startup script that runs topic creation on first boot
- [x] 3.4 Write unit tests for producer and consumer configuration builders (bootstrap servers, group ID, serializers)
- [x] 3.5 Document Redpanda topic topology and partition strategy in architecture docs

## 4. Ingestion Producer

- [x] 4.1 Implement `BinanceWebSocketClient` in `src/quantstream/ingestion/` using `websockets` library with async connection management
- [x] 4.2 Implement per-symbol stream subscription (`wss://stream.binance.com:9443/ws/<symbol>@trade`)
- [x] 4.3 Implement exponential backoff reconnection with jitter (base 1s, max 60s, max 10 retries)
- [x] 4.4 Implement `RedpandaProducer` wrapper in `src/quantstream/ingestion/producer.py` with `enable.idempotence=true`, ack=all, compression
- [x] 4.5 Implement trade event transformation: Binance JSON → TradeEvent Pydantic model → EventEnvelope → JSON bytes → Redpanda message
- [x] 4.6 Implement Prometheus metrics exporter: `quantstream_ingestion_events_total`, `quantstream_ingestion_errors_total`, `quantstream_ingestion_connection_status`
- [x] 4.7 Implement graceful shutdown: SIGTERM/SIGINT handler that closes WebSocket, flushes producer, exits cleanly
- [x] 4.8 Create `Dockerfile.ingestion` for the producer service
- [x] 4.9 Write unit tests for WebSocket client (mock responses), producer (mock Kafka), and error handling
- [x] 4.10 Write integration test that starts Redpanda, connects producer, and verifies messages in topic

## 5. Streaming Consumer (Spark)

- [x] 5.1 Implement `SparkStreamingPipeline` class in `src/quantstream/streaming/` that creates a SparkSession with MinIO S3A config and checkpoint location
- [x] 5.2 Implement Redpanda source reader using `spark.readStream.format("kafka")` with topic `raw.trades`, consumer group `quantstream-spark-consumer`
- [x] 5.3 Implement event deserialization UDF: JSON bytes → EventEnvelope → TradeEvent
- [x] 5.4 Implement watermarking on `event_time` with 60-second watermark and late-event drop with metric counter
- [x] 5.5 Implement dead-letter routing: catch deserialization/validation errors, publish raw bytes to `dead-letter.trades` with failure reason header
- [x] 5.6 Expose Spark metrics to Prometheus Pushgateway or JMX exporter: events processed, lag, late events, DLQ events
- [x] 5.7 Create `Dockerfile.spark` for the Spark consumer with PySpark and Hadoop S3A dependencies
- [x] 5.8 Write unit tests for deserialization UDF, watermarking logic, and dead-letter routing
- [x] 5.9 Write integration test: produce events to Redpanda, run Spark streaming micro-batch, verify output DataFrame schema

## 6. Bronze Storage

- [x] 6.1 Add MinIO service to `docker-compose.yml` with bucket auto-creation for `quantstream` and exposed ports (9000 API, 9001 Console)
- [x] 6.2 Implement `BronzeWriter` in `src/quantstream/storage/` that writes Spark DataFrames as Parquet to `s3a://quantstream/bronze/trades/` with `event_date` partitioning
- [x] 6.3 Configure Spark S3A filesystem connector with MinIO endpoint, access key, secret key, and path-style access
- [x] 6.4 Implement partition compaction utility script that merges small Parquet files within a date partition
- [x] 6.5 Write integration test: write events to bronze, read back via Spark, verify row count and schema
- [x] 6.6 Write contract test verifying bronze Parquet schema matches TradeEvent model definition

## 7. Silver Transformations

- [x] 7.1 Implement `SilverValidator` class that runs Pydantic validation per event, routes failures to dead-letter
- [x] 7.2 Implement field normalization: symbol uppercase, price/quantity to Decimal, timestamps to UTC, whitespace trim
- [x] 7.3 Implement deduplication by `trade_id` using Spark window function (`row_number` over partition by trade_id, order by ingest_time desc)
- [x] 7.4 Implement business rule validation: price > 0, quantity > 0, side in BUY/SELL, event_time not in future (5s tolerance)
- [x] 7.5 Implement `SilverWriter` that writes enriched DataFrames to `s3a://quantstream/silver/trades/` partitioned by `event_date/` and `symbol/`
- [x] 7.6 Wire silver transformation into Spark streaming pipeline as a post-bronze processing stage
- [x] 7.7 Write unit tests for normalization, deduplication, and business rule validation
- [x] 7.8 Write integration test: produce events (including duplicates and invalid), verify silver output contains only valid, deduplicated events

## 8. Gold Analytics

- [x] 8.1 Implement OHLC aggregation using Spark Structured Streaming tumbling windows (1m, 5m, 15m, 1h) with watermarking
- [x] 8.2 Implement volume profile aggregation: total_volume, buy_volume, sell_volume, vwap per symbol per window
- [x] 8.3 Implement trade statistics aggregation: trade_count, avg_trade_size, max_trade_size, trades_per_second
- [x] 8.4 Define `OHLCCandle`, `VolumeProfile`, `TradeStats` Pydantic models for gold dataset schemas
- [x] 8.5 Implement `GoldWriter` that writes aggregated DataFrames to `s3a://quantstream/gold/<dataset>/` partitioned by `event_date/`
- [x] 8.6 Write unit tests for aggregation logic (known input → expected output)
- [x] 8.7 Write integration test: push events through full pipeline, verify gold datasets are correctly aggregated

## 9. dbt Transforms

- [x] 9.1 Initialize dbt project at `dbt/` with `dbt init quantstream` using `dbt-duckdb` adapter
- [x] 9.2 Configure `profiles.yml` with DuckDB connection pointing to `data/quantstream.duckdb`
- [x] 9.3 Create staging models that read gold Parquet datasets into DuckDB using `read_parquet()`: `stg_ohlc.sql`, `stg_volume_profiles.sql`, `stg_trade_stats.sql`
- [x] 9.4 Create intermediate models: `int_daily_ohlc.sql` (daily rollup of minute candles), `int_symbol_metrics.sql`
- [x] 9.5 Create mart models: `marts.daily_summary`, `marts.symbol_performance`, `marts.volatility_metrics`, `marts.market_activity`
- [x] 9.6 Define dbt tests: unique on `(symbol, window_start, window_end)` for OHLC, not_null on key columns, accepted_range on volume and price columns
- [x] 9.7 Add model descriptions, column documentation, and source freshness config in `schema.yml` files
- [x] 9.8 Write simple dbt test that `dbt test` passes against known gold data fixtures

## 10. Serving API

- [x] 10.1 Create FastAPI application scaffold in `src/quantstream/api/` with app factory, lifespan handler, and CORS config
- [x] 10.2 Implement `GET /health` endpoint with component health checks (Redpanda admin, MinIO bucket, DuckDB connection)
- [x] 10.3 Implement `GET /api/v1/trades/latest` and `GET /api/v1/trades/{symbol}` using DuckDB queries on silver Parquet
- [x] 10.4 Implement `GET /api/v1/analytics/ohlc/{symbol}`, `GET /api/v1/analytics/volume/{symbol}`, `GET /api/v1/analytics/summary` using DuckDB queries on gold/dbt mart views
- [x] 10.5 Add Prometheus metrics middleware: request duration histogram, request count counter, error count counter
- [x] 10.6 Add request validation with Pydantic query parameter models and structured error responses (400, 404, 422, 500)
- [x] 10.7 Create `Dockerfile.api` for the FastAPI service
- [x] 10.8 Write unit tests for all endpoints using FastAPI TestClient with mocked DuckDB
- [x] 10.9 Write integration test: start API, ingest known data, verify endpoint responses match expected values

## 11. Observability

- [x] 11.1 Add Prometheus service to `docker-compose.yml` with `prometheus.yml` config mounting
- [x] 11.2 Create `prometheus.yml` with scrape jobs for ingestion producer, Spark metrics, FastAPI, Redpanda, and alert rules
- [x] 11.3 Add Grafana service to `docker-compose.yml` with pre-configured Prometheus datasource and dashboard provisioning
- [x] 11.4 Create Stream Health dashboard JSON: throughput, consumer lag, error rate, DLQ rate panels
- [x] 11.5 Create Ingestion Overview dashboard JSON: events/sec per symbol, connection status, reconnect count, errors by type
- [x] 11.6 Create Storage Metrics dashboard JSON: bronze/silver/gold row counts, partition sizes, file counts
- [x] 11.7 Create API Performance dashboard JSON: request latency p50/p95/p99, request rate, error rate, endpoint breakdown
- [x] 11.8 Define Prometheus alert rules for consumer lag (>120s warning, >300s critical), connection down, DLQ rate, API errors
- [x] 11.9 Implement structured JSON logging across ingestion producer, Spark pipeline, and FastAPI using Python's logging with JSON formatter

## 12. Data Quality

- [x] 12.1 Initialize Great Expectations project with `great_expectations init` in `src/quantstream/data_quality/`
- [x] 12.2 Create Expectation Suite for silver trades: schema columns/types, trade_id uniqueness, price range, quantity positive, side valid, event_time not null
- [x] 12.3 Create Expectation Suite for gold OHLC: high >= low, open/close within range, volume non-negative, window ordering
- [x] 12.4 Implement GX checkpoint configuration for silver and gold validation with filesystem data context
- [x] 12.5 Implement validation runner script that executes GX checkpoints against Parquet partitions and emits Prometheus metrics
- [x] 12.6 Write unit tests for validation runner with known valid/invalid data fixtures

## 13. CI/CD

- [x] 13.1 Create `.github/workflows/lint.yml`: ruff, black (check), mypy on push and PR
- [x] 13.2 Create `.github/workflows/test.yml`: pytest matrix (py3.11, py3.12) with unit, integration (docker services), and contract test groups
- [x] 13.3 Create `.github/workflows/build.yml`: build and push Docker images (ingestion, spark, api) to GHCR on main push and tags
- [x] 13.4 Document branch protection rules and required checks in repository settings
- [x] 13.5 Verify `.pre-commit-config.yaml` hooks match CI checks exactly

## 14. Documentation

- [x] 14.1 Write `docs/ARCHITECTURE.md`: high-level architecture diagram, component descriptions, data flow, storage layout
- [x] 14.2 Write `docs/ADRs/`: create ADR documents for key decisions (Redpanda over Kafka, Spark over Flink, MinIO over local FS, DuckDB for analytics, dbt for transforms)
- [x] 14.3 Write `docs/RUNBOOK.md`: operational procedures (starting/stopping the platform, health checks, troubleshooting, common failure scenarios)
- [x] 14.4 Write `docs/DEVELOPMENT.md`: local development setup, running tests, making changes, adding new symbols, schema evolution
- [x] 14.5 Write `docs/ROADMAP.md`: Phase 1 completion criteria, Phase 2 preview (quant analytics engine), Phase 3 preview (signal generation)

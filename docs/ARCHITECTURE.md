# QuantStream Architecture

## Overview

QuantStream is a real-time crypto market data engineering platform that ingests live trade events from Binance, processes them through a medallion architecture (bronze → silver → gold), and serves analytical results via an API. The entire platform runs locally using Docker Compose.

## Data Flow

```
Binance WebSocket (public trade streams)
        │
        ▼
Ingestion Producer (Python async)
  - Connects to wss://stream.binance.com:9443/ws
  - Parses trade events → TradeEvent Pydantic model
  - Wraps in EventEnvelope → JSON → Redpanda topic
        │
        ▼
Redpanda (Kafka-compatible broker)
  - Topic: raw.trades (3 partitions, keyed by symbol)
  - Topic: dead-letter.trades (unprocessable events)
  - 7-day retention on raw, 30-day on DLQ
        │
        ▼
Spark Structured Streaming Consumer
  - Reads from raw.trades via Kafka connector
  - Deserializes JSON → EventEnvelope → TradeEvent
  - Applies watermark (60s) on event_time
  - Routes unparseable events to dead-letter.trades
        │
        ├──► Bronze Storage (MinIO Parquet)
        │      s3a://quantstream/bronze/trades/event_date=YYYY-MM-DD/
        │      - Raw, immutable, append-only
        │      - Source of truth for replay
        │
        ├──► Silver Transformations
        │      - Schema validation (Pydantic)
        │      - Field normalization (uppercase, trim, UTC)
        │      - Deduplication by trade_id
        │      - Business rule validation
        │      → s3a://quantstream/silver/trades/event_date=YYYY-MM-DD/symbol=SYMBOL/
        │
        └──► Gold Analytics
               - OHLC candles (1m, 5m, 15m, 1h)
               - Volume profiles (buy/sell breakdown, VWAP)
               - Trade statistics
               → s3a://quantstream/gold/{dataset}/event_date=YYYY-MM-DD/
                      │
                      ▼
                 DuckDB (in-process OLAP)
                   - Direct Parquet reads
                   - dbt models for transformation
                      │
                      ▼
                 FastAPI (serving layer)
                   - GET /health
                   - GET /api/v1/trades/latest
                   - GET /api/v1/trades/{symbol}
                   - GET /api/v1/analytics/ohlc/{symbol}
                   - GET /api/v1/analytics/volume/{symbol}
                   - GET /api/v1/analytics/summary
                      │
                      ▼
          Prometheus + Grafana (observability)
```

## Component Details

### Ingestion Producer
- **Language**: Python (async, websockets library)
- **Metrics**: Events/sec, errors by type, connection status
- **Resilience**: Exponential backoff reconnect, idempotent producer, graceful shutdown

### Redpanda
- **Role**: Streaming message broker (Kafka API compatible)
- **Topics**: `raw.trades` (3 partitions), `dead-letter.trades` (3 partitions)
- **Partitioning**: By trading symbol for per-symbol ordering

### Spark Structured Streaming
- **Role**: Stream processing engine
- **Checkpointing**: Every 30s to MinIO (exactly-once semantics)
- **Watermarking**: 60s on event_time, late events dropped with metrics
- **Dead Letter**: Unparseable events routed to `dead-letter.trades`

### Storage (MinIO + Parquet)
- **Bronze**: `s3a://quantstream/bronze/trades/event_date=YYYY-MM-DD/`
- **Silver**: `s3a://quantstream/silver/trades/event_date=YYYY-MM-DD/symbol=SYMBOL/`
- **Gold**: `s3a://quantstream/gold/{dataset}/event_date=YYYY-MM-DD/`

### DuckDB
- **Role**: In-process OLAP database for analytical queries
- **Integration**: Direct Parquet reads, dbt transformations

### dbt
- **Role**: Data transformation layer (ELT)
- **Adapter**: dbt-duckdb
- **Models**: Staging → Intermediate → Marts

### FastAPI
- **Role**: Serving/API layer
- **Port**: 8000
- **Endpoints**: Health, trades, analytics
- **Metrics**: Request count, latency, errors (Prometheus)

### Prometheus + Grafana
- **Dashboards**: Stream Health, Ingestion Overview, API Performance, System Health
- **Alerts**: Consumer lag, connection down, DLQ rate, API errors

## Error Handling Strategy

| Failure Mode | Mitigation |
|---|---|
| Binance WebSocket disconnect | Exponential backoff reconnection (1s-60s, max 10 retries) |
| Redpanda broker restart | Idempotent producer (enable.idempotence=true) |
| Spark consumer crash | Checkpoint recovery from MinIO |
| Unparseable event | Route to dead-letter.trades |
| Late event (>watermark) | Drop with metrics |
| Duplicate trade_id | Deduplicate in silver (keep latest) |
| MinIO write failure | Spark task retry, checkpoint prevents data loss |
| API query failure | Graceful error response with status codes |

## Schema Evolution

Trade events carry a `schema_version` field. The schema registry maps version → Pydantic model:
- Version 1: Current trade event schema
- Future versions: Add optional fields with defaults for backward compatibility
- Unknown versions: Rejected with SchemaVersionError

# QuantStream

Real-time crypto market data engineering platform.

**Phase 1**: Production-grade streaming data platform for Binance trade events, processed through a medallion architecture (bronze/silver/gold) and served via API.

## Architecture

```
Binance WebSocket → Ingestion Producer → Redpanda → Spark Structured Streaming
                                                          ↓
MinIO (Bronze/Silver/Gold Parquet) ← Spark / dbt → DuckDB → FastAPI
                                                          ↓
                                          Prometheus + Grafana (Observability)
```

## Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Poetry

## Quick Start

```bash
# Clone the repository
git clone <repo-url>
cd QuantStream

# Install dependencies
make install

# Copy environment configuration
cp .env.example .env

# Start the platform
make up

# Run tests
make test

# Check the API
curl http://localhost:8000/health
```

## Services

| Service | Port | Description |
|---|---|---|
| Redpanda | 9092 | Streaming message broker |
| MinIO | 9000/9001 | S3-compatible object storage |
| Spark | 7077/8080 | Stream processing engine |
| DuckDB | — | In-process OLAP database |
| FastAPI | 8000 | Serving API |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Dashboards and alerts |

## Development

```bash
# Format and lint
make format
make lint
make typecheck

# Run specific test suites
make test-unit
make test-integration
make test-contract
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Development Guide](docs/DEVELOPMENT.md)
- [Runbook](docs/RUNBOOK.md)
- [Architecture Decision Records](docs/ADRs/)
- [Roadmap](docs/ROADMAP.md)

## License

MIT

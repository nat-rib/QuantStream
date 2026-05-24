# Development Guide

## Prerequisites

- Docker & Docker Compose v2+
- Python 3.11+
- Poetry

## Setup

```bash
# Clone and install
git clone <repo-url>
cd QuantStream

# Install dependencies
poetry install

# Set up environment
cp .env.example .env

# Install pre-commit hooks
poetry run pre-commit install

# Start infrastructure
make up
```

## Project Structure

```
QuantStream/
├── src/quantstream/
│   ├── api/              # FastAPI application
│   │   ├── app.py        # App factory and middleware
│   │   ├── main.py       # Entry point
│   │   ├── metrics.py    # Prometheus middleware
│   │   └── routes/       # API route handlers
│   ├── data_quality/     # Great Expectations validators
│   ├── ingestion/        # Binance WebSocket client
│   ├── observability/    # Logging configuration
│   ├── schemas/          # Pydantic models and contracts
│   ├── storage/          # MinIO/Parquet readers and writers
│   └── streaming/        # Spark Structured Streaming
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── contract/
│   └── fixtures/
├── config/               # Prometheus, Grafana configs
├── docker/               # Dockerfiles
├── dbt/                  # dbt project
├── docs/                 # Documentation
├── scripts/              # Utility scripts
├── docker-compose.yml
├── pyproject.toml
└── Makefile
```

## Running Tests

```bash
# All tests
make test

# Specific suites
make test-unit
make test-integration
make test-contract

# With coverage
make test-cov
```

## Code Quality

```bash
# Format code
make format

# Lint
make lint

# Type checking
make typecheck
```

## Making Changes

### Adding a new trading symbol
Edit `BINANCE_SYMBOLS` in `.env` or pass via CLI:
```bash
poetry run quantstream-ingestion --symbols "btcusdt,ethusdt,bnbusdt,solusdt"
```

### Schema evolution
1. Create a new Pydantic model with additional fields
2. Register it in `src/quantstream/schemas/registry.py` with a new version number
3. Update silver validation logic to handle both versions
4. Increment `pipeline_version` in silver/gold writers

### Adding a new dbt model
1. Create the SQL file in `dbt/models/` (staging, intermediate, or marts)
2. Update `schema.yml` with column descriptions and tests
3. Run `dbt run` and `dbt test`

## Environment Variables

See `.env.example` for all available variables. Key ones:

- `REDPANDA_BOOTSTRAP_SERVERS`: Redpanda address (default: localhost:19092)
- `BINANCE_SYMBOLS`: Comma-separated trading symbols
- `DUCKDB_PATH`: Path to DuckDB database file
- `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`: MinIO credentials

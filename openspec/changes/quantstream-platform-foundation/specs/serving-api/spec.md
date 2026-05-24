## ADDED Requirements

### Requirement: Health check endpoint
The FastAPI application SHALL expose a `GET /health` endpoint returning HTTP 200 with JSON body containing `status: "healthy"` and component health statuses for Redpanda, MinIO, Spark, and DuckDB.

#### Scenario: Health check returns all components healthy
- **WHEN** a GET request is made to `/health`
- **THEN** the response SHALL contain `{"status": "healthy", "components": {"redpanda": "healthy", "minio": "healthy", "spark": "healthy", "duckdb": "healthy"}}`

### Requirement: Operational endpoints
The system SHALL expose `GET /api/v1/trades/latest` returning the most recent N trades (default 100, configurable via `limit` query param) and `GET /api/v1/trades/{symbol}` returning recent trades filtered by symbol.

#### Scenario: Recent trades endpoint returns data
- **WHEN** `GET /api/v1/trades/latest?limit=50` is requested
- **THEN** the response SHALL contain at most 50 trade events from DuckDB, ordered by `event_time` descending

### Requirement: Analytical endpoints
The system SHALL expose `GET /api/v1/analytics/ohlc/{symbol}` for OHLC data, `GET /api/v1/analytics/volume/{symbol}` for volume profiles, and `GET /api/v1/analytics/summary` for daily market summaries, all with optional `start_time`, `end_time`, and `window` query parameters.

#### Scenario: OHLC data is returned for a symbol
- **WHEN** `GET /api/v1/analytics/ohlc/BTCUSDT?window=5m&start_time=2026-05-24T00:00:00Z` is requested
- **THEN** the response SHALL contain 5-minute OHLC candles for BTCUSDT from the specified start time

### Requirement: Prometheus metrics endpoint
The FastAPI application SHALL expose a `GET /metrics` endpoint in Prometheus text format, including request duration, request count by endpoint, error count by status code, and DuckDB query latency.

#### Scenario: Prometheus scrapes metrics
- **WHEN** Prometheus makes a GET request to `/metrics`
- **THEN** the response SHALL contain `quantstream_api_request_duration_seconds` and `quantstream_api_requests_total` histograms and counters

### Requirement: OpenAPI documentation
The FastAPI application SHALL auto-generate OpenAPI (Swagger) documentation at `/docs` and ReDoc at `/redoc`, with all endpoints documented with request/response schemas.

#### Scenario: Swagger UI is accessible
- **WHEN** a developer navigates to `http://localhost:8000/docs`
- **THEN** the Swagger UI SHALL display all API endpoints with request schemas and example responses

### Requirement: Request validation and error handling
The API SHALL validate all request parameters and return appropriate HTTP status codes: 400 for invalid parameters, 404 for unknown symbols, 422 for validation errors, and 500 for internal errors, all with structured JSON error responses.

#### Scenario: Invalid symbol returns 404
- **WHEN** `GET /api/v1/analytics/ohlc/INVALID` is requested
- **THEN** the response SHALL have HTTP status 404 with body `{"error": "Symbol not found", "symbol": "INVALID"}`

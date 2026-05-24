## ADDED Requirements

### Requirement: Prometheus configuration
The system SHALL provide a `prometheus.yml` configuration file with scrape jobs for all platform components: ingestion producer, Spark metrics sink, FastAPI application, and Redpanda metrics endpoint, all at configurable scrape intervals (default 15s).

#### Scenario: Prometheus scrapes all targets
- **WHEN** Prometheus starts with the provided configuration
- **THEN** the targets page at `localhost:9090/targets` SHALL show all configured scrape targets in the UP state

### Requirement: Grafana dashboards
The system SHALL provide pre-built Grafana dashboards as JSON models for: **Stream Health** (throughput, consumer lag, error rates), **Ingestion Overview** (events/sec, connection status, reconnects), **Storage Metrics** (bronze/silver/gold row counts, partition sizes), and **API Performance** (request latency, error rates, endpoint usage).

#### Scenario: Stream Health dashboard loads
- **WHEN** a developer imports the Stream Health dashboard into Grafana
- **THEN** it SHALL display panels for `quantstream_streaming_events_processed_total`, `quantstream_streaming_lag_seconds`, and `quantstream_streaming_dlq_events_total`

### Requirement: System health dashboard
The system SHALL provide a **System Health** dashboard showing component UP/DOWN status, resource usage (if available), and a single-pane-of-glass view for operational monitoring.

#### Scenario: Component health is visible
- **WHEN** the System Health dashboard is viewed
- **THEN** each component (Redpanda, MinIO, Spark, FastAPI) SHALL show a green/red status indicator based on Prometheus health metrics

### Requirement: Alerting rules
The system SHALL define Prometheus alert rules for: consumer lag exceeding 120 seconds (warning) and 300 seconds (critical), ingestion connection down for >60 seconds, dead-letter event rate exceeding 10/minute, and API error rate exceeding 5%.

#### Scenario: Consumer lag alert fires
- **WHEN** `quantstream_streaming_lag_seconds` exceeds 120 for 2 consecutive evaluation intervals
- **THEN** Prometheus SHALL fire a `ConsumerLagHigh` alert with severity `warning`

### Requirement: Structured logging
All components SHALL emit structured JSON logs to stdout with common fields: `timestamp`, `level`, `component`, `message`, and optional `context` object with component-specific details.

#### Scenario: Ingestion producer logs connection event
- **WHEN** the ingestion producer connects to Binance WebSocket
- **THEN** a log entry SHALL be emitted with `{"timestamp": "...", "level": "INFO", "component": "ingestion-producer", "message": "Connected to Binance WebSocket", "context": {"symbols": ["btcusdt", "ethusdt"]}}`

## ADDED Requirements

### Requirement: OHLC candle aggregation
The system SHALL aggregate raw trade events into OHLC (Open-High-Low-Close) candles at configurable window sizes (default: 1 minute, 5 minutes, 15 minutes, 1 hour) per trading symbol.

#### Scenario: 1-minute OHLC candle computed
- **WHEN** a 1-minute tumbling window closes with multiple BTCUSDT trade events
- **THEN** the system SHALL produce a row with `symbol=BTCUSDT`, `window_start`, `window_end`, `open_price` (first trade price in window), `high_price`, `low_price`, `close_price` (last trade price), `volume` (sum of quantities), and `trade_count`

### Requirement: Volume profile aggregation
The system SHALL compute volume profiles per symbol aggregating total traded volume and buy/sell volume breakdown over configurable time windows.

#### Scenario: Volume profile computed per symbol
- **WHEN** a time window closes for BTCUSDT with both BUY and SELL trades
- **THEN** the output SHALL include `total_volume`, `buy_volume`, `sell_volume`, `buy_count`, `sell_count`, and `vwap` (volume-weighted average price)

### Requirement: Trade statistics aggregation
The system SHALL compute trade-level statistics: trade count, average trade size, largest trade, and trade frequency (trades per second) per symbol per time window.

#### Scenario: Trade statistics computed for high-frequency symbol
- **WHEN** BTCUSDT generates 50 trades in a 1-minute window
- **THEN** the output SHALL report `trade_count=50`, `avg_trade_size`, `max_trade_size`, and `trades_per_second ≈ 0.83`

### Requirement: Incremental gold writes
Gold aggregations SHALL be written incrementally as new windows close, appending to gold Parquet storage under `s3a://quantstream/gold/<dataset_name>/event_date=<YYYY-MM-DD>/`.

#### Scenario: New window appends to gold storage
- **WHEN** a new 1-minute OHLC window completes
- **THEN** the aggregated row SHALL be appended to the gold OHLC dataset without overwriting existing data

### Requirement: Dataset-specific schemas
Each gold dataset (OHLC, volume_profiles, trade_stats) SHALL have a well-defined Pydantic model specifying its schema, enabling validation at the gold boundary and downstream consumption guarantees.

#### Scenario: Gold dataset schema is queryable
- **WHEN** a downstream consumer reads a gold Parquet dataset
- **THEN** the schema SHALL match the documented Pydantic model for that dataset

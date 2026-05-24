## ADDED Requirements

### Requirement: dbt project initialization
The system SHALL include a dbt project configured for the dbt-duckdb adapter, with a `dbt_project.yml` defining the project name (`quantstream`), profile (`duckdb`), model paths, and materialization defaults.

#### Scenario: dbt project is valid
- **WHEN** a developer runs `dbt debug` from the dbt project directory
- **THEN** all configuration checks SHALL pass and the DuckDB connection SHALL be verified

### Requirement: Gold-to-DuckDB staging models
The system SHALL provide dbt models that read gold Parquet datasets (OHLC, volume profiles, trade stats) from MinIO into DuckDB views or tables using DuckDB's `read_parquet()` function.

#### Scenario: Staging model reads gold OHLC data
- **WHEN** `dbt run --select staging` is executed
- **THEN** DuckDB SHALL contain a model reading gold OHLC Parquet files into a named view

### Requirement: Analytical transformation models
The system SHALL provide dbt models that transform staged gold data into analytical data marts: daily summaries, symbol performance rankings, volatility metrics, and market activity overviews.

#### Scenario: Daily summary mart is built
- **WHEN** `dbt run --select marts` is executed with gold data present
- **THEN** the `marts.daily_summary` model SHALL contain one row per symbol per day with open, close, high, low, volume, and trade_count

### Requirement: dbt data tests
The system SHALL define dbt tests on analytical models: uniqueness of primary keys, not-null constraints on critical columns, referential integrity between marts and staging, and accepted value ranges.

#### Scenario: Data tests pass on clean data
- **WHEN** `dbt test` is executed against validated gold data
- **THEN** all schema tests (unique, not_null) and data tests (accepted_values, relationships) SHALL pass

### Requirement: dbt documentation and lineage
The system SHALL generate dbt documentation with `dbt docs generate`, exposing model descriptions, column documentation, data lineage graphs, and source freshness information.

#### Scenario: dbt docs are generated
- **WHEN** `dbt docs generate` is executed
- **THEN** a `target/` directory SHALL contain catalog.json, manifest.json, and index.html with the full DAG lineage

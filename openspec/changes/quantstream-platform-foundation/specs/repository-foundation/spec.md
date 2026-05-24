## ADDED Requirements

### Requirement: Project directory structure
The project SHALL follow a standard Python package layout with clear separation of source code, tests, configuration, and infrastructure definitions.

#### Scenario: Developer can navigate the project
- **WHEN** a developer clones the repository and lists the root directory
- **THEN** they SHALL see `pyproject.toml`, `docker-compose.yml`, `Makefile`, `README.md`, and organized subdirectories for `src/`, `tests/`, `config/`, `docker/`, and `docs/`

### Requirement: Dependency management with Poetry
The project SHALL use Poetry for dependency management with a `pyproject.toml` defining all Python dependencies, dev dependencies, and tool configurations.

#### Scenario: Developer installs dependencies
- **WHEN** a developer runs `poetry install`
- **THEN** all production and development dependencies SHALL be installed into a virtual environment

### Requirement: Docker Compose infrastructure
The project SHALL provide a `docker-compose.yml` that boots all required services: Redpanda, Spark (master + worker), MinIO, DuckDB (optional server), FastAPI, Prometheus, and Grafana.

#### Scenario: Developer starts the full platform
- **WHEN** a developer runs `docker compose up -d`
- **THEN** all services SHALL start successfully and be reachable on their documented ports

### Requirement: Development tooling configuration
The project SHALL configure ruff for linting, black for formatting, mypy for type checking, and pre-commit hooks that run all checks before commits.

#### Scenario: Pre-commit checks run automatically
- **WHEN** a developer attempts to commit code
- **THEN** ruff, black, and mypy SHALL run automatically and block the commit if any checks fail

### Requirement: Makefile for common operations
The project SHALL provide a Makefile with targets for common developer operations: `make up`, `make down`, `make test`, `make lint`, `make clean`, `make build`.

#### Scenario: Developer uses Makefile targets
- **WHEN** a developer runs `make up`
- **THEN** Docker Compose SHALL start all services in detached mode

### Requirement: Environment configuration
The project SHALL use environment variables for all configuration, with a `.env.example` file documenting all required variables and sensible defaults for local development.

#### Scenario: Developer copies environment template
- **WHEN** a developer copies `.env.example` to `.env`
- **THEN** all services SHALL be configurable for local development without additional changes

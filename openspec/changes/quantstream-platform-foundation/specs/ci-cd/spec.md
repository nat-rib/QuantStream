## ADDED Requirements

### Requirement: Lint and type check workflow
The system SHALL define a GitHub Actions workflow triggered on every push and pull request that runs ruff linting, black format checking, and mypy type checking across the entire codebase.

#### Scenario: Push triggers lint workflow
- **WHEN** a developer pushes a commit to any branch
- **THEN** GitHub Actions SHALL run ruff, black (check mode), and mypy, failing the workflow if any check fails

### Requirement: Test workflow with matrix
The system SHALL define a GitHub Actions workflow that runs the full pytest suite on push and PR, with a Python version matrix (3.11, 3.12) and separate job groups for unit tests, integration tests, and contract tests.

#### Scenario: All test suites pass on PR
- **WHEN** a Pull Request is opened
- **THEN** unit tests, integration tests (with Docker services), and contract tests SHALL run in parallel and all must pass for the PR to be mergeable

### Requirement: Docker image build and push
The system SHALL define a GitHub Actions workflow that builds and pushes Docker images for the ingestion producer, Spark consumer, and FastAPI application to a container registry (GitHub Container Registry) on pushes to the main branch and version tags.

#### Scenario: Main branch push triggers image build
- **WHEN** a commit is pushed to the `main` branch
- **THEN** the workflow SHALL build all Docker images, tag them with the commit SHA and `latest`, and push them to GHCR

### Requirement: Pre-merge validation gates
The system SHALL configure branch protection rules (documented in CI config) requiring lint, type check, unit tests, and integration tests to pass before a PR can be merged to `main`.

#### Scenario: PR with failing tests is blocked
- **WHEN** a PR has a failing test suite
- **THEN** the merge button SHALL be blocked until all required checks pass

### Requirement: Pre-commit hook enforcement
The system SHALL configure pre-commit hooks via `.pre-commit-config.yaml` that run ruff, black, mypy, and basic pytest on staged files, with identical configuration to the CI workflows.

#### Scenario: Pre-commit hooks match CI checks
- **WHEN** a developer runs `pre-commit run --all-files`
- **THEN** the same lint, format, and type checks SHALL run locally as would run in CI, ensuring consistent results

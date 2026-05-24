.PHONY: up down build test lint format typecheck clean help

# Docker Compose
up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

# Testing
test:
	poetry run pytest

test-unit:
	poetry run pytest -m unit

test-integration:
	poetry run pytest -m integration

test-contract:
	poetry run pytest -m contract

test-cov:
	poetry run pytest --cov=src/quantstream --cov-report=term-missing

# Code Quality
lint:
	poetry run ruff check src/ tests/
	poetry run black --check src/ tests/

format:
	poetry run ruff check --fix src/ tests/
	poetry run black src/ tests/

typecheck:
	poetry run mypy src/ tests/

# Development
install:
	poetry install

shell:
	poetry shell

pre-commit-install:
	poetry run pre-commit install

pre-commit-run:
	poetry run pre-commit run --all-files

# Cleanup
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf dist/ build/ *.egg-info/

clean-data:
	rm -rf data/
	rm -rf spark-checkpoints/

# Help
help:
	@echo "QuantStream Development Commands"
	@echo "================================="
	@echo "make up              - Start all services with Docker Compose"
	@echo "make down            - Stop all services"
	@echo "make build           - Build Docker images"
	@echo "make test            - Run all tests"
	@echo "make test-unit       - Run unit tests only"
	@echo "make test-integration- Run integration tests"
	@echo "make test-contract   - Run contract tests"
	@echo "make lint            - Run linting checks (ruff + black)"
	@echo "make format          - Auto-format code (ruff + black)"
	@echo "make typecheck       - Run mypy type checking"
	@echo "make install         - Install dependencies with Poetry"
	@echo "make clean           - Remove Python cache files"
	@echo "make clean-data      - Remove data and checkpoint directories"

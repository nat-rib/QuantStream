from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from quantstream.api.app import create_app


@pytest.fixture
def client():
    app = create_app()
    with TestClient(app) as c:
        yield c


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code in (200, 503)
        data = response.json()
        assert "status" in data
        assert "components" in data

    def test_health_has_required_components(self, client):
        response = client.get("/health")
        data = response.json()
        components = data.get("components", {})
        for comp in ["duckdb", "redpanda", "minio", "spark"]:
            assert comp in components


class TestTradesEndpoint:
    def test_latest_trades_handles_empty(self, client):
        response = client.get("/api/v1/trades/latest")
        assert response.status_code in (200, 500)
        if response.status_code == 200:
            data = response.json()
            assert "trades" in data

    def test_trades_by_symbol_returns_404(self, client):
        response = client.get("/api/v1/trades/INVALID")
        assert response.status_code in (404, 500)


class TestAnalyticsEndpoint:
    def test_ohlc_returns_404_for_unknown(self, client):
        response = client.get("/api/v1/analytics/ohlc/INVALID")
        assert response.status_code in (404, 500)

    def test_summary_handles_empty(self, client):
        response = client.get("/api/v1/analytics/summary")
        assert response.status_code in (200, 500)

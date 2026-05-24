from __future__ import annotations

import pytest

from quantstream.data_quality.suites import (
    create_silver_trades_suite,
    create_gold_ohlc_suite,
)
from quantstream.data_quality.validator import ValidationRunner


class TestSilverTradesSuite:
    def test_valid_data_passes(self):
        runner = create_silver_trades_suite()
        data = [
            {
                "trade_id": "abc-123",
                "symbol": "BTCUSDT",
                "price": 68000.0,
                "quantity": 0.1,
                "side": "BUY",
                "event_time": "2026-05-24T14:00:00Z",
            }
        ]
        result = runner.validate("silver.trades", data)
        assert result["success"] is True

    def test_missing_column_fails(self):
        runner = create_silver_trades_suite()
        data = [
            {
                "trade_id": "abc-123",
                "symbol": "BTCUSDT",
                "price": 68000.0,
                "quantity": 0.1,
                "side": "BUY",
            }
        ]
        result = runner.validate("silver.trades", data)
        assert result["success"] is False

    def test_negative_price_fails(self):
        runner = create_silver_trades_suite()
        data = [
            {
                "trade_id": "abc-123",
                "symbol": "BTCUSDT",
                "price": -100.0,
                "quantity": 0.1,
                "side": "BUY",
                "event_time": "2026-05-24T14:00:00Z",
            }
        ]
        result = runner.validate("silver.trades", data)
        assert result["success"] is False

    def test_invalid_side_fails(self):
        runner = create_silver_trades_suite()
        data = [
            {
                "trade_id": "abc-123",
                "symbol": "BTCUSDT",
                "price": 68000.0,
                "quantity": 0.1,
                "side": "INVALID",
                "event_time": "2026-05-24T14:00:00Z",
            }
        ]
        result = runner.validate("silver.trades", data)
        assert result["success"] is False

    def test_duplicate_trade_id_fails(self):
        runner = create_silver_trades_suite()
        data = [
            {
                "trade_id": "abc-123",
                "symbol": "BTCUSDT",
                "price": 68000.0,
                "quantity": 0.1,
                "side": "BUY",
                "event_time": "2026-05-24T14:00:00Z",
            },
            {
                "trade_id": "abc-123",
                "symbol": "ETHUSDT",
                "price": 3100.0,
                "quantity": 1.0,
                "side": "SELL",
                "event_time": "2026-05-24T14:01:00Z",
            },
        ]
        result = runner.validate("silver.trades", data)
        assert result["success"] is False


class TestGoldOHLCSuite:
    def test_valid_ohlc_passes(self):
        runner = create_gold_ohlc_suite()
        data = [
            {
                "symbol": "BTCUSDT",
                "open_price": 68000.0,
                "high_price": 68500.0,
                "low_price": 67900.0,
                "close_price": 68400.0,
                "volume": 100.0,
                "trade_count": 50,
            }
        ]
        result = runner.validate("gold.ohlc", data)
        assert result["success"] is True

    def test_negative_volume_fails(self):
        runner = create_gold_ohlc_suite()
        data = [
            {
                "symbol": "BTCUSDT",
                "open_price": 68000.0,
                "high_price": 68500.0,
                "low_price": 67900.0,
                "close_price": 68400.0,
                "volume": -1.0,
                "trade_count": 50,
            }
        ]
        result = runner.validate("gold.ohlc", data)
        assert result["success"] is False

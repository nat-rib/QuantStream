from __future__ import annotations

from quantstream.data_quality.validator import ValidationRunner


def create_silver_trades_suite() -> ValidationRunner:
    runner = ValidationRunner()

    runner.register_expectation(
        "silver.trades",
        "expect_column_to_exist",
        {"column": "trade_id"},
    )
    runner.register_expectation(
        "silver.trades",
        "expect_column_to_exist",
        {"column": "symbol"},
    )
    runner.register_expectation(
        "silver.trades",
        "expect_column_to_exist",
        {"column": "price"},
    )
    runner.register_expectation(
        "silver.trades",
        "expect_column_to_exist",
        {"column": "quantity"},
    )
    runner.register_expectation(
        "silver.trades",
        "expect_column_to_exist",
        {"column": "side"},
    )
    runner.register_expectation(
        "silver.trades",
        "expect_column_to_exist",
        {"column": "event_time"},
    )

    runner.register_expectation(
        "silver.trades",
        "expect_column_values_to_not_be_null",
        {"column": "trade_id"},
    )
    runner.register_expectation(
        "silver.trades",
        "expect_column_values_to_not_be_null",
        {"column": "event_time"},
    )

    runner.register_expectation(
        "silver.trades",
        "expect_column_values_to_be_between",
        {"column": "price", "min_value": 0, "max_value": 1000000},
    )
    runner.register_expectation(
        "silver.trades",
        "expect_column_values_to_be_between",
        {"column": "quantity", "min_value": 0, "max_value": None},
    )

    runner.register_expectation(
        "silver.trades",
        "expect_column_values_to_be_in_set",
        {"column": "side", "value_set": ["BUY", "SELL"]},
    )

    runner.register_expectation(
        "silver.trades",
        "expect_column_values_to_be_unique",
        {"column": "trade_id"},
    )

    return runner


def create_gold_ohlc_suite() -> ValidationRunner:
    runner = ValidationRunner()

    runner.register_expectation(
        "gold.ohlc",
        "expect_column_to_exist",
        {"column": "symbol"},
    )
    runner.register_expectation(
        "gold.ohlc",
        "expect_column_to_exist",
        {"column": "open_price"},
    )
    runner.register_expectation(
        "gold.ohlc",
        "expect_column_to_exist",
        {"column": "high_price"},
    )
    runner.register_expectation(
        "gold.ohlc",
        "expect_column_to_exist",
        {"column": "low_price"},
    )
    runner.register_expectation(
        "gold.ohlc",
        "expect_column_to_exist",
        {"column": "close_price"},
    )

    runner.register_expectation(
        "gold.ohlc",
        "expect_column_values_to_not_be_null",
        {"column": "symbol"},
    )
    runner.register_expectation(
        "gold.ohlc",
        "expect_column_values_to_be_between",
        {"column": "volume", "min_value": 0, "max_value": None},
    )
    runner.register_expectation(
        "gold.ohlc",
        "expect_column_values_to_be_between",
        {"column": "trade_count", "min_value": 0, "max_value": None},
    )

    return runner

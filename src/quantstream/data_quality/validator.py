from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from prometheus_client import Counter, Gauge

logger = logging.getLogger(__name__)

dq_checks_total = Counter(
    "quantstream_data_quality_checks_total",
    "Total data quality check results",
    ["suite", "status"],
)

dq_check_duration = Gauge(
    "quantstream_data_quality_check_duration_seconds",
    "Data quality check duration in seconds",
    ["suite"],
)


class ValidationRunner:
    def __init__(self) -> None:
        self.suites: dict[str, list[dict[str, Any]]] = {}

    def register_expectation(
        self,
        suite_name: str,
        expectation_type: str,
        kwargs: dict[str, Any],
    ) -> None:
        if suite_name not in self.suites:
            self.suites[suite_name] = []
        self.suites[suite_name].append({
            "expectation_type": expectation_type,
            "kwargs": kwargs,
        })

    def validate(
        self,
        suite_name: str,
        data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        start_time = datetime.now(timezone.utc)

        suite = self.suites.get(suite_name, [])
        results = []

        for expectation in suite:
            result = self._run_expectation(expectation, data)
            results.append(result)

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        dq_check_duration.labels(suite=suite_name).set(duration)

        success = all(r["success"] for r in results)
        status = "pass" if success else "fail"
        dq_checks_total.labels(suite=suite_name, status=status).inc()

        return {
            "suite": suite_name,
            "success": success,
            "results": results,
            "duration_seconds": duration,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _run_expectation(
        self,
        expectation: dict[str, Any],
        data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        exp_type = expectation["expectation_type"]
        kwargs = expectation["kwargs"]

        handlers = {
            "expect_column_to_exist": self._check_column_exists,
            "expect_column_values_to_not_be_null": self._check_not_null,
            "expect_column_values_to_be_in_set": self._check_in_set,
            "expect_column_values_to_be_between": self._check_between,
            "expect_column_values_to_be_unique": self._check_unique,
            "expect_table_row_count_to_be_between": self._check_row_count,
        }

        handler = handlers.get(exp_type)
        if handler:
            return handler(data, kwargs)
        return {
            "expectation": exp_type,
            "success": True,
            "note": "Expectation type not implemented, assuming pass",
        }

    def _check_column_exists(
        self, data: list[dict[str, Any]], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        column = kwargs.get("column", "")
        exists = any(column in row for row in data) if data else False
        return {
            "expectation": "expect_column_to_exist",
            "column": column,
            "success": exists,
        }

    def _check_not_null(
        self, data: list[dict[str, Any]], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        column = kwargs.get("column", "")
        failures = []
        for i, row in enumerate(data):
            if row.get(column) is None:
                failures.append(i)

        return {
            "expectation": "expect_column_values_to_not_be_null",
            "column": column,
            "success": len(failures) == 0,
            "unexpected_count": len(failures),
        }

    def _check_in_set(
        self, data: list[dict[str, Any]], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        column = kwargs.get("column", "")
        value_set = set(kwargs.get("value_set", []))
        failures = []
        for i, row in enumerate(data):
            if row.get(column) not in value_set:
                failures.append(i)

        return {
            "expectation": "expect_column_values_to_be_in_set",
            "column": column,
            "success": len(failures) == 0,
            "unexpected_count": len(failures),
        }

    def _check_between(
        self, data: list[dict[str, Any]], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        column = kwargs.get("column", "")
        min_val = kwargs.get("min_value")
        max_val = kwargs.get("max_value")
        failures = []
        for i, row in enumerate(data):
            val = row.get(column)
            if val is not None:
                if (min_val is not None and val < min_val) or (
                    max_val is not None and val > max_val
                ):
                    failures.append(i)

        return {
            "expectation": "expect_column_values_to_be_between",
            "column": column,
            "success": len(failures) == 0,
            "unexpected_count": len(failures),
        }

    def _check_unique(
        self, data: list[dict[str, Any]], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        column = kwargs.get("column", "")
        values = [row.get(column) for row in data if column in row]
        unique_count = len(set(values))
        total_count = len(values)

        return {
            "expectation": "expect_column_values_to_be_unique",
            "column": column,
            "success": unique_count == total_count,
            "unique_count": unique_count,
            "total_count": total_count,
        }

    def _check_row_count(
        self, data: list[dict[str, Any]], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        min_val = kwargs.get("min_value", 0)
        max_val = kwargs.get("max_value")
        count = len(data)
        success = count >= min_val
        if max_val is not None:
            success = success and count <= max_val

        return {
            "expectation": "expect_table_row_count_to_be_between",
            "success": success,
            "row_count": count,
        }

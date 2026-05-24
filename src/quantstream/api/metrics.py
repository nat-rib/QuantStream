from __future__ import annotations

import os
import time
from typing import Callable

from fastapi import Request, Response
from prometheus_client import Counter, Histogram
from starlette.middleware.base import BaseHTTPMiddleware

request_count = Counter(
    "quantstream_api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status_code"],
)

request_duration = Histogram(
    "quantstream_api_request_duration_seconds",
    "API request duration in seconds",
    ["method", "endpoint"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)


class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.monotonic()
        response = await call_next(request)
        duration = time.monotonic() - start_time

        endpoint = request.url.path
        method = request.method

        request_count.labels(
            method=method, endpoint=endpoint, status_code=str(response.status_code)
        ).inc()

        if not endpoint.startswith("/metrics"):
            request_duration.labels(method=method, endpoint=endpoint).observe(duration)

        return response

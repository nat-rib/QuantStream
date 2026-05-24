from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check(request: Request) -> JSONResponse:
    components = {}

    try:
        db = request.app.state.duckdb
        db.execute("SELECT 1")
        components["duckdb"] = "healthy"
    except Exception as e:
        components["duckdb"] = f"unhealthy: {e}"

    try:
        import httpx
        async with httpx.AsyncClient() as client:
            resp = await client.get("http://localhost:9644/v1/status/node", timeout=5.0)
            if resp.status_code == 200:
                components["redpanda"] = "healthy"
            else:
                components["redpanda"] = "unhealthy"
    except Exception:
        components["redpanda"] = "unreachable"

    try:
        import httpx
        async with httpx.AsyncClient() as client:
            resp = await client.get("http://localhost:9000/minio/health/live", timeout=5.0)
            if resp.status_code == 200:
                components["minio"] = "healthy"
            else:
                components["minio"] = "unhealthy"
    except Exception:
        components["minio"] = "unreachable"

    try:
        import httpx
        async with httpx.AsyncClient() as client:
            resp = await client.get("http://localhost:8080/json/", timeout=5.0)
            if resp.status_code == 200:
                components["spark"] = "healthy"
            else:
                components["spark"] = "unhealthy"
    except Exception:
        components["spark"] = "unreachable"

    all_healthy = all(v == "healthy" for v in components.values())

    return JSONResponse(
        status_code=200 if all_healthy else 503,
        content={
            "status": "healthy" if all_healthy else "degraded",
            "components": components,
        },
    )

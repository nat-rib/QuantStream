from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from quantstream.api.routes import health, analytics, trades

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    db_path = os.getenv("DUCKDB_PATH", "data/quantstream.duckdb")
    app.state.duckdb = duckdb.connect(db_path)
    logger.info("DuckDB connected", extra={"path": db_path})

    app.state.duckdb.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id VARCHAR,
            exchange VARCHAR,
            symbol VARCHAR,
            price DOUBLE,
            quantity DOUBLE,
            side VARCHAR,
            event_time TIMESTAMP,
            ingest_time TIMESTAMP,
            schema_version INTEGER
        )
    """)

    yield

    app.state.duckdb.close()
    logger.info("DuckDB connection closed")


def create_app() -> FastAPI:
    app = FastAPI(
        title="QuantStream API",
        description="Real-time crypto market data serving layer",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health.router)
    app.include_router(trades.router, prefix="/api/v1")
    app.include_router(analytics.router, prefix="/api/v1")

    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    return app

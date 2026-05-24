from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["trades"])


@router.get("/trades/latest")
async def get_latest_trades(
    request: Request,
    limit: int = Query(default=100, ge=1, le=1000),
) -> JSONResponse:
    db = request.app.state.duckdb
    try:
        result = db.execute(
            "SELECT * FROM trades ORDER BY event_time DESC LIMIT ?",
            [limit],
        ).fetchall()

        columns = [desc[0] for desc in db.description] if db.description else []
        trades = [dict(zip(columns, row)) for row in result]

        return JSONResponse(content={"trades": trades, "count": len(trades)})
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Query failed: {e}",
        )


@router.get("/trades/{symbol}")
async def get_trades_by_symbol(
    request: Request,
    symbol: str,
    limit: int = Query(default=100, ge=1, le=1000),
) -> JSONResponse:
    db = request.app.state.duckdb
    try:
        result = db.execute(
            "SELECT * FROM trades WHERE UPPER(symbol) = UPPER(?) ORDER BY event_time DESC LIMIT ?",
            [symbol, limit],
        ).fetchall()

        if not result:
            return JSONResponse(
                status_code=404,
                content={"error": "Symbol not found", "symbol": symbol.upper()},
            )

        columns = [desc[0] for desc in db.description] if db.description else []
        trades = [dict(zip(columns, row)) for row in result]

        return JSONResponse(content={"symbol": symbol.upper(), "trades": trades, "count": len(trades)})
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Query failed: {e}",
        )

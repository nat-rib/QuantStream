from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["analytics"])


@router.get("/analytics/ohlc/{symbol}")
async def get_ohlc(
    request: Request,
    symbol: str,
    window: str = Query(default="1m", description="Window size (1m, 5m, 15m, 1h)"),
    start_time: Optional[str] = Query(default=None, description="Start time ISO format"),
    end_time: Optional[str] = Query(default=None, description="End time ISO format"),
) -> JSONResponse:
    db = request.app.state.duckdb
    try:
        query = "SELECT * FROM ohlc WHERE UPPER(symbol) = UPPER(?)"
        params: list = [symbol]

        if window:
            query += " AND window_duration = ?"
            params.append(window)
        if start_time:
            query += " AND window_start >= ?"
            params.append(start_time)
        if end_time:
            query += " AND window_end <= ?"
            params.append(end_time)

        query += " ORDER BY window_start DESC LIMIT 500"

        result = db.execute(query, params).fetchall()

        if not result:
            return JSONResponse(
                status_code=404,
                content={"error": "No OHLC data found", "symbol": symbol.upper()},
            )

        columns = [desc[0] for desc in db.description] if db.description else []
        candles = [dict(zip(columns, row)) for row in result]

        return JSONResponse(content={"symbol": symbol.upper(), "window": window, "candles": candles, "count": len(candles)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")


@router.get("/analytics/volume/{symbol}")
async def get_volume(
    request: Request,
    symbol: str,
    window: str = Query(default="1m"),
    start_time: Optional[str] = Query(default=None),
    end_time: Optional[str] = Query(default=None),
) -> JSONResponse:
    db = request.app.state.duckdb
    try:
        query = "SELECT * FROM volume_profiles WHERE UPPER(symbol) = UPPER(?)"
        params: list = [symbol]

        if window:
            query += " AND window_duration = ?"
            params.append(window)
        if start_time:
            query += " AND window_start >= ?"
            params.append(start_time)
        if end_time:
            query += " AND window_end <= ?"
            params.append(end_time)

        query += " ORDER BY window_start DESC LIMIT 500"

        result = db.execute(query, params).fetchall()

        if not result:
            return JSONResponse(
                status_code=404,
                content={"error": "No volume data found", "symbol": symbol.upper()},
            )

        columns = [desc[0] for desc in db.description] if db.description else []
        profiles = [dict(zip(columns, row)) for row in result]

        return JSONResponse(content={"symbol": symbol.upper(), "profiles": profiles, "count": len(profiles)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")


@router.get("/analytics/summary")
async def get_summary(
    request: Request,
    start_time: Optional[str] = Query(default=None),
    end_time: Optional[str] = Query(default=None),
) -> JSONResponse:
    db = request.app.state.duckdb
    try:
        query = "SELECT * FROM daily_summary"
        params: list = []
        conditions = []

        if start_time:
            conditions.append("trade_date >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("trade_date <= ?")
            params.append(end_time)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY trade_date DESC, symbol ASC LIMIT 200"

        result = db.execute(query, params).fetchall()

        columns = [desc[0] for desc in db.description] if db.description else []
        summaries = [dict(zip(columns, row)) for row in result]

        return JSONResponse(content={"summaries": summaries, "count": len(summaries)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

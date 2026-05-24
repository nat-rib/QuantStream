from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from quantstream.schemas.trade import TradeEvent


class EnrichedTradeEvent(TradeEvent):
    """Trade event with processing metadata added during silver transformation."""

    model_config = ConfigDict(
        alias_generator=TradeEvent.model_config.get("alias_generator"),
        populate_by_name=True,
    )

    processing_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When silver processing occurred",
    )
    pipeline_version: str = Field(
        default="0.1.0", description="Semantic version of the silver pipeline"
    )
    partition_date: str = Field(
        default="",
        description="Date used for silver partitioning (YYYY-MM-DD)",
    )


class OHLCCandle(BaseModel):
    model_config = ConfigDict(
        alias_generator=lambda field_name: "".join(
            word.capitalize() if i > 0 else word
            for i, word in enumerate(field_name.split("_"))
        ),
        populate_by_name=True,
    )

    symbol: str = Field(..., description="Trading symbol")
    window_start: datetime = Field(..., description="Window start time")
    window_end: datetime = Field(..., description="Window end time")
    open_price: Decimal = Field(..., max_digits=18, decimal_places=8)
    high_price: Decimal = Field(..., max_digits=18, decimal_places=8)
    low_price: Decimal = Field(..., max_digits=18, decimal_places=8)
    close_price: Decimal = Field(..., max_digits=18, decimal_places=8)
    volume: Decimal = Field(..., max_digits=24, decimal_places=8)
    trade_count: int = Field(..., ge=0)
    window: str = Field(default="1m", description="Window size descriptor")
    partition_date: str = Field(default="")


class VolumeProfile(BaseModel):
    model_config = ConfigDict(
        alias_generator=lambda field_name: "".join(
            word.capitalize() if i > 0 else word
            for i, word in enumerate(field_name.split("_"))
        ),
        populate_by_name=True,
    )

    symbol: str
    window_start: datetime
    window_end: datetime
    total_volume: Decimal = Field(..., max_digits=24, decimal_places=8)
    buy_volume: Decimal = Field(..., max_digits=24, decimal_places=8)
    sell_volume: Decimal = Field(..., max_digits=24, decimal_places=8)
    buy_count: int = Field(..., ge=0)
    sell_count: int = Field(..., ge=0)
    vwap: Decimal = Field(..., max_digits=18, decimal_places=8)
    window: str = Field(default="1m")
    partition_date: str = Field(default="")


class TradeStats(BaseModel):
    model_config = ConfigDict(
        alias_generator=lambda field_name: "".join(
            word.capitalize() if i > 0 else word
            for i, word in enumerate(field_name.split("_"))
        ),
        populate_by_name=True,
    )

    symbol: str
    window_start: datetime
    window_end: datetime
    trade_count: int = Field(..., ge=0)
    avg_trade_size: Decimal = Field(..., max_digits=18, decimal_places=8)
    max_trade_size: Decimal = Field(..., max_digits=18, decimal_places=8)
    trades_per_second: float = Field(..., ge=0)
    window: str = Field(default="1m")
    partition_date: str = Field(default="")

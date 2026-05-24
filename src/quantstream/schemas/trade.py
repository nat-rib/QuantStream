from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TradeEvent(BaseModel):
    model_config = ConfigDict(
        alias_generator=lambda field_name: "".join(
            word.capitalize() if i > 0 else word
            for i, word in enumerate(field_name.split("_"))
        ),
        populate_by_name=True,
    )

    trade_id: uuid.UUID = Field(..., description="Unique trade identifier")
    exchange: str = Field(
        default="binance", description="Exchange source identifier"
    )
    symbol: str = Field(..., description="Trading symbol (e.g. BTCUSDT)")
    price: Decimal = Field(
        ..., max_digits=18, decimal_places=8, description="Trade price"
    )
    quantity: Decimal = Field(
        ..., max_digits=18, decimal_places=8, description="Trade quantity"
    )
    side: Literal["BUY", "SELL"] = Field(..., description="Trade side")
    event_time: datetime = Field(..., description="Exchange event timestamp")
    ingest_time: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Ingestion timestamp",
    )
    schema_version: int = Field(
        default=1, description="Schema version for evolution"
    )

    @field_validator("price", "quantity")
    @classmethod
    def validate_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError(f"Value must be positive, got {v}")
        return v

    @field_validator("event_time")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, v: str) -> str:
        return v.upper().strip()

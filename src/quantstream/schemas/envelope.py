from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from quantstream.schemas.trade import TradeEvent

T = TypeVar("T", bound=BaseModel)


class EventEnvelope(BaseModel, Generic[T]):
    model_config = ConfigDict(
        alias_generator=lambda field_name: "".join(
            word.capitalize() if i > 0 else word
            for i, word in enumerate(field_name.split("_"))
        ),
        populate_by_name=True,
    )

    event_id: uuid.UUID = Field(
        default_factory=uuid.uuid4, description="Unique identifier for this envelope"
    )
    event_type: Literal["trade"] = Field(
        default="trade", description="Type of event in payload"
    )
    payload: T = Field(..., description="The wrapped event")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When the envelope was created",
    )
    source: str = Field(default="binance", description="Source system identifier")
    partition_key: str = Field(
        default="", description="Key used for topic partitioning"
    )

    @classmethod
    def wrap_trade(cls, trade: TradeEvent) -> EventEnvelope[TradeEvent]:
        return cls(
            payload=trade,
            source=trade.exchange,
            partition_key=trade.symbol,
        )

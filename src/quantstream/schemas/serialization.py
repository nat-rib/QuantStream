from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Type, TypeVar
from uuid import UUID

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        from datetime import datetime

        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def model_to_json(model: BaseModel) -> str:
    return model.model_dump_json(by_alias=True)


def model_to_bytes(model: BaseModel) -> bytes:
    return model_to_json(model).encode("utf-8")


def json_to_model(data: str, model_class: Type[T]) -> T:
    return model_class.model_validate_json(data)


def bytes_to_model(data: bytes, model_class: Type[T]) -> T:
    return json_to_model(data.decode("utf-8"), model_class)


def dict_to_model(data: dict[str, Any], model_class: Type[T]) -> T:
    return model_class.model_validate(data)

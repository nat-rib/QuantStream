from __future__ import annotations

from typing import Type

from pydantic import BaseModel

from quantstream.schemas.trade import TradeEvent


class SchemaVersionError(Exception):
    def __init__(self, version: int) -> None:
        self.version = version
        super().__init__(f"Unknown schema version: {version}")


class SchemaRegistry:
    def __init__(self) -> None:
        self._registry: dict[int, Type[BaseModel]] = {
            1: TradeEvent,
        }

    def register(self, version: int, model: Type[BaseModel]) -> None:
        if version in self._registry:
            raise ValueError(
                f"Schema version {version} already registered with {self._registry[version].__name__}"
            )
        self._registry[version] = model

    def get_model(self, version: int) -> Type[BaseModel]:
        model = self._registry.get(version)
        if model is None:
            raise SchemaVersionError(version)
        return model

    def get_latest_version(self) -> int:
        return max(self._registry.keys()) if self._registry else 1

    def get_latest_model(self) -> Type[BaseModel]:
        return self.get_model(self.get_latest_version())


_global_registry = SchemaRegistry()


def get_schema_registry() -> SchemaRegistry:
    return _global_registry

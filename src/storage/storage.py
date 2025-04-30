from abc import ABC, abstractmethod
from asyncio import Future
from dataclasses import dataclass
from types import CoroutineType
from typing import Any

class Storage(ABC):
    @dataclass
    class Config(ABC):
        @abstractmethod
        def create_instance(self) -> "Storage": pass

    @abstractmethod
    async def get(self, key: str) -> Any: pass
    @abstractmethod
    async def mget(self, keys: list[str]) -> list[Any]: pass
    @abstractmethod
    async def exists(self, *keys: str) -> Any: pass # returns number of keys that exist
    @abstractmethod
    async def set(self, key: str, value, expire=None) -> Any: pass
    @abstractmethod
    async def atomic_increment_and_get(self, key: str) -> Any: pass
    @abstractmethod
    async def keys(self, pattern: str) -> list: pass
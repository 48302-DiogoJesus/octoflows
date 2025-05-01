from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Union

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

    # PUB/SUB
    @abstractmethod
    async def publish(self, channel: str, message: Union[str, bytes]) -> int: pass
    @abstractmethod
    async def subscribe(self, channel: str, callback: Callable[[dict], Any], decode_responses: bool = False) -> None: pass
    @abstractmethod
    async def unsubscribe(self, channel: str) -> None: pass
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

class Storage(ABC):
    @dataclass
    class Config(ABC):
        @abstractmethod
        def create_instance(self) -> "Storage": pass

    @abstractmethod
    def get(self, key: str) -> Any: pass
    @abstractmethod
    def exists(self, *keys: str) -> Any: pass # returns number of keys that exist
    @abstractmethod
    def set(self, key: str, value, expire=None) -> Any: pass
    @abstractmethod
    def atomic_increment_and_get(self, key: str) -> Any: pass

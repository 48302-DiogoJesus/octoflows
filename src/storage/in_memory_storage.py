from dataclasses import dataclass
import threading
from dataclasses import dataclass
import time
from typing import Any

import src.storage.storage as storage

class InMemoryStorage(storage.Storage):
    @dataclass
    class Config(storage.Storage.Config):
        pass

        def create_instance(self) -> "InMemoryStorage":
            return InMemoryStorage(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self._data: dict[str, Any] = {}
        self._expiry: dict[str, float] = {}
        self._lock = threading.RLock()

    async def get(self, key: str):
        with self._lock:
            if key in self._data:
                # Check if the key has expired
                if key in self._expiry and self._expiry[key] <= time.time():
                    del self._data[key]
                    del self._expiry[key]
                    return None
                return self._data[key]
            return None

    async def set(self, key: str, value, expire=None):
        with self._lock:
            self._data[key] = value
            
            if expire is not None:
                self._expiry[key] = time.time() + expire
            elif key in self._expiry:
                del self._expiry[key]

    async def exists(self, *keys: str) -> Any:
        with self._lock:
            keys_found = 0
            for key in keys:
                if key in self._data:
                    keys_found += 1
            return keys_found

    async def atomic_increment_and_get(self, key: str):
        with self._lock:
            # Get current value or initialize to 0
            current_value = self._data.get(key, 0)
            
            if not isinstance(current_value, (int, float)):
                current_value = 0
                
            new_value = current_value + 1
            self._data[key] = new_value
            
            return new_value

    async def keys(self, pattern: str) -> list:
        raise NotImplementedError

    async def mget(self, keys: list[str]) -> list[Any]:
        with self._lock:
            return [self._data.get(key) for key in keys]



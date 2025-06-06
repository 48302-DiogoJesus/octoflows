from dataclasses import dataclass
import threading
from dataclasses import dataclass
import time
import re
from typing import Any, Callable

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
        self._subscribers: dict[str, list[tuple[Callable[[dict], Any], bool]]] = {}

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
        """
        Return keys matching the given pattern.
        Simple pattern matching with * as wildcard.
        """
        with self._lock:
            if pattern == "*":
                return list(self._data.keys())
            
            # Convert Redis-style pattern to regex pattern
            regex_pattern = "^" + pattern.replace("*", ".*") + "$"
            compiled_pattern = re.compile(regex_pattern)
            
            matching_keys = [
                key for key in self._data.keys() 
                if compiled_pattern.match(key)
            ]
            
            return matching_keys

    async def mget(self, keys: list[str]) -> list[Any]:
        with self._lock:
            return [self._data.get(key) for key in keys]

    async def publish(self, channel: str, message: str | bytes) -> int:
        """
        Publish a message to a channel.
        Returns the number of subscribers that received the message.
        """
        with self._lock:
            if channel not in self._subscribers:
                return 0
                
            subscriber_count = 0
            for callback, decode_responses in self._subscribers[channel]:
                # Create message dict similar to Redis pub/sub format
                message_dict = {
                    "type": "message",
                    "channel": channel,
                    "data": message
                }
                
                try:
                    callback(message_dict)
                    subscriber_count += 1
                except Exception:
                    # Silently ignore callback errors
                    pass
                    
            return subscriber_count

    async def subscribe(self, channel: str, callback: Callable[[dict], Any], decode_responses: bool = False) -> None:
        """
        Subscribe to a channel with a callback function.
        """
        with self._lock:
            if channel not in self._subscribers:
                self._subscribers[channel] = []
                
            self._subscribers[channel].append((callback, decode_responses))

    async def unsubscribe(self, channel: str) -> None:
        """
        Unsubscribe from a channel.
        """
        with self._lock:
            if channel in self._subscribers:
                del self._subscribers[channel]

    async def delete(self, key: str, *, pattern: bool = False, prefix: bool = False, suffix: bool = False) -> int:
        """
        Delete keys from the storage.
        
        Args:
            key: The key pattern/prefix/suffix to match for deletion
            pattern: If True, treat key as a pattern to match (e.g., 'user:*')
            prefix: If True, delete all keys that start with the given key
            suffix: If True, delete all keys that end with the given key
            
        Returns:
            int: Number of keys that were deleted
            
        Note:
            Only one of pattern, prefix, or suffix should be True at a time.
            If none are True, performs an exact key match delete.
        """
        with self._lock:
            # Count how many of the flags are True
            match_flags = sum([bool(pattern), bool(prefix), bool(suffix)])
            if match_flags > 1:
                raise ValueError("Only one of pattern, prefix, or suffix can be True")
                
            if pattern:
                # Find all keys matching the pattern
                matching_keys = await self.keys(key)
            elif prefix:
                # Find all keys starting with the prefix
                matching_keys = [k for k in self._data.keys() if k.startswith(key)]
            elif suffix:
                # Find all keys ending with the suffix
                matching_keys = [k for k in self._data.keys() if k.endswith(key)]
            else:
                # Exact match
                matching_keys = [key] if key in self._data else []
            
            # Delete all matching keys
            deleted_count = 0
            for k in matching_keys:
                if k in self._data:
                    del self._data[k]
                    if k in self._expiry:
                        del self._expiry[k]
                    deleted_count += 1
            
            return deleted_count

    async def _execute_batch(self, operations: list[storage.BatchOperation]) -> list[Any]:
        raise NotImplementedError

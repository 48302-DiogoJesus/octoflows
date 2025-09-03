from dataclasses import dataclass
import threading
from dataclasses import dataclass
import time
import re
import uuid
from typing import Any, Callable, Dict, List, Optional

import src.storage.storage as storage

@dataclass
class SubscriptionInfo:
    """Information about a subscription callback."""
    subscription_id: str
    callback: Callable[[dict, str], Any]
    decode_responses: bool
    coroutine_tag: str
    worker_id: Optional[str]

class InMemoryStorage(storage.Storage):
    @dataclass
    class Config(storage.Storage.Config):
        pass

        def create_instance(self) -> "InMemoryStorage":
            return InMemoryStorage(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self._data: dict[str, Any] = {}
        self._lock = threading.RLock()
        # Changed to store multiple subscriptions per channel
        # Format: {channel: {subscription_id: SubscriptionInfo, ...}}
        self._channel_subscriptions: Dict[str, Dict[str, SubscriptionInfo]] = {}

    async def get(self, key: str):
        with self._lock:
            return self._data.get(key)

    async def set(self, key: str, value):
        with self._lock:
            self._data[key] = value

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
            if channel not in self._channel_subscriptions:
                return 0
                
            subscriber_count = 0
            
            # Process message for each subscription
            for subscription_id, sub_info in self._channel_subscriptions[channel].items():
                try:
                    # Create message dict similar to Redis pub/sub format
                    message_dict = {
                        "type": "message",
                        "channel": channel,
                        "data": message
                    }
                    
                    # Decode message if requested for this subscription
                    if sub_info.decode_responses and isinstance(message, bytes):
                        message_dict["data"] = message.decode("utf-8")
                    
                    # Call the callback with message and subscription_id
                    sub_info.callback(message_dict, subscription_id)
                    subscriber_count += 1
                    
                except Exception:
                    # Silently ignore callback errors to prevent affecting other callbacks
                    pass
                    
            return subscriber_count

    async def subscribe(self, channel: str, callback: Callable[[dict, str], Any], decode_responses: bool = False, coroutine_tag: str = "", worker_id: str | None = None) -> str:
        """
        Subscribe to a channel with a callback function.
        
        Args:
            channel: The channel to subscribe to
            callback: A function that will be called with each message
            decode_responses: Whether to decode the message payload as UTF-8
            coroutine_tag: Tag for the coroutine (for logging/debugging)
            worker_id: Worker identifier (for logging/debugging)
            
        Returns:
            str: Unique subscription identifier that can be used to unsubscribe this specific callback
        """
        with self._lock:
            # Generate unique subscription ID
            subscription_id = str(uuid.uuid4())
            
            # Create subscription info
            sub_info = SubscriptionInfo(
                subscription_id=subscription_id,
                callback=callback,
                decode_responses=decode_responses,
                coroutine_tag=coroutine_tag,
                worker_id=worker_id
            )
            
            # Initialize channel subscriptions if not exists
            if channel not in self._channel_subscriptions:
                self._channel_subscriptions[channel] = {}
            
            # Add the new subscription
            self._channel_subscriptions[channel][subscription_id] = sub_info
            
            return subscription_id

    async def unsubscribe(self, channel: str, subscription_id: Optional[str] = None):
        """
        Unsubscribe from a channel.
        
        Args:
            channel: The channel to unsubscribe from
            subscription_id: Specific subscription to remove. If None, removes all subscriptions for the channel.
            
        Returns:
            bool: True if subscription was found and removed, False otherwise
        """
        with self._lock:
            if channel not in self._channel_subscriptions:
                return
            
            if subscription_id is None:
                # Remove all subscriptions for this channel
                removed_count = len(self._channel_subscriptions[channel])
                del self._channel_subscriptions[channel]
                return
            else:
                # Remove specific subscription
                if subscription_id not in self._channel_subscriptions[channel]:
                    return
                
                del self._channel_subscriptions[channel][subscription_id]
                
                # If no more subscriptions for this channel, remove the channel entry
                if not self._channel_subscriptions[channel]:
                    del self._channel_subscriptions[channel]

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
                    deleted_count += 1
            
            return deleted_count

    async def _execute_batch(self, operations: list[storage.BatchOperation]) -> list[Any]:
        """
        Execute a batch of operations in sequence.
        
        Args:
            operations: List of BatchOperation objects to execute
            
        Returns:
            List of results in the same order as the operations
        """
        results = []
        
        with self._lock:
            for op in operations:
                try:
                    if op.op_type == storage.BatchOperation.Type.GET:
                        result = await self.get(*op.args, **op.kwargs)
                    elif op.op_type == storage.BatchOperation.Type.MGET:
                        result = await self.mget(*op.args, **op.kwargs)
                    elif op.op_type == storage.BatchOperation.Type.EXISTS:
                        result = await self.exists(*op.args, **op.kwargs)
                    elif op.op_type == storage.BatchOperation.Type.SET:
                        # Set operations don't return anything meaningful in Redis
                        await self.set(*op.args, **op.kwargs)
                        result = True
                    elif op.op_type == storage.BatchOperation.Type.ATOMIC_INCREMENT_AND_GET:
                        result = await self.atomic_increment_and_get(*op.args, **op.kwargs)
                    elif op.op_type == storage.BatchOperation.Type.KEYS:
                        result = await self.keys(*op.args, **op.kwargs)
                    elif op.op_type == storage.BatchOperation.Type.PUBLISH:
                        result = await self.publish(*op.args, **op.kwargs)
                    elif op.op_type == storage.BatchOperation.Type.DELETE:
                        result = await self.delete(*op.args, **op.kwargs)
                    else:
                        raise ValueError(f"Unsupported operation type: {op.op_type}")
                    
                    # Store the result if a result key was provided
                    if op.result_key is not None:
                        self._data[op.result_key] = result
                        
                    results.append(result)
                except Exception as e:
                    # If an operation fails, store the exception as the result
                    results.append(e)
        
        return results

    def get_channel_subscription_count(self, channel: str) -> int:
        """
        Get the number of active subscriptions for a channel.
        
        Args:
            channel: The channel name
            
        Returns:
            int: Number of active subscriptions for the channel
        """
        with self._lock:
            return len(self._channel_subscriptions.get(channel, {}))

    def get_all_channels_with_subscriptions(self) -> List[str]:
        """
        Get a list of all channels that have active subscriptions.
        
        Returns:
            List[str]: List of channel names with active subscriptions
        """
        with self._lock:
            return list(self._channel_subscriptions.keys())

    def get_subscription_info(self, channel: str, subscription_id: str) -> Optional[SubscriptionInfo]:
        """
        Get information about a specific subscription.
        
        Args:
            channel: The channel name
            subscription_id: The subscription identifier
            
        Returns:
            Optional[SubscriptionInfo]: Subscription information if found, None otherwise
        """
        with self._lock:
            if channel in self._channel_subscriptions:
                return self._channel_subscriptions[channel].get(subscription_id)
            return None

    def get_all_subscription_ids(self, channel: str) -> List[str]:
        """
        Get all subscription IDs for a channel.
        
        Args:
            channel: The channel name
            
        Returns:
            List[str]: List of subscription IDs for the channel
        """
        with self._lock:
            if channel in self._channel_subscriptions:
                return list(self._channel_subscriptions[channel].keys())
            return []
from dataclasses import dataclass
import nest_asyncio
import asyncio
import traceback
from typing import Any, Callable, Dict, List, Optional, Union
from redis.asyncio import Redis

import src.storage.storage as storage
from src.utils.logger import create_logger

logger = create_logger(__name__)

nest_asyncio.apply()

class RedisStorage(storage.Storage):
    @dataclass
    class Config(storage.Storage.Config):
        host: str
        port: int
        password: str
        # Optional parameters
        db: int = 0
        socket_connect_timeout = 5
        socket_timeout = None # required to allow using subscribe (pub/sub)

        def create_instance(self) -> "RedisStorage":
            return RedisStorage(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.redis_config = config
        self._connection: Optional[Redis] = None
        self._pubsub = None
        self._subscription_tasks: Dict[str, tuple[asyncio.Task, Any]] = {}
        
        # Don't initialize connection immediately to avoid event loop issues
        # Connection will be created lazily when first needed

    async def _get_or_create_connection(self, skip_verification: bool = False) -> Redis:
        if not skip_verification and await self._verify_connection():
            return self._connection # type: ignore
        else:
            self._connection = Redis(
                host=self.redis_config.host,
                port=self.redis_config.port,
                db=self.redis_config.db,
                password=self.redis_config.password,
                decode_responses=False,  # Necessary to allow serialized bytes
                socket_connect_timeout=self.redis_config.socket_connect_timeout,
                socket_timeout=self.redis_config.socket_timeout
            )
            return self._connection

    async def get(self, key: str) -> Any:
        conn = await self._get_or_create_connection()
        if not await conn.exists(key):
            return None
        return await conn.get(key)

    async def set(self, key: str, value: Any) -> bool:
        conn = await self._get_or_create_connection()
        return await conn.set(key, value)

    async def atomic_increment_and_get(self, key: str) -> int:
        conn = await self._get_or_create_connection()
        # Atomically increment and get the new value
        return await conn.incr(key, amount=1)

    async def exists(self, *keys: str) -> int:
        conn = await self._get_or_create_connection()
        return await conn.exists(*keys)
    
    async def close_connection(self) -> None:
        """Close Redis connection and cancel any running subscription tasks."""
        # Cancel all subscription tasks
        for channel, (task, pubsub) in self._subscription_tasks.items():
            logger.info(f"Cancelling subscription to channel: {channel}")
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            await pubsub.aclose()
        
        self._subscription_tasks.clear()
        
        # Close PubSub if it exists
        if self._pubsub is not None:
            await self._pubsub.aclose()
            self._pubsub = None
        
        # Close main connection
        if self._connection is not None:
            await self._connection.aclose()
            self._connection = None

    async def _verify_connection(self) -> bool:
        try:
            if self._connection is None:
                return False
            return await self._connection.ping()
        except Exception:
            return False

    async def keys(self, pattern: str) -> List[str]:
        conn = await self._get_or_create_connection()
        return await conn.keys(pattern)

    async def mget(self, keys: List[str]) -> List[Any]:
        conn = await self._get_or_create_connection()
        return await conn.mget(keys)

    async def _get_pubsub(self):
        """Get or create the PubSub object."""
        if self._pubsub is None:
            conn = await self._get_or_create_connection()
            self._pubsub = conn.pubsub()
        return self._pubsub
    
    async def publish(self, channel: str, message: Union[str, bytes]) -> int:
        """
        Publish a message to a channel.
        
        Args:
            channel: The channel to publish to
            message: The message to publish (string or bytes)
            
        Returns:
            Number of clients that received the message
        """
        conn = await self._get_or_create_connection()
        logger.info(f"Publishing message to: {channel}")
        return await conn.publish(channel, message)

    async def subscribe(self, channel: str, callback: Callable[[dict], Any], decode_responses: bool = False, debug_tag: str = "") -> None:
        """
        Subscribe to a channel and process messages with a callback.
        
        Args:
            channel: The channel to subscribe to
            callback: A function that will be called with each message (can be sync or async)
            decode_responses: Whether to decode the message payload as UTF-8
        """
        # Cancel existing subscription if any
        if channel in self._subscription_tasks:
            task, pubsub = self._subscription_tasks[channel]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            await pubsub.aclose()
            
        # Create a new connection and PubSub instance for this subscription
        conn = await self._get_or_create_connection()
        pubsub = conn.pubsub()
        await pubsub.subscribe(channel)
        
        # Start a background task to process messages
        task = asyncio.create_task(
            self._message_handler(pubsub, channel, callback, decode_responses),
            name=f"redis_subscribe_message_handler(channel={channel}, debug_tag={debug_tag})"
        )
        self._subscription_tasks[channel] = (task, pubsub)
        
        logger.info(f"Subscribed to channel: {channel} | debug tag: {debug_tag}")

    async def unsubscribe(self, channel: str) -> None:
        """
        Unsubscribe from a channel.
        
        Args:
            channel: The channel to unsubscribe from
        """
        if channel in self._subscription_tasks:
            task, pubsub = self._subscription_tasks[channel]
            task.cancel()
            try:
                await task  # Wait for task to be cancelled
            except asyncio.CancelledError:
                pass
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
            del self._subscription_tasks[channel]
            logger.info(f"Unsubscribed from channel: {channel}")

    async def _message_handler(
        self, 
        pubsub: Any,
        channel: str, 
        callback: Callable[[dict], Any], 
        decode_responses: bool = False
    ) -> None:
        """
        Background task to handle incoming messages.
        
        Args:
            pubsub: The PubSub instance for this subscription
            channel: The channel being handled
            callback: The callback function to process messages (can be sync or async)
            decode_responses: Whether to decode message data as UTF-8
        """
        try:
            async for message in pubsub.listen():
                # Filter messages by type and channel
                if message["type"] == "message":
                    channel_name = message["channel"].decode() if isinstance(message["channel"], bytes) else message["channel"]
                    if channel_name == channel:
                        if decode_responses and isinstance(message["data"], bytes):
                            message["data"] = message["data"].decode("utf-8")
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(message)
                            else:
                                callback(message)
                        except Exception as e:
                            logger.error(f"Error in callback for channel {channel}: {e}")
                            raise e
                            # traceback.print_exc()
        except asyncio.CancelledError:
            logger.debug(f"Message handler for {channel} was cancelled")
        except Exception as e:
            # logger.error(f"Error in message handler for {channel}: {e}")
            # traceback.print_exc()
            raise e
    
    async def _delete_matching_keys(self, conn: Redis, match_pattern: str) -> int:
        """Helper method to delete keys matching a pattern using scan_iter.
        
        Args:
            conn: Redis connection
            match_pattern: Pattern to match keys against
            
        Returns:
            int: Number of keys deleted
        """
        # First, collect all matching keys using scan_iter
        keys_to_delete = []
        async for key in conn.scan_iter(match=match_pattern, count=1000):
            keys_to_delete.append(key)
            
        # If no keys found, return early
        if not keys_to_delete:
            return 0
            
        # Delete all matching keys in a single operation
        return await conn.delete(*keys_to_delete)

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
        conn = await self._get_or_create_connection()
        
        # Count how many of the flags are True
        match_flags = sum([bool(pattern), bool(prefix), bool(suffix)])
        if match_flags > 1:
            raise ValueError("Only one of pattern, prefix, or suffix can be True")
        
        if pattern:
            # Use the provided pattern directly
            return await self._delete_matching_keys(conn, key)
        elif prefix:
            # Match keys starting with the prefix
            return await self._delete_matching_keys(conn, f"{key}*")
        elif suffix:
            # Match keys ending with the suffix
            return await self._delete_matching_keys(conn, f"*{key}")
        else:
            # Exact match delete
            deleted = await conn.delete(key)
            return deleted
        
    async def _execute_batch(self, operations: list[storage.BatchOperation]) -> list[Any]:
        """Execute a batch of operations in a single Redis pipeline."""
        if not operations:
            return []
            
        # Get Redis connection
        conn = await self._get_or_create_connection()
        
        # Create pipeline
        pipe = conn.pipeline()
        
        # Map operation types to Redis pipeline methods
        op_handlers = {
            storage.BatchOperation.Type.GET: lambda op: pipe.get(*op.args, **op.kwargs),
            storage.BatchOperation.Type.MGET: lambda op: pipe.mget(*op.args, **op.kwargs),
            storage.BatchOperation.Type.EXISTS: lambda op: pipe.exists(*op.args, **op.kwargs),
            storage.BatchOperation.Type.SET: lambda op: pipe.set(*op.args, **op.kwargs),
            storage.BatchOperation.Type.ATOMIC_INCREMENT_AND_GET: lambda op: pipe.incr(*op.args, **op.kwargs),
            storage.BatchOperation.Type.KEYS: lambda op: pipe.keys(*op.args, **op.kwargs),
            storage.BatchOperation.Type.PUBLISH: lambda op: pipe.publish(*op.args, **op.kwargs),
            storage.BatchOperation.Type.DELETE: lambda op: (
                pipe.delete(*op.args, **{k: v for k, v in op.kwargs.items() 
                                    if k in ['pattern', 'prefix', 'suffix']})
                if any(op.kwargs.get(flag) for flag in ['pattern', 'prefix', 'suffix'])
                else pipe.delete(*op.args)
            ),
        }
        
        # Queue all operations in the pipeline
        for op in operations:
            if op.op_type not in op_handlers:
                raise ValueError(f"Unsupported batch operation type: {op.op_type}")
            op_handlers[op.op_type](op)
        
        # Execute pipeline and return results
        results = await pipe.execute()
        return results

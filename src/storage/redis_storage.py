from dataclasses import dataclass
import nest_asyncio
import asyncio
import traceback
import uuid
from typing import Any, Callable, Dict, List, Optional, Union, Tuple
from redis.asyncio import Redis

import src.storage.storage as storage
from src.utils.logger import create_logger

logger = create_logger(__name__)

nest_asyncio.apply()

@dataclass
class SubscriptionInfo:
    """Information about a subscription callback."""
    subscription_id: str
    callback: Callable[[dict, str], Any]
    decode_responses: bool
    worker_id: Optional[str]

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
        self.ARTIFICIAL_NETWORK_LATENCY_S = 0.3
        # Changed to store multiple subscriptions per channel
        # Format: {channel: {subscription_id: SubscriptionInfo, ...}}
        self._channel_subscriptions: Dict[str, Dict[str, SubscriptionInfo]] = {}
        # Format: {channel: (task, pubsub)}
        self._channel_tasks: Dict[str, Tuple[asyncio.Task, Any]] = {}
        
        # Don't initialize connection immediately to avoid event loop issues
        # Connection will be created lazily when first needed

    async def _simulate_network_latency(self) -> None:
        await asyncio.sleep(self.ARTIFICIAL_NETWORK_LATENCY_S)

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
        await self._simulate_network_latency()
        conn = await self._get_or_create_connection()
        if not await conn.exists(key):
            return None
        return await conn.get(key)

    async def set(self, key: str, value: Any) -> bool:
        await self._simulate_network_latency()
        conn = await self._get_or_create_connection()
        return await conn.set(key, value)

    async def atomic_increment_and_get(self, key: str) -> int:
        await self._simulate_network_latency()
        conn = await self._get_or_create_connection()
        # Atomically increment and get the new value
        return await conn.incr(key, amount=1)

    async def exists(self, *keys: str) -> int:
        await self._simulate_network_latency()
        conn = await self._get_or_create_connection()
        return await conn.exists(*keys)
    
    async def close_connection(self) -> None:
        """Close Redis connection and cancel any running subscription tasks."""
        # Cancel all channel tasks
        for channel, (task, pubsub) in self._channel_tasks.items():
            logger.info(f"Cancelling subscription task for channel: {channel}")
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            await pubsub.aclose()
        
        self._channel_tasks.clear()
        self._channel_subscriptions.clear()
        
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
        await self._simulate_network_latency()
        conn = await self._get_or_create_connection()
        return await conn.keys(pattern)

    async def mget(self, keys: List[str]) -> List[Any]:
        await self._simulate_network_latency()
        conn = await self._get_or_create_connection()
        return await conn.mget(keys)
    
    async def publish(self, channel: str, message: Union[str, bytes]) -> int:
        """
        Publish a message to a channel.
        
        Args:
            channel: The channel to publish to
            message: The message to publish (string or bytes)
            
        Returns:
            Number of clients that received the message
        """
        await self._simulate_network_latency()
        conn = await self._get_or_create_connection()
        logger.info(f"Publishing message to: {channel}")
        return await conn.publish(channel, message)

    async def subscribe(self, channel: str, callback: Callable[[dict, str], Any], decode_responses: bool = False, coroutine_tag: str = "", worker_id: str | None = None) -> str:
        """
        Subscribe to a channel and process messages with a callback.
        
        Args:
            channel: The channel to subscribe to
            callback: A function that will be called with each message (can be sync or async)
            decode_responses: Whether to decode the message payload as UTF-8
            coroutine_tag: Tag for the coroutine (for logging/debugging)
            worker_id: Worker identifier (for logging/debugging)
            
        Returns:
            str: Unique subscription identifier that can be used to unsubscribe this specific callback
        """
        await self._simulate_network_latency()
        # Generate unique subscription ID
        subscription_id = str(uuid.uuid4())
        
        # Create subscription info
        sub_info = SubscriptionInfo(
            subscription_id=subscription_id,
            callback=callback,
            decode_responses=decode_responses,
            worker_id=worker_id
        )
        
        # Initialize channel subscriptions if not exists
        if channel not in self._channel_subscriptions:
            self._channel_subscriptions[channel] = {}
        
        # Add the new subscription
        self._channel_subscriptions[channel][subscription_id] = sub_info
        
        # If this is the first subscription to this channel, create the task
        if channel not in self._channel_tasks:
            # Create a new connection and PubSub instance for this channel
            conn = await self._get_or_create_connection()
            pubsub = conn.pubsub()
            await pubsub.subscribe(channel)
            
            # Start a background task to process messages for all callbacks on this channel
            task = asyncio.create_task(
                self._channel_message_handler(pubsub, channel),
                name=f"redis_channel_message_handler(channel={channel}, coroutine_tag={coroutine_tag})"
            )
            self._channel_tasks[channel] = (task, pubsub)
        
        logger.info(f"W({worker_id}) Subscribed to channel: {channel} | coroutine tag: {coroutine_tag} | subscription_id: {subscription_id}")
        
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
        if channel not in self._channel_subscriptions:
            logger.warning(f"No subscriptions found for channel: {channel}")
            return
        
        if subscription_id is None:
            # Remove all subscriptions for this channel
            removed_count = len(self._channel_subscriptions[channel])
            del self._channel_subscriptions[channel]
            
            # Cancel and cleanup the channel task
            if channel in self._channel_tasks:
                task, pubsub = self._channel_tasks[channel]
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()
                del self._channel_tasks[channel]
            
            logger.info(f"Unsubscribed all {removed_count} callbacks from channel: {channel}")
            return
        else:
            # Remove specific subscription
            if subscription_id not in self._channel_subscriptions[channel]:
                logger.warning(f"Subscription {subscription_id} not found for channel: {channel}")
                return
            
            del self._channel_subscriptions[channel][subscription_id]
            logger.info(f"Unsubscribed callback {subscription_id} from channel: {channel}")
            
            # If no more subscriptions for this channel, cleanup the task
            if not self._channel_subscriptions[channel]:
                del self._channel_subscriptions[channel]
                
                if channel in self._channel_tasks:
                    task, pubsub = self._channel_tasks[channel]
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    await pubsub.unsubscribe(channel)
                    await pubsub.aclose()
                    del self._channel_tasks[channel]
                    logger.info(f"Cleaned up channel task for: {channel}")
            
    async def _channel_message_handler(self, pubsub: Any, channel: str) -> None:
        """
        Background task to handle incoming messages for all callbacks on a channel.
        
        Args:
            pubsub: The PubSub instance for this channel
            channel: The channel being handled
        """
        try:
            async for message in pubsub.listen():
                # Filter messages by type and channel
                if message["type"] == "message":
                    channel_name = message["channel"].decode() if isinstance(message["channel"], bytes) else message["channel"]
                    if channel_name == channel:
                        # Get all active subscriptions for this channel
                        if channel in self._channel_subscriptions:
                            # Create a snapshot of subscriptions to avoid "dictionary changed size during iteration" error
                            subscriptions_snapshot = list(self._channel_subscriptions[channel].items())
                            
                            # Process message for each subscription
                            for subscription_id, sub_info in subscriptions_snapshot:
                                # Double-check that subscription still exists (it might have been removed)
                                if (channel in self._channel_subscriptions and 
                                    subscription_id in self._channel_subscriptions[channel]):
                                    try:
                                        # Decode message if requested for this subscription
                                        message_data = message.copy()
                                        if sub_info.decode_responses and isinstance(message_data["data"], bytes):
                                            message_data["data"] = message_data["data"].decode("utf-8")
                                        
                                        # Call the callback with message and subscription_id
                                        await self._simulate_network_latency()
                                        if asyncio.iscoroutinefunction(sub_info.callback):
                                            await sub_info.callback(message_data, subscription_id)
                                        else:
                                            sub_info.callback(message_data, subscription_id)
                                            
                                    except Exception as e:
                                        logger.error(f"Error in callback {subscription_id} for channel {channel}: {e}")
                                        # Don't raise here - we want to continue processing other callbacks
                                        traceback.print_exc()
        except asyncio.CancelledError:
            logger.debug(f"Message handler for {channel} was cancelled")
        except Exception as e:
            logger.error(f"Error in message handler for {channel}: {e}")
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
        await self._simulate_network_latency()
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
        await self._simulate_network_latency()
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

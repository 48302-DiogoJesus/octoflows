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

    async def set(self, key: str, value: Any, expire: Optional[int] = None) -> bool:
        conn = await self._get_or_create_connection()
        return await conn.set(key, value, ex=expire)

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

    async def subscribe(self, channel: str, callback: Callable[[dict], Any], decode_responses: bool = False) -> None:
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
            name=f"redis_subscribe_message_handler(channel={channel})"
        )
        self._subscription_tasks[channel] = (task, pubsub)
        
        logger.info(f"Subscribed to channel: {channel}")

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
        except asyncio.CancelledError:
            logger.debug(f"Message handler for {channel} was cancelled")
        except Exception as e:
            logger.error(f"Error in message handler for {channel}: {e}")
            traceback.print_exc()
        finally:
            await pubsub.aclose()

    async def _execute_batch(self, operations: List[storage.BatchOperation]) -> List[Any]:
        if not operations: return []
        
        conn = await self._get_or_create_connection()
        
        # https://redis.io/docs/latest/develop/use/pipelining/
        pipe = conn.pipeline()
        
        # Queue all operations
        for op in operations:
            try:
                if op.op_type == storage.BatchOperation.Type.GET:
                    key = op.args[0]
                    pipe.get(key)
                    
                elif op.op_type == storage.BatchOperation.Type.MGET:
                    keys = op.args[0]
                    pipe.mget(keys)
                    
                elif op.op_type == storage.BatchOperation.Type.EXISTS:
                    keys = op.args
                    pipe.exists(*keys)
                    
                elif op.op_type == storage.BatchOperation.Type.SET:
                    key, value = op.args
                    expire = op.kwargs.get('expire')
                    pipe.set(key, value, ex=expire)
                    
                elif op.op_type == storage.BatchOperation.Type.ATOMIC_INCREMENT_AND_GET:
                    key = op.args[0]
                    pipe.incr(key, amount=1)
                    
                elif op.op_type == storage.BatchOperation.Type.KEYS:
                    pattern = op.args[0]
                    pipe.keys(pattern)
                    
                elif op.op_type == storage.BatchOperation.Type.PUBLISH:
                    channel, message = op.args
                    pipe.publish(channel, message)
                    
                else:
                    raise ValueError(f"Unsupported batch operation type: {op.op_type}")
                    
            except Exception as e:
                logger.error(f"Error queuing batch operation {op.op_type}: {e}")
                raise
        
        try:
            # Execute all operations in the pipeline
            results = await pipe.execute()
            
            # Process results
            processed_results = []
            for i, (op, result) in enumerate(zip(operations, results)):
                if op.op_type == storage.BatchOperation.Type.GET:
                    processed_results.append(result)
                    
                elif op.op_type == storage.BatchOperation.Type.MGET:
                    processed_results.append(result)
                    
                elif op.op_type == storage.BatchOperation.Type.EXISTS:
                    processed_results.append(result)
                    
                elif op.op_type == storage.BatchOperation.Type.SET:
                    processed_results.append(result)
                    
                elif op.op_type == storage.BatchOperation.Type.ATOMIC_INCREMENT_AND_GET:
                    processed_results.append(result)
                    
                elif op.op_type == storage.BatchOperation.Type.KEYS:
                    processed_results.append(result)
                    
                elif op.op_type == storage.BatchOperation.Type.PUBLISH:
                    processed_results.append(result)
                    
                else:
                    processed_results.append(result)
            
            logger.debug(f"Successfully executed batch of {len(operations)} operations")
            return processed_results
            
        except Exception as e:
            logger.error(f"Error executing batch operations: {e}")
            raise

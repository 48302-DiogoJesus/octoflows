from dataclasses import dataclass
import asyncio
import traceback
from typing import Any, Callable, Dict, List, Optional, Union
from redis.asyncio import Redis

import src.storage.storage as storage
from src.utils.logger import create_logger

logger = create_logger(__name__)

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
        self._subscription_tasks: Dict[str, asyncio.Task] = {}
        
        # Initialize connection in a non-blocking way
        asyncio.create_task(self._get_or_create_connection(skip_verification=True))

    async def _get_or_create_connection(self, skip_verification: bool = False) -> Redis:
        if not skip_verification and await self._verify_connection():
            return self._connection
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
        for channel, task in self._subscription_tasks.items():
            logger.info(f"Cancelling subscription to channel: {channel}")
            task.cancel()
        
        # Close PubSub if it exists
        if self._pubsub is not None:
            await self._pubsub.aclose()
        
        # Close main connection
        if self._connection is not None:
            await self._connection.aclose()

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
        return await conn.publish(channel, message)


    async def subscribe(self, channel: str, callback: Callable[[dict], Any], decode_responses: bool = False) -> None:
        """
        Subscribe to a channel and process messages with a callback.
        
        Args:
            channel: The channel to subscribe to
            callback: A function that will be called with each message
            decode_responses: Whether to decode the message payload as UTF-8
        """
        # Cancel existing subscription if any
        if channel in self._subscription_tasks:
            self._subscription_tasks[channel].cancel()
            
        # Create a new subscription
        pubsub = await self._get_pubsub()
        await pubsub.subscribe(channel)
        
        # Start a background task to process messages
        task = asyncio.create_task(self._message_handler(channel, callback, decode_responses))
        self._subscription_tasks[channel] = task
        
        logger.info(f"Subscribed to channel: {channel}")

    async def unsubscribe(self, channel: str) -> None:
        """
        Unsubscribe from a channel.
        
        Args:
            channel: The channel to unsubscribe from
        """
        if channel in self._subscription_tasks:
            self._subscription_tasks[channel].cancel()
            del self._subscription_tasks[channel]
            
            pubsub = await self._get_pubsub()
            await pubsub.unsubscribe(channel)
            logger.info(f"Unsubscribed from channel: {channel}")

    async def _message_handler(self,  channel: str, callback: Callable[[dict], Any], decode_responses: bool = False) -> None:
        """
        Background task to handle incoming messages.
        
        Args:
            channel: The channel or pattern being handled
            callback: The callback function to process messages
            decode_responses: Whether to decode message data as UTF-8
        """
        pubsub = await self._get_pubsub()
        try:
            async for message in pubsub.listen():
                # Filter messages by type and channel
                if message["type"] == "message" and message["channel"].decode() == channel:
                    if decode_responses and isinstance(message["data"], bytes):
                        message["data"] = message["data"].decode("utf-8")
                    callback(message)
        except asyncio.CancelledError:
            logger.debug(f"Message handler for {channel} was cancelled")
        except Exception as e:
            logger.error(f"Error in message handler for {channel}: {e}")
            # show stack trace
            traceback.print_exc()
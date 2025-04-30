from dataclasses import dataclass
import asyncio
from redis.asyncio import Redis

import src.storage.storage as storage

class AsyncRedisStorage(storage.Storage):
    @dataclass
    class Config(storage.Storage.Config):
        host: str
        port: int
        password: str

        def create_instance(self) -> "AsyncRedisStorage":
            return AsyncRedisStorage(self)

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.redis_config = config
        self._connection: Redis = None
        # Initialize connection in a non-blocking way
        asyncio.create_task(self._get_or_create_connection(skip_verification=True))

    async def _get_or_create_connection(self, skip_verification: bool = False) -> Redis:
        if not skip_verification and await self._verify_connection():
            return self._connection
        else:
            self._connection = Redis(
                host=self.redis_config.host,
                port=self.redis_config.port,
                db=0,
                password=self.redis_config.password,
                decode_responses=False,  # Necessary to allow serialized bytes
                socket_connect_timeout=5,
                socket_timeout=5
            )
            return self._connection

    async def get(self, key: str):
        conn = await self._get_or_create_connection()
        if not await conn.exists(key):
            return None
        return await conn.get(key)

    async def set(self, key: str, value, expire=None):
        conn = await self._get_or_create_connection()
        return await conn.set(key, value, ex=expire)

    async def atomic_increment_and_get(self, key: str):
        conn = await self._get_or_create_connection()
        # Atomically increment and get the new value
        return await conn.incr(key, amount=1)

    async def exists(self, *keys: str):
        conn = await self._get_or_create_connection()
        return await conn.exists(*keys)
    
    async def close_connection(self):
        await self._connection.aclose()

    async def _verify_connection(self):
        try:
            if self._connection is None:
                return False
            return await self._connection.ping()
        except Exception:
            return False

    async def keys(self, pattern: str) -> list:
        conn = await self._get_or_create_connection()
        return await conn.keys(pattern)

    async def mget(self, keys: list[str]) -> list:
        conn = await self._get_or_create_connection()
        return await conn.mget(keys)


async def main():
    # Create configuration
    config = AsyncRedisStorage.Config(host="localhost", port=6380, password="redisdevpwd123")
    
    # Initialize storage
    redis_storage = AsyncRedisStorage(config)
    
    # Test basic operations
    print("Setting key 'test_key' with value 'test_value'")
    await redis_storage.set("test_key", "test_value")
    
    print("Getting value for 'test_key'")
    value = await redis_storage.get("test_key")
    print(f"Retrieved value: {value}")
    
    print("Checking if key exists")
    exists = await redis_storage.exists("test_key")
    print(f"Key exists: {exists}")
    
    print("Incrementing counter")
    counter = await redis_storage.atomic_increment_and_get("counter")
    print(f"Counter value: {counter}")
    
    print("Setting multiple keys")
    await redis_storage.set("key1", "value1")
    await redis_storage.set("key2", "value2")
    
    print("Getting keys with pattern '*'")
    keys = await redis_storage.keys("*")
    print(f"Found keys: {keys}")
    
    print("Getting multiple values")
    values = await redis_storage.mget(["key1", "key2"])
    print(f"Retrieved values: {values}")
    
    # Clean up
    await redis_storage.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
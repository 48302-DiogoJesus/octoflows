from typing import Any
import redis

class IntermediateStorage:
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = None
    _connection = None  # Singleton connection

    @classmethod
    def _get_connection(cls):
        """
        Get the singleton Redis connection. If it doesn't exist, create it.
        """
        if cls._connection is None:
            cls._connection = redis.Redis(
                host=cls.REDIS_HOST,
                port=cls.REDIS_PORT,
                db=cls.REDIS_DB,
                password=cls.REDIS_PASSWORD,
                decode_responses=False
            )
        return cls._connection

    @classmethod
    def verify_connection(cls):
        """
        Verify if the Redis server is reachable.
        """
        conn = cls._get_connection()
        try:
            return conn.ping()
        except redis.ConnectionError:
            return False

    @classmethod
    def get(cls, key):
        """
        Get a value from Redis by key.
        """
        conn = cls._get_connection()
        if not conn.exists(key): return None
        return conn.get(key)

    @classmethod
    def exists(cls, key):
        """
        Check if a key exists in Redis.
        """
        conn = cls._get_connection()
        return conn.exists(key)

    @classmethod
    def set(cls, key, value, expire=None):
        """
        Set a value in Redis by key.
        
        Args:
            key (str): The key to set
            value (str): The value to store
            expire (int, optional): Time in seconds after which the key will expire
            
        Returns:
            bool: True if successful
        """
        conn = cls._get_connection()
        return conn.set(key, value, ex=expire)

    @classmethod
    def increment_and_get(cls, key):
        """
        Atomically increment a key and check if it matches the target value.
        """
        conn = cls._get_connection()
        # Atomically increment and get the new value
        return conn.incr(key, amount=1)

    @classmethod
    def configure(cls, host=None, port=None, db=None, password=None):
        """
        Configure Redis connection parameters.
        """
        if host is not None:
            cls.REDIS_HOST = host
        if port is not None:
            cls.REDIS_PORT = port
        if db is not None:
            cls.REDIS_DB = db
        if password is not None:
            cls.REDIS_PASSWORD = password
        # Reset the connection if configuration changes
        if cls._connection is not None:
            cls._connection.close()
            cls._connection = None

    @classmethod
    def close_connection(cls):
        """
        Close the Redis connection.
        """
        if cls._connection is not None:
            cls._connection.close()
            cls._connection = None

# Example usage
# async def example_usage():
#     # Publish a message (non-blocking)
#     await IntermediateStorage.publish_message("my_channel", "Hello, Redis!")

#     # Subscribe to a channel (non-blocking)
#     async def message_handler(message):
#         print(f"Callback received: {message['data'].decode('utf-8')}")

#     task = await IntermediateStorage.subscribe_to_channel("my_channel", callback=message_handler)

#     # Publish another message
#     await IntermediateStorage.publish_message("my_channel", "Another message!")

#     # Wait for a few seconds to receive messages
#     await asyncio.sleep(2)

#     # Unsubscribe (non-blocking)
#     await IntermediateStorage.unsubscribe_from_channel("my_channel")

#     # Close the connection
#     IntermediateStorage.close_connection()

# # Run the example
# asyncio.run(example_usage())
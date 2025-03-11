import redis
import asyncio

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

    @classmethod
    async def publish_message(cls, channel, message, wait_for_result=False):
        """
        Publish a message to a Redis channel asynchronously.
        
        Args:
            channel (str): The channel to publish to.
            message (str): The message to publish.
            wait_for_result (bool): Whether to wait for the result.
            
        Returns:
            int: Number of subscribers that received the message (if wait_for_result is True).
        """
        conn = cls._get_connection()
        if wait_for_result:
            return conn.publish(channel, message)
        else:
            asyncio.create_task(asyncio.to_thread(conn.publish, channel, message))
            return None

    @classmethod
    async def subscribe_to_channel(cls, channel, callback=None, wait_for_result=False):
        """
        Subscribe to a Redis channel asynchronously.
        
        Args:
            channel (str): The channel to subscribe to.
            callback (callable): A callback function to process incoming messages.
            wait_for_result (bool): Whether to block and wait for messages.
            
        Returns:
            None or asyncio.Task: Returns a task if wait_for_result is False.
        """
        conn = cls._get_connection()
        pubsub = conn.pubsub()
        pubsub.subscribe(channel)

        async def listen():
            while True:
                message = pubsub.get_message(ignore_subscribe_messages=True)
                if message:
                    if callback:
                        callback(message)
                    else:
                        print(f"Received message: {message['data'].decode('utf-8')}")
                await asyncio.sleep(0.1)

        if wait_for_result:
            await listen()
        else:
            return asyncio.create_task(listen())

    @classmethod
    async def unsubscribe_from_channel(cls, channel, wait_for_result=False):
        """
        Unsubscribe from a Redis channel asynchronously.
        
        Args:
            channel (str): The channel to unsubscribe from.
            wait_for_result (bool): Whether to wait for the result.
            
        Returns:
            None or asyncio.Task: Returns a task if wait_for_result is False.
        """
        conn = cls._get_connection()
        pubsub = conn.pubsub()
        pubsub.unsubscribe(channel)

        if wait_for_result:
            await asyncio.to_thread(pubsub.close)
        else:
            return asyncio.create_task(asyncio.to_thread(pubsub.close))

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
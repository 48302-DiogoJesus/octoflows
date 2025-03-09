import redis

class IntermediateStorage:
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = None

    @classmethod
    def _get_connection(cls):
        return redis.Redis(
            host=cls.REDIS_HOST,
            port=cls.REDIS_PORT,
            db=cls.REDIS_DB,
            password=cls.REDIS_PASSWORD,
            decode_responses=True  # Auto-decode response bytes to strings
        )

    @classmethod
    def get(cls, key):
        conn = cls._get_connection()
        try:
            return conn.get(key)
        finally:
            conn.close()

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
        try:
            return conn.set(key, value, ex=expire)
        finally:
            conn.close()

    @classmethod
    def configure(cls, host=None, port=None, db=None, password=None):
        if host is not None:
            cls.REDIS_HOST = host
        if port is not None:
            cls.REDIS_PORT = port
        if db is not None:
            cls.REDIS_DB = db
        if password is not None:
            cls.REDIS_PASSWORD = password
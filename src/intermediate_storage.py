from dataclasses import dataclass
import redis

class IntermediateStorage:
    @dataclass
    class Config:
        host: str
        port: int
        password: str

    config: Config

    def __init__(self, config: Config) -> None:
        self.config = config
        self._connection: redis.Redis = self._get_or_create_connection(skip_verification=True)

    def _get_or_create_connection(self, skip_verification: bool = False) -> redis.Redis:
        if not skip_verification and self._verify_connection():
            return self._connection
        else:
            self._connection = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=0,
                password=self.config.password,
                decode_responses=False # Necessary to allow serialized bytes
            )
            return self._connection

    def get(self, key: str):
        conn = self._get_or_create_connection()
        if not conn.exists(key): return None
        return conn.get(key)

    def exists(self, key: str):
        conn = self._get_or_create_connection()
        return conn.exists(key)

    def set(self, key: str, value, expire=None):
        conn = self._get_or_create_connection()
        return conn.set(key, value, ex=expire)

    def increment_and_get(self, key: str):
        conn = self._get_or_create_connection()
        # Atomically increment and get the new value
        return conn.incr(key, amount=1)

    def close_connection(self):
        self._connection.close()

    def _verify_connection(self):
        try:
            return self._connection.ping()
        except redis.ConnectionError:
            return False
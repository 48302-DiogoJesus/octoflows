# from dataclasses import dataclass
# import redis

# import src.storage.storage as storage

# class RedisStorage(storage.Storage):
#     @dataclass
#     class Config(storage.Storage.Config):
#         host: str
#         port: int
#         password: str

#         def create_instance(self) -> "RedisStorage":
#             return RedisStorage(self)

#     def __init__(self, config: Config) -> None:
#         super().__init__()
#         self.redis_config = config
#         self._connection: redis.Redis = self._get_or_create_connection(skip_verification=True)

#     def _get_or_create_connection(self, skip_verification: bool = False) -> redis.Redis:
#         if not skip_verification and self._verify_connection():
#             return self._connection
#         else:
#             self._connection = redis.Redis(
#                 host=self.redis_config.host,
#                 port=self.redis_config.port,
#                 db=0,
#                 password=self.redis_config.password,
#                 decode_responses=False, # Necessary to allow serialized bytes
#                 socket_connect_timeout=5,
#                 socket_timeout=5
#             )
#             return self._connection

#     def get(self, key: str):
#         conn = self._get_or_create_connection()
#         if not conn.exists(key): return None
#         return conn.get(key)

#     def set(self, key: str, value, expire=None):
#         conn = self._get_or_create_connection()
#         return conn.set(key, value, ex=expire)

#     def atomic_increment_and_get(self, key: str):
#         conn = self._get_or_create_connection()
#         # Atomically increment and get the new value
#         return conn.incr(key, amount=1)

#     def exists(self, *keys: str):
#         conn = self._get_or_create_connection()
#         return conn.exists(*keys)
    
#     def close_connection(self):
#         self._connection.close()

#     def _verify_connection(self):
#         try:
#             return self._connection.ping()
#         except redis.ConnectionError:
#             return False

#     def keys(self, pattern: str) -> list:
#         conn = self._get_or_create_connection()
#         return conn.keys(pattern) # type: ignore

#     def mget(self, keys: list[str]) -> list:
#         conn = self._get_or_create_connection()
#         return conn.mget(keys) # type: ignore



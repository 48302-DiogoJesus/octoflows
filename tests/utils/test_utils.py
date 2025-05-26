from src.storage.redis_storage import RedisStorage
from src.storage.in_memory_storage import InMemoryStorage
from src.workers.local_worker import LocalWorker

redis_intermediate_storage_config = RedisStorage.Config(host="localhost", port=6379, password="redisdevpwd123")
inmemory_intermediate_storage_config = InMemoryStorage.Config()

localWorkerConfig = LocalWorker.Config(
    intermediate_storage_config=inmemory_intermediate_storage_config,
    metadata_storage_config=inmemory_intermediate_storage_config,
    metrics_storage_config=None,
)

def get_worker_config():
    return localWorkerConfig

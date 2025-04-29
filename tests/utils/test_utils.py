import os
import sys

from src.planning.dag_planner import DefaultDAGPlanner
from src.storage.in_memory_storage import InMemoryStorage
from src.storage.metrics.metrics_storage import MetricsStorage
from src.storage.redis_storage import RedisStorage
from src.worker import DockerWorker, LocalWorker
from src.worker_resource_configuration import TaskWorkerResourceConfiguration

redis_intermediate_storage_config = RedisStorage.Config(host="localhost", port=6379, password="redisdevpwd123")
inmemory_intermediate_storage_config = InMemoryStorage.Config()
selected_planner = DefaultDAGPlanner

localWorkerConfig = LocalWorker.Config(
    intermediate_storage_config=inmemory_intermediate_storage_config,
    metrics_storage_config=None,
    planner=selected_planner,
)

dockerWorkerConfig = DockerWorker.Config(
    docker_gateway_address="http://localhost:5000",
    intermediate_storage_config=redis_intermediate_storage_config,
    metrics_storage_config=MetricsStorage.Config(
        storage_config=RedisStorage.Config(host="localhost", port=6380, password="redisdevpwd123")
    ),
    available_resource_configurations=[
        TaskWorkerResourceConfiguration(cpus=2, memory_mb=256),
        TaskWorkerResourceConfiguration(cpus=3, memory_mb=512)
    ],
    planner=selected_planner,
)

def get_worker_config():
    return localWorkerConfig

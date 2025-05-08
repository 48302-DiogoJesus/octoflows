from src.planning.dag_planner import SimpleDAGPlanner
from src.storage.redis_storage import RedisStorage
from src.storage.in_memory_storage import InMemoryStorage
from src.storage.metrics.metrics_storage import MetricsStorage
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.workers.docker_worker import DockerWorker
from src.workers.local_worker import LocalWorker

redis_intermediate_storage_config = RedisStorage.Config(host="localhost", port=6379, password="redisdevpwd123")
inmemory_intermediate_storage_config = InMemoryStorage.Config()
selected_planner = SimpleDAGPlanner

localWorkerConfig = LocalWorker.Config(
    intermediate_storage_config=inmemory_intermediate_storage_config,
    metadata_storage_config=inmemory_intermediate_storage_config,
    metrics_storage_config=None,
)

dockerWorkerConfig = DockerWorker.Config(
    docker_gateway_address="http://localhost:5000",
    intermediate_storage_config=redis_intermediate_storage_config,
    metrics_storage_config=MetricsStorage.Config(
        storage_config=RedisStorage.Config(host="localhost", port=6380, password="redisdevpwd123")
    ),
    planner_config=SimpleDAGPlanner.Config(
        sla="avg",
        available_worker_resource_configurations=[
            TaskWorkerResourceConfiguration(cpus=2, memory_mb=256),
            TaskWorkerResourceConfiguration(cpus=3, memory_mb=512),
            TaskWorkerResourceConfiguration(cpus=4, memory_mb=1024)
        ],
    )
)

def get_worker_config():
    return localWorkerConfig

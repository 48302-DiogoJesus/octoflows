import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.planning.first_planner_algorithm import FirstPlannerAlgorithm
from src.planning.second_planner_algorithm import SecondPlannerAlgorithm
from src.storage.metrics.metrics_storage import MetricsStorage
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.workers.docker_worker import DockerWorker
from src.workers.local_worker import LocalWorker
from src.storage.in_memory_storage import InMemoryStorage
from src.storage.redis_storage import RedisStorage
from src.dag_task_node import DAGTask
import numpy as np

@DAGTask
def task(_: np.ndarray) -> np.ndarray:
    """
    perform an expensive memory computation
    return input string + ~ 10 bytes
    """
    size = 2500
    a = np.random.rand(size, size)
    b = np.random.rand(size, size)
    res = np.matmul(a, b)
    return res

redis_intermediate_storage_config = RedisStorage.Config(host="localhost", port=6379, password="redisdevpwd123")
inmemory_intermediate_storage_config = InMemoryStorage.Config()

# METRICS STORAGE
redis_metrics_storage_config = RedisStorage.Config(host="localhost", port=6380, password="redisdevpwd123")

localWorkerConfig = LocalWorker.Config(
    intermediate_storage_config=redis_intermediate_storage_config,
    metadata_storage_config=redis_intermediate_storage_config,  # will use the same as intermediate_storage_config
)

dockerWorkerConfig = DockerWorker.Config(
    docker_gateway_address="http://localhost:5000",
    intermediate_storage_config=redis_intermediate_storage_config,
    metrics_storage_config=MetricsStorage.Config(storage_config=redis_metrics_storage_config),
    planner_config=SecondPlannerAlgorithm.Config(
        sla="avg",
        available_worker_resource_configurations=[
            TaskWorkerResourceConfiguration(cpus=2, memory_mb=512),
            TaskWorkerResourceConfiguration(cpus=3, memory_mb=1024)
        ],
    )
    # planner_config=FirstPlannerAlgorithm.Config(
    #     sla="avg",
    #     worker_resource_configuration=TaskWorkerResourceConfiguration(cpus=3, memory_mb=1024),
    # )
)

# Define the workflow
a1 = task(np.random.rand(2500, 2500))
a2 = task(a1)
a3 = task(a2)
a4 = task(a3)
a5 = task(a4)
a6 = task(a5)
a7 = task(a6)
a8 = task(a7)

for i in range(1):
    start_time = time.time()
    result = a8.compute(dag_name="simple dag", config=dockerWorkerConfig)
    print(f"[{i} Result: {result} | Makespan: {time.time() - start_time}s")
import os
import sys
import time

# import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag.dag import FullDAG
from src.planning.dag_planner import SimpleDAGPlanner
from src.worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.metrics.metrics_storage import MetricsStorage
from src.storage.in_memory_storage import InMemoryStorage
from src.storage.redis_storage import RedisStorage
from src.worker import DockerWorker, LocalWorker
from src.dag_task_node import DAGTask, DAGTaskNode

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
    planner_config=SimpleDAGPlanner.Config(
        sla="avg",
        available_worker_resource_configurations=[
            TaskWorkerResourceConfiguration(cpus=2, memory_mb=256),
            TaskWorkerResourceConfiguration(cpus=3, memory_mb=512),
            TaskWorkerResourceConfiguration(cpus=4, memory_mb=1024)
        ],
    )
)

@DAGTask
def add(x: float, y: float) -> float:
    return x + y

# Define the workflow
L = range(512)
while len(L) > 1:
  L = list(map(add, L[0::2], L[1::2]))

sink: DAGTaskNode = L[0] # type: ignore
# sink.visualize_dag(output_file=os.path.join("..", "_dag_visualization", "tree_reduction"), open_after=True)

for i in range(1):
    start_time = time.time()
    # result = sink.compute(config=localWorkerConfig)
    result = sink.compute(config=dockerWorkerConfig)
    print(f"[{i}] Result: {result} | Makespan: {time.time() - start_time}s")
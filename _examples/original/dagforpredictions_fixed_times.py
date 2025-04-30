import os
import sys
import time

# import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag.dag import FullDAG
from src.planning.dag_planner import DefaultDAGPlanner, SimpleDAGPlanner
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
def time_task(dummy_data: int) -> int:
    time.sleep(.5)
    return dummy_data + 1

@DAGTask
def last_task(dummy_data_1: int, dummy_data_2: int, dummy_data_3: int, dummy_data_4: int, dummy_data_5: int) -> str:
    time.sleep(.5)
    return f"{dummy_data_1} {dummy_data_2} {dummy_data_3} {dummy_data_4} {dummy_data_5}"

# Define the workflow
"""
Good for testing resource downgrades outside the critical path because different branches have considerably different completion times
"""
b1_t1 = time_task(10)
b1_t2 = time_task(b1_t1)
b1_t3 = time_task(b1_t2)
b1_t4 = time_task(b1_t3)
b1_t5 = time_task(b1_t4)

b2_t1 = time_task(20)
b2_t2 = time_task(b2_t1)
b2_t3 = time_task(b2_t2)
b2_t4 = time_task(b2_t3)

b3_t1 = time_task(30)
b3_t2 = time_task(b3_t1)
b3_t3 = time_task(b3_t2)

b4_t1 = time_task(40)
b4_t2 = time_task(b4_t1)

b5_t1 = time_task(50)

sink_task = last_task(b1_t5, b2_t4, b3_t3, b4_t2, b5_t1)

for i in range(1):
    start_time = time.time()
    # result = sink.compute(config=localWorkerConfig)
    result = sink_task.compute(config=dockerWorkerConfig)
    print(f"[{i}] Result: {result} | Makespan: {time.time() - start_time}s")
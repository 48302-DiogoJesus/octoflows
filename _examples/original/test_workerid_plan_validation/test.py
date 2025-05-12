import os
import sys
import time
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from src.workers.docker_worker import DockerWorker
from src.workers.local_worker import LocalWorker
from src.storage.redis_storage import RedisStorage
from src.planning.dag_planner import DummyDAGPlanner, SimpleDAGPlanner
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.metrics.metrics_storage import MetricsStorage
from src.storage.in_memory_storage import InMemoryStorage
from src.dag_task_node import DAGTask

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
    planner_config=DummyDAGPlanner.Config(sla="avg")
)

@DAGTask
def dummy_task(dummy_data: int):
    pass

# Define the workflow
"""
Good for testing resource downgrades outside the critical path because different branches have considerably different completion times
"""
t1 = dummy_task(1)
t1.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="A"))

t2 = dummy_task(t1)
t2.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="B"))
t3 = dummy_task(t1)
t3.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="B"))

t4 = dummy_task(t2)
t4.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="C"))
t5 = dummy_task(t3, t4)
t5.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="B"))

t6 = dummy_task(t5)
t6.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="C"))
t7 = dummy_task(t5)
t7.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="C"))

t8 = dummy_task(t6, t7)
t8.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="C"))

# t8.visualize_dag(open_after=True)
# exit()

for i in range(1):
    start_time = time.time()
    # result = sink.compute(config=localWorkerConfig)
    result = t8.compute(config=dockerWorkerConfig)
    print(f"[{i}] Result: {result} | Makespan: {time.time() - start_time}s")
import os
import sys
import time
import numpy as np
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.dag_task_node import DAGTask
from src.workers.docker_worker import DockerWorker
from src.storage.redis_storage import RedisStorage
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.metrics.metrics_storage import MetricsStorage
from src.planning.dummy_planner import DummyDAGPlanner

@DAGTask
def dummy_task(*dummy_data: int):
    pass

## Workflow 1 (INVALID)
# t1 = dummy_task(1)
# t1.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="A"))

# t2 = dummy_task(t1)
# t2.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="B"))
# t3 = dummy_task(t1)
# t3.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="B"))

# t4 = dummy_task(t2)
# t4.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="C"))
# t5 = dummy_task(t3, t4)
# t5.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="B"))

# t6 = dummy_task(t5) # !SHOULD TRIGGER INVALID HERE!
# t6.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="C"))
# t7 = dummy_task(t5)
# t7.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="C"))

# wf = dummy_task(t6, t7)
# wf.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="C"))

## Workflow 2 (VALID)
# t1 = dummy_task(1)
# t1.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="A"))

# t2 = dummy_task(t1)
# t2.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="B"))
# t3 = dummy_task(t1)
# t3.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="B"))

# t4 = dummy_task(t2)
# t4.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="B"))
# t5 = dummy_task(t3)
# t5.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="B"))

# wf = dummy_task(t4, t5)
# wf.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="C"))

## Workflow 3 (VALID)
# t1 = dummy_task(1)
# t1.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="A"))

# t2 = dummy_task(2)
# t2.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="B"))

# t3 = dummy_task(3)
# t3.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="C"))

# t4 = dummy_task(t1, t2, t3)
# t4.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="D"))

# wf = dummy_task(t4)
# wf.add_annotation(TaskWorkerResourceConfiguration(cpus=1, memory_mb=1, worker_id="D"))

docker_worker_config = DockerWorker.Config(
    docker_gateway_address="http://localhost:5000",
    intermediate_storage_config=RedisStorage.Config(
        host="localhost",
        port=6379,
        password="redisdevpwd123"
    ),
    metrics_storage_config=MetricsStorage.Config(storage_config=RedisStorage.Config(
        host="localhost",
        port=6380,
        password="redisdevpwd123"
    )),
    planner_config=DummyDAGPlanner.Config(sla="median")
)

for i in range(1):
    start_time = time.time()
    result = wf.compute(dag_name="test_worker_validation", config=docker_worker_config)
    print(f"[{i}] Result: {result} | Makespan: {time.time() - start_time}s")
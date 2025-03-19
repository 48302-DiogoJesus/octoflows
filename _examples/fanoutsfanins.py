import os
import sys
import time
# import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.storage.in_memory_storage import InMemoryStorage
from src.storage.redis_storage import RedisStorage
from src.worker import DockerWorker, LocalWorker
from src.dag_task_node import DAGTask

@DAGTask
def a(x: float, y: float) -> float:
    return x * y

@DAGTask
def b(values: list[float]) -> float:
    return sum(values)

@DAGTask
def c(values: float) -> float:
    return values * values

redis_intermediate_storage_config = RedisStorage.Config(host="localhost", port=6379, password="redisdevpwd123")
inmemory_intermediate_storage_config = InMemoryStorage.Config()

localWorkerConfig = LocalWorker.Config(
    intermediate_storage_config=inmemory_intermediate_storage_config
)

dockerWorkerConfig = DockerWorker.Config(
    docker_gateway_address="http://localhost:5000",
    intermediate_storage_config=redis_intermediate_storage_config
)

# Define the workflow
a1 = a(2.0, 3.0)
a2 = a(3.0, 4.0)
a3 = a(4.0, 5.0)
b1 = b([a1, a2, a3])
c1 = c(b1)
c2 = c(b1)
c3 = c(b1)
b2 = b([c1, c2, c3])

# c5_out = c(b1)

# b2.visualize_dag()
start_time = time.time()
result = b2.compute(config=localWorkerConfig)
print(f"Total Revenue: ${result} | Makespan: {time.time() - start_time}s")
result2 = b2.compute(config=localWorkerConfig)
print(f"Total Revenue: ${result2} | Makespan: {time.time() - start_time}s")

# result2 = total_revenue.compute(local=True)
# print(f"Total Revenue: ${result2}")
import os
import sys
import time
import numpy as np

# Add parent directory to path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import get_worker_config

# Import common worker configurations
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.worker_config import (
    IN_MEMORY_STORAGE_CONFIG,
    REDIS_INTERMEDIATE_STORAGE_CONFIG,
    REDIS_METRICS_STORAGE_CONFIG
)

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

# Get worker configuration
worker_config = get_worker_config(
    worker_type="docker",
    planner_type="first",
    worker_resource_configuration={"cpus": 3, "memory_mb": 512},
    available_worker_resource_configurations=[
        {"cpus": 2, "memory_mb": 512},
        {"cpus": 3, "memory_mb": 1024}
    ]
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
    result = a8.compute(dag_name="simpledag", config=worker_config)
    print(f"[{i} Result: {result} | Makespan: {time.time() - start_time}s")
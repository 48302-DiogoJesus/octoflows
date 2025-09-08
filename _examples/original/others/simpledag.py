import os
import sys
import time
import numpy as np

# Add parent directory to path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import WORKER_CONFIG

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

# Define the workflow
a1 = task(np.random.rand(2500, 2500))
a2 = task(a1)
a3 = task(a2)
a4 = task(a3)
a5 = task(a4)
a6 = task(a5)
a7 = task(a6)
a8 = task(a7)

start_time = time.time()
result = a8.compute(dag_name="simpledag", config=WORKER_CONFIG)
print(f"Result: {result} | Makespan: {time.time() - start_time}s")
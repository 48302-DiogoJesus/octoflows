import os
import sys
import time

# Add parent directory to path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import WORKER_CONFIG

@DAGTask
def a(x: float, y: float) -> float:
    return x * y

@DAGTask
def b(values: list[float]) -> float:
    return sum(values)

@DAGTask
def c(values: float) -> float:
    return values * values

# Define the workflow
a1 = a(2.0, 3.0)
a2 = a(3.0, 4.0)
a3 = a(4.0, 5.0)
b1 = b([a1, a2, a3])
c1 = c(b1)
c2 = c(b1)
c3 = c(b1)
b2 = b([c1, c2, c3])
c4 = c(b2)

# c4.visualize_dag(output_file=os.path.join("..", "_dag_visualization", "fanoutsfanins"), open_after=True)

start_time = time.time()
result = c4.compute(dag_name="fanoutsfanins", config=WORKER_CONFIG)
print(f"Total Revenue: ${result} | Makespan: {time.time() - start_time}s")
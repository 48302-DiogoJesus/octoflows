import os
import sys
import time
# import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask, DAGTaskNode

# Import common worker configurations
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import WORKER_CONFIG

@DAGTask
def add(x: float, y: float) -> float:
    return x + y

# Define the workflow
#!Note: The dev faas docker engine has a limit on the number of containers available at the same time, which limits the amount of branches that can exist at a time. Could lead to a situation where workers are just hanging because they can't create new containers.
L = range(64)
while len(L) > 1:
  L = list(map(add, L[0::2], L[1::2]))

sink: DAGTaskNode = L[0] # type: ignore
# sink.visualize_dag(output_file=os.path.join("..", "_dag_visualization", "tree_reduction"), open_after=True)

for i in range(1):
    start_time = time.time()
    result = sink.compute(dag_name="tree_reduction", config=WORKER_CONFIG, open_dashboard=False)
    print(f"[{i}] Result: {result} | Makespan: {time.time() - start_time}s")
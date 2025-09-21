import os
import sys
import time
import numpy as np

# Add parent directory to path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from src.dag_task_node import DAGTask
from src.planning.optimizations.preload import PreLoadOptimization

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.config import WORKER_CONFIG

# 1) Task definition
@DAGTask
def task_a(a: int) -> int:
    return a + 1

@DAGTask(forced_optimizations=[PreLoadOptimization()])
def task_b(*args: int) -> int:
    return sum(args)

# 2) Task composition (DAG/Workflow)
a1 = task_a(10)
a2 = task_a(a1)
a3 = task_a(a1)
b1 = task_b(a2, a3)
a4 = task_a(b1)

a4.visualize_dag()
exit()

start_time = time.time()
result = a4.compute(dag_name="simpledag", config=WORKER_CONFIG)
# result = a4.compute(
#     dag_name="simpledag", 
#     config=Worker.Config(
#         faas_gateway_address="http://localhost:5000",
#         intermediate_storage_config=...,
#         metrics_storage_config=...,
#         planner_config=SimplePlannerAlgorithm.Config(
#             sla=sla,
#             all_flexible_workers=False,
#             worker_resource_configuration=TaskWorkerResourceConfiguration(cpus=3, memory_mb=256),
#         )
#     )
# )
print(f"Result: {result} | Makespan: {time.time() - start_time}s")
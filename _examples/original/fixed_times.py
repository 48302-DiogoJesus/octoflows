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
def time_task(*dummy_data: str) -> str:
    time.sleep(.5)
    st = ""
    for d in dummy_data: st += d
    return st

# Define the workflow
"""
Good for testing resource downgrades outside the critical path because different branches have considerably different completion times
"""
b1_t1 = time_task("a")
b1_t2 = time_task(b1_t1)
b1_t3 = time_task(b1_t2)
b1_t4 = time_task(b1_t3)
b1_t5 = time_task(b1_t4)

b2_t1 = time_task("b")
b2_t2 = time_task(b2_t1)
b2_t3 = time_task(b2_t2)
b2_t4 = time_task(b2_t3)

b3_t1 = time_task("c")
b3_t2 = time_task(b3_t1)
b3_t3 = time_task(b3_t2)

b4_t1 = time_task("d")
b4_t2 = time_task(b4_t1)

b5_t1 = time_task("e")

sink_task = time_task(b1_t5, b2_t4, b3_t3, b4_t2, b5_t1)

start_time = time.time()
result = sink_task.compute(dag_name="fixed_execution_times", config=WORKER_CONFIG)
print(f"Result: {result} | Makespan: {time.time() - start_time}s")
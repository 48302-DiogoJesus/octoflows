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
def time_task_short(*dummy_data: str) -> str:
    time.sleep(.2)
    st = ""
    for d in dummy_data: st += d
    return st

@DAGTask
def time_task_long(*dummy_data: str) -> str:
    time.sleep(2)
    st = ""
    for d in dummy_data: st += d
    return st

# Define the workflow
"""
Good for testing resource downgrades outside the critical path because different branches have considerably different completion times
"""
b1_t1 = time_task_long("a")
b1_t2 = time_task_long(b1_t1)
b1_t3 = time_task_long(b1_t2)
b1_t4 = time_task_long(b1_t3)
b1_t5 = time_task_long(b1_t4)

b2_t1 = time_task_short(b1_t2)
b2_t2 = time_task_short(b2_t1)

b3_t1 = time_task_short(b1_t2)
b3_t2 = time_task_short(b3_t1)

b4_t1 = time_task_short(b1_t2)
b4_t2 = time_task_short(b4_t1)

b5_t1 = time_task_short(b1_t2)
b5_t2 = time_task_short(b5_t1)

sink_task = time_task_short(b2_t2, b1_t5, b3_t2, b4_t2, b5_t2)

for i in range(1):
    start_time = time.time()
    # result = sink.compute(config=localWorkerConfig)
    result = sink_task.compute(dag_name="test_prewarm", config=WORKER_CONFIG)
    print(f"[{i}] Result: {result} | Makespan: {time.time() - start_time}s")
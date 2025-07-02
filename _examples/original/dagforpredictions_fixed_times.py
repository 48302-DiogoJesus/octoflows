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
def time_task(dummy_data: int) -> int:
    time.sleep(.5)
    return dummy_data + 1

@DAGTask
def last_task(dummy_data_1: int, dummy_data_2: int, dummy_data_3: int, dummy_data_4: int, dummy_data_5: int) -> str:
    time.sleep(.5)
    return f"{dummy_data_1} {dummy_data_2} {dummy_data_3} {dummy_data_4} {dummy_data_5}"

# Define the workflow
"""
Good for testing resource downgrades outside the critical path because different branches have considerably different completion times
"""
b1_t1 = time_task(10)
b1_t2 = time_task(b1_t1)
b1_t3 = time_task(b1_t2)
b1_t4 = time_task(b1_t3)
b1_t5 = time_task(b1_t4)

b2_t1 = time_task(20)
b2_t2 = time_task(b2_t1)
b2_t3 = time_task(b2_t2)
b2_t4 = time_task(b2_t3)

b3_t1 = time_task(30)
b3_t2 = time_task(b3_t1)
b3_t3 = time_task(b3_t2)

b4_t1 = time_task(40)
b4_t2 = time_task(b4_t1)

b5_t1 = time_task(50)

sink_task = last_task(b1_t5, b2_t4, b3_t3, b4_t2, b5_t1)

for i in range(1):
    start_time = time.time()
    # result = sink.compute(config=localWorkerConfig)
    result = sink_task.compute(dag_name="fixed_execution_times", config=WORKER_CONFIG)
    print(f"[{i}] Result: {result} | Makespan: {time.time() - start_time}s")
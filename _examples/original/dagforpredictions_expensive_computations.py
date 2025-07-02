import os
import sys
import time
import argparse
import numpy as np

# Add parent directory to path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import get_worker_config

# Parse command line arguments
parser = argparse.ArgumentParser(description='Run DAG with specified planner type')
parser.add_argument('planner_type', nargs='?', default='first',
                   choices=['simple', 'first', 'second'],
                   help='Type of planner to use: simple, first, or second')
args = parser.parse_args()
planner_type = args.planner_type.lower()

if planner_type not in ['simple', 'first', 'second']:
    print("Please specify a valid planner type: simple, first, or second")
    print("Usage: python", sys.argv[0], "[simple|first|second]")
    sys.exit(1)

# Get worker configuration
worker_config = get_worker_config(worker_type="docker", planner_type=planner_type)

@DAGTask
def time_task_expensive(dummy_data: int) -> int:
    # memory-sensitive computation
    size = 2500
    a = np.random.rand(size, size)
    b = np.random.rand(size, size)
    result = np.matmul(a, b)
    return dummy_data + int(result[0, 0] % 100)  # Just use a small part of the result

@DAGTask
def time_task_more_expensive_task(dummy_data: int) -> int:
    # memory-sensitive computation
    size = 4300
    a = np.random.rand(size, size)
    b = np.random.rand(size, size)
    result = np.matmul(a, b)
    return dummy_data + int(result[0, 0] % 100)  # Just use a small part of the result

@DAGTask
def last_task_expensive(dummy_data_1: int, dummy_data_2: int, dummy_data_3: int, 
                       dummy_data_4: int, dummy_data_5: int) -> str:
    # memory-sensitive computation
    size = 2500
    matrices = [np.random.rand(size, size) for _ in range(5)]
    result = matrices[0]
    for m in matrices[1:]:
        result = np.matmul(result, m)  # Chained matrix multiplications
    
    modifier = int(result[0, 0] % 100)
    return f"{dummy_data_1+modifier} {dummy_data_2} {dummy_data_3} {dummy_data_4} {dummy_data_5}"

# Define the workflow
"""
Good for testing resource downgrades outside the critical path because different branches have considerably different completion times
"""
b1_t1 = time_task_expensive(10)
b1_t2 = time_task_expensive(b1_t1)
b1_t3 = time_task_expensive(b1_t2)
b1_t4 = time_task_expensive(b1_t3)
b1_t5 = time_task_expensive(b1_t4)

b2_t1 = time_task_expensive(20)
b2_t2 = time_task_expensive(b2_t1)
b2_t3 = time_task_expensive(b2_t2)
b2_t4 = time_task_expensive(b2_t3)

b3_t1 = time_task_expensive(30)
b3_t2 = time_task_expensive(b3_t1)
b3_t3 = time_task_expensive(b3_t2)

b4_t1 = time_task_expensive(40)
b4_t2 = time_task_expensive(b4_t1)

b5_t1 = time_task_more_expensive_task(50)

sink_task = last_task_expensive(b1_t5, b2_t4, b3_t3, b4_t2, b5_t1)
# sink_task.visualize_dag(open_after=True)

for i in range(1):
    start_time = time.time()
    result = sink_task.compute(dag_name="memory_intensive_computations", config=worker_config, open_dashboard=False)
    print(f"[{i}] Result: {result} | Makespan: {time.time() - start_time}s")
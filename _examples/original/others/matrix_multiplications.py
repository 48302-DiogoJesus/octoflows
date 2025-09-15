import os
import sys
import time
import numpy as np

# Add parent directory to path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from _examples.common.config import WORKER_CONFIG

@DAGTask
def mmul_500(dummy_data: int) -> int:
    # memory-sensitive computation
    size = 500
    a = np.random.rand(size, size)
    b = np.random.rand(size, size)
    result = np.matmul(a, b)
    return dummy_data + int(result[0, 0] % 100)  # Just use a small part of the result

@DAGTask
def mmul_2500(dummy_data: int) -> int:
    # memory-sensitive computation
    size = 2500
    a = np.random.rand(size, size)
    b = np.random.rand(size, size)
    result = np.matmul(a, b)
    return dummy_data + int(result[0, 0] % 100)  # Just use a small part of the result

@DAGTask
def fixed_time_task_5_seconds(dummy_data: int) -> int:
    time.sleep(5)
    return dummy_data

@DAGTask
def mmul_4000(dummy_data: int) -> int:
    # memory-sensitive computation
    size = 4000
    a = np.random.rand(size, size)
    b = np.random.rand(size, size)
    result = np.matmul(a, b)
    return dummy_data + int(result[0, 0] % 100)  # Just use a small part of the result

@DAGTask
def mmul_fan_in(*dummy_data: int) -> int:
    size = 1000
    num_matrices = max(1, len(dummy_data) // 2)  # Scale number of matrices based on input size
    
    # Generate random matrices
    matrices = [np.random.rand(size, size) for _ in range(num_matrices)]
    
    # Chained matrix multiplications
    if num_matrices > 1:
        result = matrices[0]
        for m in matrices[1:]:
            result = np.matmul(result, m)
        modifier = int(result[0, 0] % 100)
    else:
        modifier = int(matrices[0][0, 0] % 100)
        
    return sum(dummy_data) + modifier

# Define the workflow
"""
Good for testing resource downgrades outside the critical path because different branches have considerably different completion times
"""
b1_t1 = mmul_2500(10)
b1_t2 = mmul_2500(b1_t1)
b1_t3 = mmul_2500(b1_t2)
b1_t4 = mmul_2500(b1_t3)
b1_t5 = mmul_2500(b1_t4)

b2_t1 = mmul_2500(20)
b2_t2 = fixed_time_task_5_seconds(b2_t1)
b2_t2_t1 = mmul_2500(b2_t2)
b2_t2_t2 = mmul_2500(b2_t2)
b2_t2_t3 = mmul_2500(b2_t2)
b2_t2_t4 = mmul_2500(b2_t2)
b2_t2_t5 = mmul_2500(b2_t2)
b2_t2_t6 = mmul_2500(b2_t2)
b2_t2_t7 = mmul_2500(b2_t2)
b2_t3 = mmul_fan_in(b2_t2_t1, b2_t2_t2, b2_t2_t3, b2_t2_t4, b2_t2_t5, b2_t2_t6, b2_t2_t7)
b2_t4 = mmul_2500(b2_t3)

b3_t1 = mmul_2500(30)
b3_t2 = mmul_2500(b3_t1)
b3_t3 = mmul_2500(b3_t2)

b4_t1 = mmul_2500(40)
b4_t1_t1 = mmul_2500(b4_t1)
b4_t1_t2 = mmul_2500(b4_t1)
b4_t2 = mmul_fan_in(b4_t1_t1, b4_t1_t2)

b5_t1 = mmul_4000(50)

b6 = mmul_fan_in(b1_t5, b2_t4, b3_t3, b4_t2, b5_t1)
sink_task = mmul_fan_in(b6)

start_time = time.time()
result = sink_task.compute(dag_name="memory_intensive_computations", config=WORKER_CONFIG, open_dashboard=False)
print(f"Result: {result} | Makespan: {time.time() - start_time}s")
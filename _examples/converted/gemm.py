import os
import sys
import time
import numpy as np
import hashlib
import math

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.storage.in_memory_storage import InMemoryStorage
from src.storage.redis_storage import RedisStorage
from src.worker import DockerWorker, LocalWorker
from src.dag_task_node import DAGTask

redis_intermediate_storage_config = RedisStorage.Config(host="localhost", port=6379, password="redisdevpwd123")
inmemory_intermediate_storage_config = InMemoryStorage.Config()

localWorkerConfig = LocalWorker.Config(
    intermediate_storage_config=inmemory_intermediate_storage_config
)

dockerWorkerConfig = DockerWorker.Config(
    docker_gateway_address="http://localhost:5000",
    intermediate_storage_config=redis_intermediate_storage_config
)

def create_matrix_chunks(matrix, row_chunk_size=1, col_chunk_size=1):
    """Split matrix into smaller chunks based on specified sizes"""
    chunks = []
    for i in range(0, matrix.shape[0], row_chunk_size):
        for j in range(0, matrix.shape[1], col_chunk_size):
            chunk = matrix[i:i+row_chunk_size, j:j+col_chunk_size]
            chunks.append(((i, j), chunk))  # Store position and chunk
    return chunks

@DAGTask
def multiply_chunks(a_chunk_with_pos, b_chunk_with_pos):
    """Multiply two matrix chunks and return result with position"""
    (i_a, _), a_chunk = a_chunk_with_pos
    (_, j_b), b_chunk = b_chunk_with_pos
    product = np.matmul(a_chunk, b_chunk)
    return ((i_a, j_b), product)

@DAGTask
def aggregate_results(partial_results, final_shape):
    """Combine all partial results into final matrix"""
    result = np.zeros(final_shape)
    for position, value in partial_results:
        i, j = position
        rows, cols = value.shape
        result[i:i+rows, j:j+cols] = value
    return result

if __name__ == "__main__":
    # Original matrices
    # matrix_a = np.array([
    #     [5, 2, 8, 1],
    #     [3, 6, 4, 9],
    #     [7, 2, 5, 3]
    # ])

    # matrix_b = np.array([
    #     [4, 7],
    #     [2, 1],
    #     [5, 3],
    #     [8, 6]
    # ])

    RANDOM_MATRIX_COLS = 1000
    RANDOM_MATRIX_ROWS = 1000
    CHUNK_SIZE = 100

    def generate_matrices(rows_a, cols_a):
        matrix_a = np.random.randint(1, 10, (rows_a, cols_a))
        matrix_b = np.random.randint(1, 10, (cols_a, rows_a))
        
        return matrix_a, matrix_b

    start_time = time.time()
    matrix_a, matrix_b = generate_matrices(RANDOM_MATRIX_ROWS, RANDOM_MATRIX_COLS)
    print(f"Random matrices ({RANDOM_MATRIX_ROWS}x{RANDOM_MATRIX_COLS}) generated in {time.time() - start_time:.4f} seconds")

    start_time = time.time()
    # ! Not included in the workflow, not @DAGTask
    a_chunks = create_matrix_chunks(matrix_a, row_chunk_size=CHUNK_SIZE, col_chunk_size=CHUNK_SIZE)
    # ! Not included in the workflow, not @DAGTask
    b_chunks = create_matrix_chunks(matrix_b, row_chunk_size=CHUNK_SIZE, col_chunk_size=CHUNK_SIZE)
    print(f"Created {len(a_chunks) + len(b_chunks)} chunks for matrices in {time.time() - start_time:.4f} seconds")

    start_time = time.time()
    partial_results = []
    for a_chunk in a_chunks:
        for b_chunk in b_chunks:
            result = multiply_chunks(a_chunk, b_chunk)
            partial_results.append(result)

    print(f"Created {len(partial_results)} partial results in {time.time() - start_time:.4f} seconds")

    distributed_result = aggregate_results(partial_results, (matrix_a.shape[0], matrix_b.shape[1]))

    # distributed_result.visualize_dag(output_file=os.path.join("..", "_dag_visualization", "gemm"), open_after=True)

    correct_result = np.matmul(matrix_a, matrix_b)

    start_time = time.time()
    distributed_result = distributed_result.compute(config=localWorkerConfig)
    print(f"GEMM completed in {time.time() - start_time:.4f} seconds")
    print(f"Is Multiplication correct: {np.allclose(correct_result, distributed_result)}")
    print(f"Correct Result: {correct_result}")
    print(f"Distributed Result: {distributed_result}")
import os
import sys
import time
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import WORKER_CONFIG


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
    (i_a, k_a), a_chunk = a_chunk_with_pos
    (k_b, j_b), b_chunk = b_chunk_with_pos

    # Only multiply if inner dimensions match
    if a_chunk.shape[1] != b_chunk.shape[0]:
        return None

    product = np.matmul(a_chunk, b_chunk)
    return ((i_a, j_b), product)


@DAGTask
def aggregate_results(partial_results, final_shape, alpha=1.0, beta=0.0, C_init=None):
    """Combine all partial results into final matrix (full GEMM: C = alpha*A*B + beta*C)"""
    result = np.zeros(final_shape) if C_init is None else beta * C_init

    # Accumulate instead of overwrite
    for entry in partial_results:
        if entry is None:
            continue
        position, value = entry
        i, j = position
        rows, cols = value.shape
        result[i:i+rows, j:j+cols] += alpha * value
    return result


RANDOM_MATRIX_COLS = 1_000
RANDOM_MATRIX_ROWS = 1_000
CHUNK_SIZE = 400

start_time = time.time()
matrix_a = np.random.randint(1, 10, (RANDOM_MATRIX_ROWS, RANDOM_MATRIX_COLS))
matrix_b = np.random.randint(1, 10, (RANDOM_MATRIX_COLS, RANDOM_MATRIX_ROWS))
print(f"Random matrices ({RANDOM_MATRIX_ROWS}x{RANDOM_MATRIX_COLS}) generated in {time.time() - start_time:.4f} seconds")

start_time = time.time()
a_chunks = create_matrix_chunks(matrix_a, row_chunk_size=CHUNK_SIZE, col_chunk_size=CHUNK_SIZE)
b_chunks = create_matrix_chunks(matrix_b, row_chunk_size=CHUNK_SIZE, col_chunk_size=CHUNK_SIZE)
print(f"Created {len(a_chunks) + len(b_chunks)} chunks for matrices in {time.time() - start_time:.4f} seconds")

start_time = time.time()
partial_results = []
for a_chunk in a_chunks:
    for b_chunk in b_chunks:
        # Only valid if column index of A matches row index of B
        if a_chunk[1].shape[1] == b_chunk[1].shape[0]:
            result = multiply_chunks(a_chunk, b_chunk)
            partial_results.append(result)

distributed_result = aggregate_results(
    partial_results,
    (matrix_a.shape[0], matrix_b.shape[1]),
    alpha=1.0,
    beta=0.0,
    C_init=None
)

# distributed_result.visualize_dag(output_file=os.path.join("..", "_dag_visualization", "gemm"), open_after=True)

start_time = time.time()
distributed_result.compute(dag_name="gemm", config=WORKER_CONFIG, open_dashboard=False)
print(f"GEMM completed in {time.time() - start_time:.4f} seconds")

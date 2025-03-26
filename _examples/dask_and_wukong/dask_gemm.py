import hashlib
import time
import dask.array as da
import numpy as np

def hash_matrix(matrix):
    matrix_bytes = np.round(matrix, decimals=6).tobytes()
    return hashlib.sha256(matrix_bytes).hexdigest()

# matrix dimensions
m, n, k = 1000, 1000, 1000

# random matrices
A = da.random.random((m, k), chunks=(250, 250))
B = da.random.random((k, n), chunks=(250, 250))

# matrix multiplication
C = da.matmul(A, B)

start_time = time.time()
# C.visualize(filename="gemm_dag", format="png")
C_result = C.compute()

print(f"GEMM | Hash: {hash_matrix(C_result)} | Result: {C_result} completed in {time.time() - start_time:.4f} seconds")

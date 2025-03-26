import hashlib
import time
import dask.array as da
import numpy as np

def hash_matrix(matrix):
    matrix_bytes = np.round(matrix, decimals=6).tobytes()
    return hashlib.sha256(matrix_bytes).hexdigest()

# random matrices
A = da.random.random((1000, 1000), chunks=(50, 50))
B = da.random.random((1000, 1000), chunks=(50, 50))
C = da.matmul(A, B)

start_time = time.time()
# C.visualize(filename="gemm_dag", format="png")
C_result = C.compute()

print(f"GEMM | Hash: {hash_matrix(C_result)} | Result: {C_result} completed in {time.time() - start_time:.4f} seconds")

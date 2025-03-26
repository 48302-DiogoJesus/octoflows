import time
import dask.array as da

# matrix dimensions
m, n, k = 1000, 1000, 1000

# random matrices
A = da.random.random((m, k), chunks=(250, 250))
B = da.random.random((k, n), chunks=(250, 250))

# matrix multiplication
C = da.matmul(A, B)

start_time = time.time()
C_result = C.compute()

print(f"GEMM operation completed in {time.time() - start_time:.4f} seconds")

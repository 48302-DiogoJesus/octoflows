import time
import dask.array as da

# Create a random Dask array
# Example: 1000x500 matrix
X = da.random.random((10_000, 10_000), chunks=(2_000, 2_000))
u, s, v = da.linalg.svd_compressed(X, k=5)

start_time = time.time()
result = v.compute()
print(f"{time.time() - start_time}s Result: {type(result)} {result}") 
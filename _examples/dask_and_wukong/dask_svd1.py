import time
import dask.array as da

# Create a random Dask array
# Example: 1000x500 matrix
X = da.random.random((200000, 1000), chunks=(10000, 1000))
u, s, v = da.linalg.svd(X)

# Start the computation.
# v.visualize(filename="svd_dag", format="png")
start_time = time.time()
result = v.compute()
print(f"{time.time() - start_time}s Result: {type(result)} {result}") 
import time
import dask.array as da
import numpy as np

def compare_svd():
    # Create a random Dask array
    X = da.random.random((20_000, 1_000), chunks=(1_000, 1_000))

    # Compute SVD multiple times
    start_time = time.time()
    u1, s1, v1 = da.linalg.svd(X)
    u2, s2, v2 = da.linalg.svd(X)

    # Compute and compare results
    u1_val = u1.compute()
    s1_val = s1.compute()
    v1_val = v1.compute()

    u2_val = u2.compute()
    s2_val = s2.compute()
    v2_val = v2.compute()

    # Check similarity of singular values
    print("Singular values comparison:")
    print("Max difference in singular values:", np.max(np.abs(s1_val - s2_val)))

    # Check similarity of matrix norms
    print("\nMatrix norms:")
    print("U matrix norm difference:", np.linalg.norm(u1_val - u2_val))
    print("V matrix norm difference:", np.linalg.norm(v1_val - v2_val))

    # Compute reconstruction error
    X_reconstructed1 = u1_val @ np.diag(s1_val) @ v1_val
    X_reconstructed2 = u2_val @ np.diag(s2_val) @ v2_val
    print("\nReconstruction error:", np.linalg.norm(X_reconstructed1 - X_reconstructed2))

compare_svd()
import numpy as np
import threading

def compute_XXT(X, result_dict):
    """Compute XX^T and store in shared dict."""
    result_dict['XXT'] = X @ X.T

def compute_XTX(X, result_dict):
    """Compute X^T X and store in shared dict."""
    result_dict['XTX'] = X.T @ X

def eigen_decomposition(matrix, result_dict, key):
    """Compute eigenvalues and eigenvectors."""
    eigvals, eigvecs = np.linalg.eigh(matrix)
    result_dict[key] = (eigvals, eigvecs)

def compute_singular_values(eigenvalues, k):
    """Compute top-k singular values as sqrt of eigenvalues."""
    return np.sqrt(np.abs(eigenvalues[-k:]))[::-1]  # Keep only the largest k values

def compute_svd_threaded(X):
    """Parallelized SVD computation using threads."""
    results = {}

    # Step 1: Compute XX^T and X^T X in parallel using threads
    t1 = threading.Thread(target=compute_XXT, args=(X, results))
    t2 = threading.Thread(target=compute_XTX, args=(X, results))
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Step 2: Compute eigenvalue decompositions in parallel using threads
    t3 = threading.Thread(target=eigen_decomposition, args=(results['XXT'], results, 'U'))
    t4 = threading.Thread(target=eigen_decomposition, args=(results['XTX'], results, 'V'))

    t3.start()
    t4.start()
    t3.join()
    t4.join()

    m, n = X.shape
    k = min(m, n)  # Only keep the top k singular values

    eigenvalues_U, U = results['U']
    _, V = results['V']  # We use only the eigenvectors

    # Step 3: Compute top-k singular values
    S = compute_singular_values(eigenvalues_U, k)
    
    return U[:, -k:], S, V[:, -k:].T  # Take only the relevant singular vectors

# Generate random test matrix
np.random.seed(42)
X = np.random.rand(100, 50)  # Sample matrix

# Compute SVD using our threaded method
U_threaded, S_threaded, V_threaded = compute_svd_threaded(X)

# Compute SVD using NumPy's built-in function
U_np, S_np, V_np = np.linalg.svd(X, full_matrices=False)

# Ensure singular values are close
singular_values_match = np.allclose(S_threaded, S_np, atol=1e-6)
print("Singular values match:", singular_values_match)

# Ensure left singular vectors match up to sign flips
U_match = np.allclose(np.abs(U_threaded), np.abs(U_np), atol=1e-6)
print("Left singular vectors match (up to sign):", U_match)

# Ensure right singular vectors match up to sign flips
V_match = np.allclose(np.abs(V_threaded), np.abs(V_np), atol=1e-6)
print("Right singular vectors match (up to sign):", V_match)

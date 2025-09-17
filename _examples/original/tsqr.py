import os
import sys
import time
import numpy as np

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import WORKER_CONFIG

# --- Dask TSQR implementation for result correctness check --- #
import dask.array as da
from dask.array.linalg import tsqr

def tsqr_decomposition(A, chunk_size=1000):
    """
    Perform TSQR decomposition on a tall-and-skinny matrix.
    
    Parameters:
    -----------
    A : array-like
        Input matrix (can be numpy array or dask array)
    chunk_size : int, optional
        Chunk size for rows if A is not already a dask array
    
    Returns:
    --------
    Q, R : dask arrays
        Q is orthogonal matrix, R is upper triangular matrix
        such that A = Q @ R
    """
    # Convert to dask array if needed
    if not isinstance(A, da.Array):
        A = da.from_array(A, chunks=(chunk_size, A.shape[1]))
    
    # Perform TSQR decomposition
    Q, R = tsqr(A)
    
    return Q, R


def compare_tsqr_results(A, other_Q, other_R, tolerance=1e-8, chunk_size=1000):
    """
    Compare results from another TSQR implementation with dask TSQR.
    
    Parameters:
    -----------
    A : array-like
        Original input matrix
    other_Q, other_R : array-like
        Q and R matrices from another TSQR implementation
    tolerance : float, optional
        Tolerance for comparison (default: 1e-10)
    chunk_size : int, optional
        Chunk size for dask array conversion
    
    Returns:
    --------
    bool
        True if results are equivalent within tolerance, False otherwise
    """
    # Get dask TSQR results
    Q_dask, R_dask = tsqr_decomposition(A, chunk_size)
    Q_dask_comp = Q_dask.compute()
    R_dask_comp = R_dask.compute()
    
    # Convert other results to numpy if needed
    if hasattr(other_Q, 'compute'):
        other_Q = other_Q.compute()
    if hasattr(other_R, 'compute'):
        other_R = other_R.compute()
    
    other_Q = np.asarray(other_Q)
    other_R = np.asarray(other_R)
    
    # QR decomposition is not unique - Q can differ by column signs
    # So we compare the reconstructed matrices A = Q @ R
    A_dask_reconstructed = Q_dask_comp @ R_dask_comp
    A_other_reconstructed = other_Q @ other_R
    
    # Compare reconstructions
    reconstruction_diff = np.linalg.norm(A_dask_reconstructed - A_other_reconstructed)
    
    return reconstruction_diff < tolerance

# --- DAG Tasks --- 
@DAGTask
def local_qr(block: np.ndarray):
    Q, R = np.linalg.qr(block, mode='reduced')
    return Q, R

@DAGTask
def get_Q(qr_result):
    return qr_result[0]

@DAGTask
def get_R(qr_result):
    return qr_result[1]

@DAGTask
def merge_r_factors(*R_blocks):
    stacked_R = np.vstack(R_blocks)
    Q_R, R_final = np.linalg.qr(stacked_R, mode='reduced')
    return Q_R, R_final

@DAGTask
def get_Q_R(merge_result):
    return merge_result[0]

@DAGTask
def get_R_final(merge_result):
    return merge_result[1]

@DAGTask
def adjust_local_q(Q_block, Q_R, block_index, rows_per_block):
    """Fixed: Use the Q_R matrix to properly adjust local Q matrices"""
    # Extract the corresponding block from Q_R
    start_row = block_index * rows_per_block
    end_row = start_row + Q_block.shape[1]  # Q_block has shape (rows_per_block, n_cols)
    Q_R_block = Q_R[start_row:end_row, :]
    
    # Correct adjustment: Q_adjusted = Q_local @ Q_R_block
    return Q_block @ Q_R_block

@DAGTask
def merge_q_blocks(*Q_blocks):
    return np.vstack(Q_blocks)

@DAGTask
def combine_qr_results(Q_final, R_final):
    """Single sink task that combines Q and R results"""
    return Q_final, R_final

# # --- Prepare input matrix locally --- 
A = np.random.rand(1024, 8)
n_blocks = 16
rows_per_block = A.shape[0] // n_blocks

# --- Partition matrix into 16 blocks locally ---
block0 = A[0*rows_per_block:1*rows_per_block, :]
block1 = A[1*rows_per_block:2*rows_per_block, :]
block2 = A[2*rows_per_block:3*rows_per_block, :]
block3 = A[3*rows_per_block:4*rows_per_block, :]
block4 = A[4*rows_per_block:5*rows_per_block, :]
block5 = A[5*rows_per_block:6*rows_per_block, :]
block6 = A[6*rows_per_block:7*rows_per_block, :]
block7 = A[7*rows_per_block:8*rows_per_block, :]
block8 = A[8*rows_per_block:9*rows_per_block, :]
block9 = A[9*rows_per_block:10*rows_per_block, :]
block10 = A[10*rows_per_block:11*rows_per_block, :]
block11 = A[11*rows_per_block:12*rows_per_block, :]
block12 = A[12*rows_per_block:13*rows_per_block, :]
block13 = A[13*rows_per_block:14*rows_per_block, :]
block14 = A[14*rows_per_block:15*rows_per_block, :]
block15 = A[15*rows_per_block:, :]

# --- DAG: local QR for each block ---
qr0 = local_qr(block0)
qr1 = local_qr(block1)
qr2 = local_qr(block2)
qr3 = local_qr(block3)
qr4 = local_qr(block4)
qr5 = local_qr(block5)
qr6 = local_qr(block6)
qr7 = local_qr(block7)
qr8 = local_qr(block8)
qr9 = local_qr(block9)
qr10 = local_qr(block10)
qr11 = local_qr(block11)
qr12 = local_qr(block12)
qr13 = local_qr(block13)
qr14 = local_qr(block14)
qr15 = local_qr(block15)

# Extract Q and R from each qr result
Q0, R0 = get_Q(qr0), get_R(qr0)
Q1, R1 = get_Q(qr1), get_R(qr1)
Q2, R2 = get_Q(qr2), get_R(qr2)
Q3, R3 = get_Q(qr3), get_R(qr3)
Q4, R4 = get_Q(qr4), get_R(qr4)
Q5, R5 = get_Q(qr5), get_R(qr5)
Q6, R6 = get_Q(qr6), get_R(qr6)
Q7, R7 = get_Q(qr7), get_R(qr7)
Q8, R8 = get_Q(qr8), get_R(qr8)
Q9, R9 = get_Q(qr9), get_R(qr9)
Q10, R10 = get_Q(qr10), get_R(qr10)
Q11, R11 = get_Q(qr11), get_R(qr11)
Q12, R12 = get_Q(qr12), get_R(qr12)
Q13, R13 = get_Q(qr13), get_R(qr13)
Q14, R14 = get_Q(qr14), get_R(qr14)
Q15, R15 = get_Q(qr15), get_R(qr15)

# --- Merge R factors (now returns both Q_R and R_final) ---
merge_result = merge_r_factors(R0, R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15)
Q_R = get_Q_R(merge_result)
R_final = get_R_final(merge_result)

# --- Adjust local Qs using Q_R ---
Q0_adj = adjust_local_q(Q0, Q_R, 0, rows_per_block)
Q1_adj = adjust_local_q(Q1, Q_R, 1, rows_per_block)
Q2_adj = adjust_local_q(Q2, Q_R, 2, rows_per_block)
Q3_adj = adjust_local_q(Q3, Q_R, 3, rows_per_block)
Q4_adj = adjust_local_q(Q4, Q_R, 4, rows_per_block)
Q5_adj = adjust_local_q(Q5, Q_R, 5, rows_per_block)
Q6_adj = adjust_local_q(Q6, Q_R, 6, rows_per_block)
Q7_adj = adjust_local_q(Q7, Q_R, 7, rows_per_block)
Q8_adj = adjust_local_q(Q8, Q_R, 8, rows_per_block)
Q9_adj = adjust_local_q(Q9, Q_R, 9, rows_per_block)
Q10_adj = adjust_local_q(Q10, Q_R, 10, rows_per_block)
Q11_adj = adjust_local_q(Q11, Q_R, 11, rows_per_block)
Q12_adj = adjust_local_q(Q12, Q_R, 12, rows_per_block)
Q13_adj = adjust_local_q(Q13, Q_R, 13, rows_per_block)
Q14_adj = adjust_local_q(Q14, Q_R, 14, rows_per_block)
Q15_adj = adjust_local_q(Q15, Q_R, 15, rows_per_block)

# --- Merge all Q blocks ---
Q_final = merge_q_blocks(Q0_adj, Q1_adj, Q2_adj, Q3_adj, Q4_adj, Q5_adj, Q6_adj, Q7_adj,
                         Q8_adj, Q9_adj, Q10_adj, Q11_adj, Q12_adj, Q13_adj, Q14_adj, Q15_adj)

# --- Single sink task combining Q and R ---
final_result = combine_qr_results(Q_final, R_final)

# --- Run workflow ---
start_time = time.time()
Q, R = final_result.compute(dag_name="tsqr", config=WORKER_CONFIG)
print(f"Q shape: {Q.shape}, R shape: {R.shape} | User waited: {time.time() - start_time:.3f}s")
print("Is Correct: ", compare_tsqr_results(A, Q, R))
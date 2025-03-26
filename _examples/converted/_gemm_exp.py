import numpy as np

def create_matrix_chunks(matrix, row_chunk_size=1, col_chunk_size=1):
    """Split matrix into smaller chunks based on specified sizes"""
    chunks = []
    for i in range(0, matrix.shape[0], row_chunk_size):
        for j in range(0, matrix.shape[1], col_chunk_size):
            chunk = matrix[i:i+row_chunk_size, j:j+col_chunk_size]
            chunks.append(((i, j), chunk))  # Store position and chunk
    return chunks

def multiply_chunks(a_chunk, b_chunk):
    """Multiply two matrix chunks"""
    return np.matmul(a_chunk, b_chunk)

def aggregate_results(partial_results, final_shape):
    """Combine all partial results into final matrix"""
    result = np.zeros(final_shape)
    for position, value in partial_results:
        i, j = position
        rows, cols = value.shape
        result[i:i+rows, j:j+cols] = value
    return result

# Original matrices
matrix_a = np.array([
    [5, 2, 8, 1],
    [3, 6, 4, 9],
    [7, 2, 5, 3]
])

matrix_b = np.array([
    [4, 7],
    [2, 1],
    [5, 3],
    [8, 6]
])

# Create chunks - process A by rows and B by columns
a_chunks = create_matrix_chunks(matrix_a, row_chunk_size=1, col_chunk_size=matrix_a.shape[1])
b_chunks = create_matrix_chunks(matrix_b, row_chunk_size=matrix_b.shape[0], col_chunk_size=1)

# Multiply corresponding chunks
partial_results = []
for (i_a, _), a_chunk in a_chunks:
    for (_, j_b), b_chunk in b_chunks:
        # Multiply chunks and store result with position
        product = multiply_chunks(a_chunk, b_chunk)
        partial_results.append(((i_a, j_b), product))

distributed_result = aggregate_results(partial_results, (matrix_a.shape[0], matrix_b.shape[1]))

correct_result = np.matmul(matrix_a, matrix_b)

print("Original multiplication result:")
print(correct_result)
print("\nDistributed computation result:")
print(distributed_result)
print("\nResults match:", np.allclose(correct_result, distributed_result))
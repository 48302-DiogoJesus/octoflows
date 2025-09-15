import os
import sys
import time
from typing import List
import numpy as np
from PIL import Image, ImageFilter

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import WORKER_CONFIG

def _split_image(img: np.ndarray) -> List[np.ndarray]:
    """Split image into 16 fixed chunks (4x4 grid)."""
    h, w = img.shape[:2]
    chunks = []
    h_step = h // 4
    w_step = w // 4
    for i in range(4):
        for j in range(4):
            chunk = img[i*h_step:(i+1)*h_step, j*w_step:(j+1)*w_step].copy()
            chunks.append(chunk)
    return chunks

# --- DAG Tasks ---

# Branch A
@DAGTask
def resize_chunk(chunk: np.ndarray, size=(64, 64)) -> np.ndarray:
    img = Image.fromarray(chunk)
    img_resized = img.resize(size)
    return np.array(img_resized)

@DAGTask
def blur_chunk(chunk: np.ndarray) -> np.ndarray:
    img = Image.fromarray(chunk)
    img_blurred = img.filter(ImageFilter.GaussianBlur(1))
    return np.array(img_blurred)

@DAGTask
def normalize_chunk(chunk: np.ndarray) -> np.ndarray:
    return ((chunk - chunk.min()) / (chunk.max() - chunk.min()) * 255).astype(np.uint8)

# Branch B
@DAGTask
def edge_detect_chunk(chunk: np.ndarray) -> np.ndarray:
    img = Image.fromarray(chunk).convert("L")   # grayscale
    img_edge = img.filter(ImageFilter.FIND_EDGES)
    img_edge = img_edge.resize((64, 64))        # match branch A
    return np.array(img_edge)


@DAGTask
def sharpen_chunk(chunk: np.ndarray) -> np.ndarray:
    img = Image.fromarray(chunk)
    img_sharp = img.filter(ImageFilter.UnsharpMask())
    img_sharp = img_sharp.resize((64, 64))
    return np.array(img_sharp)

# Combine per chunk
@DAGTask
def combine_chunk(branch_a: np.ndarray, branch_b: np.ndarray) -> np.ndarray:
    # Ensure same shape
    if branch_b.shape != branch_a.shape:
        if len(branch_b.shape) == 2:  # grayscale → 3 channels
            branch_b = np.stack([branch_b]*3, axis=-1)
        branch_b = np.array(Image.fromarray(branch_b).resize((branch_a.shape[1], branch_a.shape[0])))
    return ((branch_a.astype(np.float32) + branch_b.astype(np.float32)) / 2).astype(np.uint8)


# Merge two chunks
@DAGTask
def merge_chunks_grid(chunks: list[np.ndarray], grid_size: int = 4) -> np.ndarray:
    """
    Combine a list of chunks into a grid of shape (grid_size x grid_size)
    Assumes chunks are in row-major order.
    """
    rows = []
    for r in range(grid_size):
        row_chunks = chunks[r*grid_size:(r+1)*grid_size]
        # Horizontally stack chunks for this row
        row = np.hstack(row_chunks)
        rows.append(row)
    # Vertically stack rows to form the full image
    full_image = np.vstack(rows)
    return full_image

# Save final image
def _save_image(img: np.ndarray, path: str) -> str:
    Image.fromarray(img).save(path)
    return path

# --- Define Workflow ---

input_file = "../_inputs/test_image_2.jpg"
output_file = "../_outputs/image_transform_2.jpg"

img = np.array(Image.open(input_file))

chunks = _split_image(img)

# Fan-out pipelines for each chunk
combined_chunks = []
for i, chunk in enumerate(chunks):
    # Branch A
    a1 = resize_chunk(chunk)
    a2 = blur_chunk(a1)
    a3 = normalize_chunk(a2)
    # Branch B
    b1 = edge_detect_chunk(chunk)
    b2 = sharpen_chunk(b1)
    # Combine per chunk
    combined = combine_chunk(a3, b2)
    combined_chunks.append(combined)

final_img = merge_chunks_grid(combined_chunks, grid_size=4)

# --- Run Workflow ---
# final_img.visualize_dag(output_file=os.path.join("_dag_visualization", "image_transformations"), open_after=True)

start_time = time.time()
result = final_img.compute(dag_name="image_transformations", config=WORKER_CONFIG)
print(f"Image Transformations Complete | Makespan: {time.time() - start_time:.3f}s")

# Sink
saved = _save_image(result, output_file)
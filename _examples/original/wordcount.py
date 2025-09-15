import os
import sys
import time
from typing import List

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import WORKER_CONFIG


def _split_text(text: str, num_chunks: int) -> List[str]:
    """Split text into exactly num_chunks chunks (by words)."""
    words = text.split()
    n = len(words)
    chunk_size = max(1, n // num_chunks)
    chunks = []
    for i in range(0, n, chunk_size):
        chunks.append(" ".join(words[i:i+chunk_size]))
        if len(chunks) == num_chunks:
            break
    while len(chunks) < num_chunks:
        chunks.append("")
    return chunks
    

# --- DAG Tasks ---

@DAGTask
def word_count_chunk(chunk: str) -> int:
    return len([w for w in chunk.split() if w.strip()])


@DAGTask
def merge_counts(counts: List[int]) -> int:
    return sum(counts)


@DAGTask
def final_report(total_count: int) -> str:
    return f"Total word count: {total_count}"


# --- Define Workflow ---

NUMBER_OF_CHUNKS = 16

input_file = "../_inputs/shakespeare.txt"
with open(input_file, "r", encoding="utf-8") as f:
    text_data = f.read()

chunks = _split_text(text_data, NUMBER_OF_CHUNKS)

# Fan-out: NUMBER_OF_CHUNKS parallel word count tasks
wc1 = word_count_chunk(chunks[0])
wc2 = word_count_chunk(chunks[1])
wc3 = word_count_chunk(chunks[2])
wc4 = word_count_chunk(chunks[3])
wc5 = word_count_chunk(chunks[4])
wc6 = word_count_chunk(chunks[5])
wc7 = word_count_chunk(chunks[6])
wc8 = word_count_chunk(chunks[7])
wc9 = word_count_chunk(chunks[8])
wc10 = word_count_chunk(chunks[9])
wc11 = word_count_chunk(chunks[10])
wc12 = word_count_chunk(chunks[11])
wc13 = word_count_chunk(chunks[12])
wc14 = word_count_chunk(chunks[13])
wc15 = word_count_chunk(chunks[14])
wc16 = word_count_chunk(chunks[15])

# Level 1 merges (8 pairs)
m1 = merge_counts([wc1, wc2])
m2 = merge_counts([wc3, wc4])
m3 = merge_counts([wc5, wc6])
m4 = merge_counts([wc7, wc8])
m5 = merge_counts([wc9, wc10])
m6 = merge_counts([wc11, wc12])
m7 = merge_counts([wc13, wc14])
m8 = merge_counts([wc15, wc16])

# Level 2 merges (4 groups)
m9  = merge_counts([m1, m2])
m10 = merge_counts([m3, m4])
m11 = merge_counts([m5, m6])
m12 = merge_counts([m7, m8])

# Level 3 merges (2 groups)
m13 = merge_counts([m9, m10])
m14 = merge_counts([m11, m12])

# Level 4 merge (root)
m15 = merge_counts([m13, m14])

# Sink
report = final_report(m15)

report.visualize_dag(output_file=os.path.join("_dag_visualization", "wordcount"), open_after=False)
exit()

# --- Run workflow ---
start_time = time.time()
result = report.compute(dag_name="wordcount", config=WORKER_CONFIG, open_dashboard=False)
print(f"Result: {result} | User waited: {time.time() - start_time:.3f}s")

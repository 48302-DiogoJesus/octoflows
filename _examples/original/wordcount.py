import hashlib
import json
import os
import sys
import time
import re
from collections import Counter
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.dag_task_node import DAGTask

# Import common worker configurations
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.worker_config import (
    get_local_worker_config,
    get_docker_worker_config,
    get_redis_storage_config,
    IN_MEMORY_STORAGE_CONFIG,
    REDIS_INTERMEDIATE_STORAGE_CONFIG,
    REDIS_METRICS_STORAGE_CONFIG
)
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration

# Get storage configurations
redis_intermediate_storage_config = get_redis_storage_config(port=6379)
redis_metrics_storage_config = get_redis_storage_config(port=6380)
inmemory_intermediate_storage_config = IN_MEMORY_STORAGE_CONFIG

# Get worker configurations
localWorkerConfig = get_local_worker_config(
    intermediate_storage_config=redis_intermediate_storage_config
)

# Configure Docker worker with specific resource requirements
dockerWorkerConfig = get_docker_worker_config(
    planner_type="first",
    intermediate_storage_config=redis_intermediate_storage_config,
    metrics_storage_config=redis_metrics_storage_config,
    docker_gateway_address="http://localhost:5000",
    worker_resource_configuration=TaskWorkerResourceConfiguration(cpus=3, memory_mb=512)
)

def read_and_chunk_text(file_path: str, chunk_size: int) -> list[str]:
    with open(file_path, 'r', encoding='utf-8') as file:
        text = file.read()
    
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        
        # If we're not at the end of the text, adjust end to nearest word boundary
        if end < len(text):
            # Look backwards to find a space or punctuation
            while end > start and not text[end].isspace():
                end -= 1
        
        chunk = text[start:end].strip()
        
        if chunk:
            chunks.append(chunk)
        
        start = end
    
    return chunks

@DAGTask
def preprocess_text(text: str) -> list[str]:
    """
    1. Convert to lowercase
    2. Remove punctuation
    3. Split into words
    4. Remove empty strings
    """
    words = re.findall(r'\b\w+\b', text.lower())
    return words

@DAGTask
def count_words_in_chunk(words: list[str]) -> dict[str, int]:
    return dict(Counter(words))

@DAGTask
def merge_word_counts(counts: list[dict[str, int]]) -> dict[str, int]:
    result = {}
    for count_dict in counts:
        for word, count in count_dict.items():
            result[word] = result.get(word, 0) + count
    return result

def hash_dict(d):
    sorted_dict = dict(sorted(d.items()))
    json_string = json.dumps(sorted_dict, sort_keys=True)
    return hashlib.sha256(json_string.encode()).hexdigest()

if __name__ == "__main__":
    INPUT_FILE = os.path.join("..", "_inputs", "shakespeare.txt")
    OUTPUT_FILE = os.path.join("..", "_outputs", "word_frequencies.txt")
    # CHUNK_SIZE = 10_000
    CHUNK_SIZE = 100_000

    # ! Not part of the workflow (not a DAGTask, as the number of chunks is dynamic)
    text_chunks = read_and_chunk_text(INPUT_FILE, CHUNK_SIZE)
    print(f"Number of chunks: {len(text_chunks)}")

    word_lists = [preprocess_text(chunk) for chunk in text_chunks]
    word_counts = [count_words_in_chunk(words) for words in word_lists]
    final_word_count = merge_word_counts(word_counts)

    start_time = time.time()
    result = final_word_count.compute(dag_name="wordcount", config=dockerWorkerConfig, open_dashboard=False)
    
    # with open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
    #     outfile.write(str(result))
    
    print(f"Wordcount Hash: {hash_dict(result)} | Makespan: {time.time() - start_time}s")

    # top_10 = sorted(result.items(), key=lambda x: x[1], reverse=True)[:10]
    # print("\nTop 10 Words:")
    # for word, count in top_10:
    #     print(f"{word}: {count}")
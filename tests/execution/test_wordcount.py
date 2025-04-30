import hashlib
import json
import os
import sys
import re
from collections import Counter

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from tests.utils.test_utils import get_worker_config
from src.dag_task_node import DAGTask

worker_config = get_worker_config()

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

def correct_wordcount(text: str) -> dict[str, int]:
    words = re.findall(r'\b\w+\b', text.lower())
    word_counts = {}
    for word in words:
        word_counts[word] = word_counts.get(word, 0) + 1
    return word_counts

def test_wordcount_shakespeare():
    INPUT_FILE = os.path.join(os.path.dirname(__file__), "_inputs", "shakespeare.txt")
    CHUNK_SIZE = 10_000

    text_chunks = read_and_chunk_text(INPUT_FILE, CHUNK_SIZE)

    word_lists = [preprocess_text(chunk) for chunk in text_chunks]
    word_counts = [count_words_in_chunk(words) for words in word_lists]
    final_word_count = merge_word_counts(word_counts)

    distributed_result = final_word_count.compute(config=worker_config, open_dashboard=False)
    distributed_result_hash = hash_dict(distributed_result)

    with open(INPUT_FILE, 'r', encoding='utf-8') as file:
        text = file.read()
    correct_result_hash = hash_dict(correct_wordcount(text))
    assert distributed_result_hash == correct_result_hash

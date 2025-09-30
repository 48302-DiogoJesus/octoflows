import sys
import random
import numpy as np
from collections import deque

def deep_getsizeof(obj) -> int:
    """Recursively calculate the memory footprint of an object and its contents."""
    seen_ids = set()
    size = 0
    queue = deque([obj])

    while queue:
        current = queue.popleft()
        if id(current) in seen_ids:
            continue
        seen_ids.add(id(current))

        size += sys.getsizeof(current)

        if isinstance(current, dict):
            queue.extend(list(current.keys()))
            queue.extend(list(current.values()))
        elif isinstance(current, (list, tuple, set, frozenset)):
            queue.extend(current)

    return size

def calculate_data_structure_size_bytes(obj, fast=True, sample_size=50) -> int:
    """
    Calculate size of data structure
    returns in bytes
    """
    if isinstance(obj, np.ndarray):
        return obj.nbytes
    
    elif isinstance(obj, (tuple, list)):
        container_size = deep_getsizeof(obj)
        
        if not obj:
            return container_size
        
        # Fast mode: sample items for large collections
        if fast and len(obj) > sample_size:
            sample_items = random.sample(list(obj), sample_size)
            avg_item_size = sum(deep_getsizeof(item) for item in sample_items) / sample_size
            return int(container_size + avg_item_size * len(obj))
        else:
            # Regular mode: calculate all items
            return int(container_size + sum(calculate_data_structure_size_bytes(item, fast, sample_size) for item in obj))
    
    else:
        return int(deep_getsizeof(obj))

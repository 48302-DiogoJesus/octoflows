import sys
import random
import numpy as np

def calculate_data_structure_size(obj, fast=True, sample_size=50) -> int:
    """
    Calculate size of data structure
    returns in bytes
    """
    if isinstance(obj, np.ndarray):
        return obj.nbytes
    
    elif isinstance(obj, (tuple, list)):
        container_size = sys.getsizeof(obj)
        
        if not obj:
            return container_size
        
        # Fast mode: sample items for large collections
        if fast and len(obj) > sample_size:
            sample_items = random.sample(list(obj), sample_size)
            avg_item_size = sum(sys.getsizeof(item) for item in sample_items) / sample_size
            return int(container_size + avg_item_size * len(obj))
        else:
            # Regular mode: calculate all items
            return int(container_size + sum(calculate_data_structure_size(item, fast, sample_size) for item in obj))
    
    else:
        return int(sys.getsizeof(obj))

import sys
from pympler import asizeof

import numpy as np


def calculate_data_structure_size(obj):
    if isinstance(obj, np.ndarray):
        return obj.nbytes
    elif isinstance(obj, tuple) or isinstance(obj, list):
        return sys.getsizeof(obj) + sum(calculate_data_structure_size(item) for item in obj)
    else:
        try:
            return asizeof.asizeof(obj)
        except:
            return sys.getsizeof(obj)
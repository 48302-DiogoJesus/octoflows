import inspect
import sys
from typing import Callable, Optional
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
        
# def get_method_overridden(
#     potential_class_overriding_method: type, 
#     base_method_ref: Callable,
# ) -> Optional[Callable]:
#     # Get the planner's method (without invoking descriptors)
#     planner_method = inspect.getattr_static(potential_class_overriding_method, base_method_ref.__name__, None)
#     if planner_method is None: return None

#     # Unwrap staticmethod if needed
#     base_func = base_method_ref.__func__ if isinstance(base_method_ref, staticmethod) else base_method_ref
#     planner_func = planner_method.__func__ if isinstance(planner_method, staticmethod) else planner_method
    
#     # Return the callable method if it's actually overridden
#     if planner_func.__code__ != base_func.__code__: return planner_method
#     return None
import cloudpickle
from src.utils.utils import calculate_data_structure_size_bytes

data = {
    "a": 2,
    "b": 3
}

data_s = cloudpickle.dumps(data)

print(data_s)
print(len(data_s))
print(calculate_data_structure_size_bytes(data_s))

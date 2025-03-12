import json
import flask
from src.dag_task_node import DAGTask
import numpy as np

@DAGTask
def a(x: int):
    return x + 10

@DAGTask
def b(x: int, y: int):
    data = { "x": x, "y": y }
    data_serialized = json.dumps(data)
    return data_serialized

@DAGTask
def c(data: bytes, extra: int) -> str:
    json_data = json.loads(data)
    x = json_data["x"]
    return f"{(x * 10) + extra}"

@DAGTask
def d(x: str, y: str) -> str:
    return x + "_final_" + y

a1 = a(10)
a2 = a(20)

b1 = b(a1, a2)

c1 = c(b1, 2)
c2 = c(b1, 4)
d1 = d(c1, c2)

# d1.visualize_dag()
result = d1.compute(local=True)
print(f"UserCode | Final Result: {result}")
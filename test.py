import json
from src.TaskNode import Task

@Task
def a(x: int):
    return x + 10

@Task
def b(x: int, y: int):
    return x + y

@Task
def c(x: int) -> str:
    return f"{x * 10}"

@Task
def d(x: int) -> str:
    return f"{x}"

a1 = a(10)
a2 = a(20)

b1 = b(a1, a2)

c1 = c(b1)
d1 = d(c1)

dag_json = d1.dag_json()
# print(json.dumps(dag_json, indent=2))

# d1.dag_visualize()

result = d1.compute()
print(f"Result: {result}")
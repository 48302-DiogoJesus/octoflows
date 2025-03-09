import json
from src.DAG import DAG, DAGTask

@DAGTask
def a(x: int):
    return x + 10

@DAGTask
def b(x: int, y: int):
    return x + y

@DAGTask
def c(x: int, extra: int) -> str:
    return f"{(x * 10) + extra}"

@DAGTask
def d(x: int) -> str:
    return f"{x}"

a1 = a(10)
a2 = a(20)

b1 = b(a1, a2)

c1 = c(b1, 2)
d1 = d(c1)

# dag_json = d1.dag_json()
# print(json.dumps(dag_json, indent=2))

# s = d1.serialize()
# dr: DAGNode[Any] = DAGNode.from_serialized(s)

# res = d1.compute()
dag = DAG(d1)
# dag.visualize()
print(f"Result: {json.dumps(dag.get_json(), indent=2)}")

# d1.dag_visualize()
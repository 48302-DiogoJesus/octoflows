import json
from src.dag import DAG
from src.dag_task_node import DAGTask

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
def d(x: str, y: str) -> str:
    return x + "_final_" + y

a1 = a(10)
a2 = a(20)

b1 = b(a1, a2)

c1 = c(b1, 2)
c2 = c(b1, 4)
d1 = d(c1, c2)

dag = DAG(sink_node=d1)
# dag.visualize()
result = dag.start_local_execution(wait_for_final_result=True)
print(f"DONE | Result: {result} | TypeOf Result: {type(result)}")
import json
from task import task

@task
def incr(x: int):
    return x + 1

@task
def decr(x: int):
    return x - 1

@task
def double(x: int):
    return x * 2

# Linear 3-Node DAG
a = incr(9)
b = decr(a)
c = double(b)


dag_json = c.dag_json()
print(json.dumps(dag_json, indent=2))

result = c.dag_visualize()

result = c.compute()
print(f"Result: {result}")
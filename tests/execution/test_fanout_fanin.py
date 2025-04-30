import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask
from tests.utils.test_utils import get_worker_config

worker_config = get_worker_config()

@DAGTask
def a(x: float, y: float) -> float:
    return x * y

@DAGTask
def b(values: list[float]) -> float:
    return sum(values)

@DAGTask
def c(values: float) -> float:
    return values * values

def test_fanout_fanin():
    a1 = a(2.0, 3.0)
    a2 = a(3.0, 4.0)
    a3 = a(4.0, 5.0)
    b1 = b([a1, a2, a3])
    c1 = c(b1)
    c2 = c(b1)
    c3 = c(b1)
    b2 = b([c1, c2, c3])
    c4 = c(b2)

    for _ in range(10):
        result = c4.compute(config=worker_config)
        assert result == 18766224
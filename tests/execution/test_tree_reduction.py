import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask, DAGTaskNode
from src.utils.logger import create_logger
from tests.utils.test_utils import get_worker_config

logger = create_logger(__name__)

@DAGTask
def add(x: float, y: float) -> float:
    return x + y

def test_tree_reduction_1024():
    L = range(1024)
    while len(L) > 1:
        L = list(map(add, L[0::2], L[1::2]))

    sink: DAGTaskNode = L[0] # type: ignore

    # use iterations to ensure consistency
    for i in range(5):
        start_time = time.time()
        # result = sink.compute(config=localWorkerConfig)
        result = sink.compute(config=get_worker_config())
        assert result == 523776
        logger.info(f"[{i}] Result: ${result} | Makespan: {time.time() - start_time}s")

def test_tree_reduction_4096():
    L = range(4096)
    while len(L) > 1:
        L = list(map(add, L[0::2], L[1::2]))

    sink: DAGTaskNode = L[0] # type: ignore

    # use iterations to ensure consistency
    for i in range(3):
        start_time = time.time()
        # result = sink.compute(config=localWorkerConfig)
        result = sink.compute(config=get_worker_config())
        assert result == 8386560
        logger.info(f"[{i}] Result: ${result} | Makespan: {time.time() - start_time}s")
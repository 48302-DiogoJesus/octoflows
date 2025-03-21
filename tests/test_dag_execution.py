# test_math.py (pytest tests)
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.worker import LocalWorker
from src.storage.in_memory_storage import InMemoryStorage
from src.utils.logger import create_logger
logger = create_logger(__name__)
import src.dag_task_node as dag_task_node

localWorkerConfig = LocalWorker.Config(intermediate_storage_config=InMemoryStorage.Config())

@dag_task_node.DAGTask
def task_a(prev: str, append: str) -> str:
    return f"{prev}-{append}"

def test_execution_dag_no_fan_ins_no_fan_outs():
    t1 = task_a("", "1")
    t2 = task_a(t1, "2")
    t3 = task_a(t2, "3")
    t4 = task_a(t3, "4")
    t5 = task_a(t4, "5")

    result = t5.compute(config=localWorkerConfig)
    assert result == "-1-2-3-4-5"

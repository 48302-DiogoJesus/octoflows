import os
import sys

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag.dag import MultipleSinkNodesError
from tests.utils.test_utils import get_planner, get_worker_config
from src.utils.logger import create_logger
import src.dag_task_node as dag_task_node

logger = create_logger(__name__)
worker_config = get_worker_config()
selected_planner = get_planner()

@dag_task_node.DAGTask
def task_a(prev: str, append: str) -> str:
    return f"{prev}-{append}"

def test_execution_dag_no_fan_ins_no_fan_outs():
    t1 = task_a("", "1")
    t2 = task_a(t1, "2")
    t3 = task_a(t2, "3")
    t4 = task_a(t3, "4")
    t5 = task_a(t4, "5")

    result = t5.compute(config=worker_config)
    assert result == "-1-2-3-4-5"

def test_dag_2():
    """ 
    DAG where the last task depends on a task that one of its upstream tasks also depends on.
    Cutting the subdag naively would make this not work
    """
    from src.dag.dag import FullDAG
    t1 = task_a("", "1")
    t2 = task_a("", "2")
    t3 = task_a(t1, t2)
    t4 = task_a(t1, t3)

    dag = FullDAG(sink_node=t4)
    assert dag.root_nodes
    assert len(dag.root_nodes) == 2
    assert len(dag._all_nodes) == 4

    res = t4.compute(config=worker_config)
    assert res == "-1--1--2"

def test_dag_fake_sink_node():
    """ 
    DAG where the last task depends on a task that one of its upstream tasks also depends on.
    Cutting the subdag naively would make this not work
    """
    from src.dag.dag import FullDAG
    t1 = task_a("", "1")
    t2 = task_a(t1, "2")
    t3 = task_a(t1, "3")

    with pytest.raises(MultipleSinkNodesError):
        FullDAG(sink_node=t3)

def test_dag_execution_root_node_ahead():
    t1 = task_a("", "1")
    t2 = task_a(t1, "")
    t3 = task_a(t1, "")
    t4 = task_a(t2, "")
    t6 = task_a("", "6") # loose node/root node ahead
    t5 = task_a(t3, t6)
    t7 = task_a(t4, t5)

    result = t7.compute(config=worker_config)

    assert result == "-1----1---6"
# test_math.py (pytest tests)
import os
import sys


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.utils.logger import create_logger
import src.dag_task_node as dag_task_node

logger = create_logger(__name__)

@dag_task_node.DAGTask
def task_a(input: str) -> str:
    return f"{input}b"

@dag_task_node.DAGTask
def task_b(input: str) -> str:
    return f"{input}"

def test_dag_task_cloning():
    # t1 => [t2, t3]
    t1 = task_a("1")
    t2 = task_a(t1)
    t3 = task_a(t1)

    t3_clone = t3.clone()

    t3.upstream_nodes = []
    assert len(t3.upstream_nodes) == 0
    assert len(t3_clone.upstream_nodes) == 1
    t1_clone = t3_clone.upstream_nodes[0]
    
    t2_clone = t1_clone.downstream_nodes[0]
    assert t2_clone.id.get_full_id() == t2.id.get_full_id()
    t2_clone.upstream_nodes = []
    assert len(t2_clone.upstream_nodes) == 0
    assert len(t2.upstream_nodes) == 1

    # modify t1 clone, inside t3 clone. To ensure if the original t1 was modified
    t1_clone.downstream_nodes = []
    assert len(t1_clone.downstream_nodes) == 0
    assert len(t1.downstream_nodes) == 2

def test_dag_no_fan_ins_no_fan_outs():
    from src.dag.dag import FullDAG
    t1 = task_a("1")
    t2 = task_a(t1)
    t3 = task_a(t2)
    t4 = task_a(t3)
    t5 = task_a(t4)

    dag = FullDAG(sink_node=t5)

    assert dag.root_nodes
    assert len(dag.root_nodes) == 1
    assert len(dag._all_nodes) == 5
    assert len(t1.upstream_nodes) == 0
    assert len(t1.downstream_nodes) == 1
    assert len(t2.upstream_nodes) == 1
    assert len(t2.downstream_nodes) == 1
    assert len(t3.upstream_nodes) == 1
    assert len(t3.downstream_nodes) == 1
    assert len(t4.upstream_nodes) == 1
    assert len(t4.downstream_nodes) == 1
    assert len(t5.upstream_nodes) == 1
    assert len(t5.downstream_nodes) == 0

def test_dag_root_node_ahead():
    from src.dag.dag import FullDAG
    t1 = task_a("1")
    t2 = task_a(t1)
    t3 = task_a(t1)
    t4 = task_a(t2)
    t6 = task_b("") # loose node/root node ahead
    t5 = task_a(t3, t6)
    t7 = task_a(t4, t5)

    dag = FullDAG(sink_node=t7)

    assert dag.root_nodes
    assert len(dag.root_nodes) == 2
    assert len(dag._all_nodes) == 7
from dataclasses import dataclass
from src.planning.dag_planner import DAGPlanner
from src.planning.metadata_access.metadata_access import MetadataAccess
from src.utils.logger import create_logger
from src.workers.worker_execution_logic import WorkerExecutionLogic

logger = create_logger(__name__, prefix="PLANNING")

class DummyDAGPlanner(DAGPlanner, WorkerExecutionLogic):
    @dataclass
    class Config(DAGPlanner.Config):
        def create_instance(self) -> "DummyDAGPlanner":
            return DummyDAGPlanner(self)
        
    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    def plan(self, dag, metadata_access: MetadataAccess):
        """
        dag: dag.DAG

        This planning algorithm:
        - Assigns the best resource config to each node in the DAG
        - Finds the critical path
        - Simulates downgrading resource configs of tasks outside the critical path without affecting the critical path significantly

        """
        from src.dag.dag import FullDAG
        _dag: FullDAG = dag
        topo_sorted_nodes = self._topological_sort(_dag)
        self._visualize_plan(_dag)
        self.validate_plan(_dag.root_nodes)
        # !!! FOR QUICK TESTING ONLY. REMOVE LATER !!!
        exit()
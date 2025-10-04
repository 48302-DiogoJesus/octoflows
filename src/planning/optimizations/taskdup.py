import asyncio
from dataclasses import dataclass, field
import time
from src.dag.dag import SubDAG
from src.task_optimization import TaskOptimization
from src.dag_task_node import DAGTaskNode, DAGTaskNodeId
from src.storage.storage import Storage
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.utils.logger import create_logger
from src.utils.errors import CancelCurrentWorkerLoopException
from src.storage.metadata.metrics_types import TaskOptimizationMetrics

logger = create_logger(__name__)

DUPPABLE_TASK_STARTED_PREFIX = "taskdup-task-started-"
DUPPABLE_TASK_TIME_SAVED_THRESHOLD_MS = 500 # the least amount of time we need to save to justify duplication

@dataclass
class TaskDupOptimization(TaskOptimization, WorkerExecutionLogic):
    """ 
    Indicates that this task can be duplicated IF NEEDED (decided at runtime)
    """

    @dataclass
    class OptimizationMetrics(TaskOptimizationMetrics):
        dupped: DAGTaskNodeId

    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    
    @property
    def name(self) -> str: return "TaskDup"

    def clone(self): return TaskDupOptimization()

    @staticmethod
    def planning_assignment_logic(planner, dag, predictions_provider, nodes_info: dict, topo_sorted_nodes: list[DAGTaskNode]):
        from src.planning.abstract_dag_planner import AbstractDAGPlanner
        _nodes_info: dict[str, AbstractDAGPlanner.PlanningTaskInfo] = nodes_info
        for node_info in _nodes_info.values():
            if node_info.node_ref.try_get_optimization(TaskDupOptimization): 
                # Skip if node already has TaskDup annotation. Cloud have been added by the user
                continue
            node_has_at_least_one_downstream_with_diff_resources = any([dnode.worker_config.worker_id is None or dnode.worker_config.worker_id != node_info.node_ref.worker_config.worker_id for dnode in node_info.node_ref.downstream_nodes])
            if not node_has_at_least_one_downstream_with_diff_resources: continue
            if len(node_info.node_ref.downstream_nodes) == 0: continue
            node_info.node_ref.add_optimization(TaskDupOptimization())

    @staticmethod
    async def wel_before_task_handling(planner, this_worker, metadata_storage: Storage, subdag: SubDAG, current_task: DAGTaskNode):
        is_duppable = current_task.try_get_optimization(TaskDupOptimization) is not None
        if is_duppable: await metadata_storage.set(f"{DUPPABLE_TASK_STARTED_PREFIX}{current_task.id.get_full_id_in_dag(subdag)}", time.time())

    @staticmethod
    async def wel_override_should_upload_output(planner, current_task, subdag: SubDAG, this_worker, metadata_storage: Storage, is_dupping: bool):
        if not is_dupping: return None # don't care
        if is_dupping: 
            raise CancelCurrentWorkerLoopException("This task was dupped, don't continue branch")

import asyncio
from dataclasses import dataclass, field
import time
from src.dag.dag import SubDAG
from src.task_optimization import TaskOptimization
from src.dag_task_node import DAGTaskNode, DAGTaskNodeId
from src.storage.storage import Storage
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.utils.logger import create_logger
from src.storage.metadata.metrics_types import TaskOptimizationMetrics

logger = create_logger(__name__)

DUPPABLE_TASK_STARTED_PREFIX = "taskdup-task-started-"
DUPPABLE_TASK_TIME_SAVED_THRESHOLD_MS = 150 # the least amount of time we need to save to justify duplication

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
            node_has_at_least_one_downstream_with_diff_worker = any([dnode.worker_config.worker_id is None or dnode.worker_config.worker_id != node_info.node_ref.worker_config.worker_id for dnode in node_info.node_ref.downstream_nodes])
            if not node_has_at_least_one_downstream_with_diff_worker: continue
            if len(node_info.node_ref.downstream_nodes) == 0: continue
            node_info.node_ref.add_optimization(TaskDupOptimization())

    @staticmethod
    async def wel_before_task_handling(planner, this_worker, metadata_storage: Storage, subdag: SubDAG, current_task: DAGTaskNode, is_dupping: bool):
        is_duppable = current_task.try_get_optimization(TaskDupOptimization) is not None
        if is_duppable and not is_dupping: await metadata_storage.set(f"{DUPPABLE_TASK_STARTED_PREFIX}{current_task.id.get_full_id_in_dag(subdag)}", time.time())

    @staticmethod
    async def wel_override_should_upload_output(planner, current_task, subdag: SubDAG, this_worker, metadata_storage: Storage, is_dupping: bool):
        if not is_dupping: return None # don't care
        if is_dupping: return False

    @staticmethod
    async def wel_update_dependency_counters(planner, this_worker, metadata_storage, subdag, current_task: DAGTaskNode, is_dupping: bool) -> list | None:
        # idea: for each dtask w/ my worker_id, if dtask has duppable utasks AND DC not ready => use exists() to know which tasks are ready and check if the non-ready are present locally
        from src.storage.prefixes import DEPENDENCY_COUNTER_PREFIX
        if not is_dupping: return None

        downstream_tasks_ready = []
        for dtask in current_task.downstream_nodes:
            if dtask.cached_result is not None: continue
            dependencies_met = await metadata_storage.storage.get(f"{DEPENDENCY_COUNTER_PREFIX}{dtask.id.get_full_id_in_dag(subdag)}")
            dependencies_met = int(dependencies_met) if dependencies_met is not None else 0
            # dupped tasks don't update DC, so if there's only one dependency missing, it's this task, so it's ready
            if dependencies_met == len(dtask.upstream_nodes) - 1:
                downstream_tasks_ready.append(dtask)
        return downstream_tasks_ready


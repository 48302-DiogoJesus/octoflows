import asyncio
from dataclasses import dataclass, field
import time
from src.dag.dag import SubDAG
from src.task_optimization import TaskOptimization
from src.dag_task_node import DAGTaskNode
from src.storage.storage import Storage
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.utils.logger import create_logger
from src.utils.errors import CancelCurrentWorkerLoopException

logger = create_logger(__name__)

# if task execution time exceeds this, don't allow dupping. Short tasks are better for dupping
DUPPABLE_TASK_MAX_EXEC_TIME_MS: float = 2_000
# if task input size exceeds 10MB, don't allow dupping
DUPPABLE_TASK_MAX_INPUT_SIZE: int = 10 * 1024 * 1024
DUPPABLE_TASK_STARTED_PREFIX = "taskdup-task-started-"
DUPPABLE_TASK_TIME_SAVED_THRESHOLD_MS = 1_500 # the least amount of time we need to save to justify duplication

@dataclass
class TaskDupOptimization(TaskOptimization, WorkerExecutionLogic):
    """ 
    Indicates that this task can be duplicated IF NEEDED (decided at runtime)
    """

    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    
    @property
    def name(self) -> str: return "TaskDup"

    def clone(self): return TaskDupOptimization()

    @staticmethod
    def planning_assignment_logic(planner, dag, predictions_provider, nodes_info: dict, topo_sorted_nodes: list[DAGTaskNode]):
        for node_info in nodes_info.values():
            if node_info.node_ref.try_get_optimization(TaskDupOptimization): 
                # Skip if node already has TaskDup annotation. Cloud have been added by the user
                continue
            if len(node_info.node_ref.downstream_nodes) == 0: continue
            if node_info.tp_exec_time_ms > DUPPABLE_TASK_MAX_EXEC_TIME_MS: continue
            if node_info.deserialized_input_size > DUPPABLE_TASK_MAX_INPUT_SIZE: continue
            node_info.node_ref.add_optimization(TaskDupOptimization())
        return

    @staticmethod
    async def wel_before_task_handling(planner, this_worker, metadata_storage: Storage, subdag: SubDAG, current_task: DAGTaskNode, is_dupping: bool):
        is_duppable = current_task.try_get_optimization(TaskDupOptimization) is not None
        if is_duppable: await metadata_storage.set(f"{DUPPABLE_TASK_STARTED_PREFIX}{current_task.id.get_full_id_in_dag(subdag)}", time.time())

    @staticmethod
    async def wel_before_task_execution(planner, this_worker, metadata_storage: Storage, subdag: SubDAG, current_task, is_dupping: bool):
        is_duppable = current_task.try_get_optimization(TaskDupOptimization) is not None
        # set the cancellation flag to notify other workers to not execute this task. if all inputs are available, then we can dup the task. Warn others that they MAY NOT need to execute it

    @staticmethod
    async def wel_override_should_upload_output(planner, current_task, subdag: SubDAG, this_worker, metadata_storage: Storage, is_dupping: bool):
        if not is_dupping: return None # don't care
        if is_dupping: 
            raise CancelCurrentWorkerLoopException("This task was dupped, don't continue branch")

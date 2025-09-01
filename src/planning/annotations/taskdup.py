import asyncio
from dataclasses import dataclass, field
import time
from src.dag.dag import SubDAG
from src.dag_task_annotation import TaskAnnotation
from src.dag_task_node import DAGTaskNode
from src.storage.storage import Storage
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.utils.logger import create_logger

logger = create_logger(__name__)

# if task execution time exceeds this, don't allow dupping. Short tasks are better for dupping
DUPPABLE_TASK_MAX_EXEC_TIME_MS: float = 2_000
# if task input size exceeds 5MB, don't allow dupping
DUPPABLE_TASK_MAX_INPUT_SIZE: int = 1024 * 1024 * 5
DUPPABLE_TASK_STARTED_PREFIX = "taskdup-task-started-"
DUPPABLE_TASK_CANCELLATION_PREFIX = "taskdup-cancellation-"
DUPPABLE_TASK_TIME_SAVED_THRESHOLD_MS = 0 # the least amount of time we need to save to justify duplication

@dataclass
class TaskDupOptimization(TaskAnnotation, WorkerExecutionLogic):
    """ 
    Indicates that this task can be duplicated IF NEEDED (decided at runtime)
    """

    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def clone(self): return TaskDupOptimization()

    @staticmethod
    async def override_before_task_handling(this_worker, metadata_storage: Storage, subdag: SubDAG, current_task: DAGTaskNode):
        taskdup_annotation = current_task.try_get_annotation(TaskDupOptimization)
        is_duppable = taskdup_annotation is not None
        if not is_duppable: return

        await metadata_storage.set(f"{DUPPABLE_TASK_STARTED_PREFIX}{current_task.id.get_full_id_in_dag(subdag)}", time.time())

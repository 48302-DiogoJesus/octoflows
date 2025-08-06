import asyncio
from dataclasses import dataclass, field
from types import CoroutineType
from typing import Any
import time
import cloudpickle
from src.dag.dag import FullDAG, SubDAG
from src.dag_task_annotation import TaskAnnotation
from src.dag_task_node import _CachedResultWrapper, DAGTaskNode
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.events import TASK_COMPLETION_EVENT_PREFIX
from src.storage.storage import Storage
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.utils.logger import create_logger

logger = create_logger(__name__)

@dataclass
class TaskDupOptimization(TaskAnnotation, WorkerExecutionLogic):
    """ 
    Indicates that this task can be duplicated if needed (decided at runtime)
    """
    TASK_STARTED_PREFIX = "taskdup-task-started-"
    TASK_DUP_CANCELLATION_PREFIX = "taskdup-cancellation-"
    TIME_THRESHOLD_MS = 500 # the least amount of time we need to save to justify duplication

    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def clone(self): return TaskDupOptimization()

    @staticmethod
    async def override_before_task_handling(this_worker, metadata_storage: Storage, subdag: SubDAG, current_task: DAGTaskNode):
        taskdup_annotation = current_task.try_get_annotation(TaskDupOptimization)
        is_duppable = taskdup_annotation is not None
        if not is_duppable: return

        await metadata_storage.set(f"{TaskDupOptimization.TASK_STARTED_PREFIX}{current_task.id.get_full_id_in_dag(subdag)}", time.time())

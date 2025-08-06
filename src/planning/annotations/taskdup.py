import asyncio
from dataclasses import dataclass, field
from types import CoroutineType
from typing import Any
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

    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def clone(self): return TaskDupOptimization()

    @staticmethod
    async def override_before_task_handling(this_worker, subdag: SubDAG, current_task: DAGTaskNode):
        taskdup_annotation = current_task.try_get_annotation(TaskDupOptimization)
        is_duppable = taskdup_annotation is not None
        can_dup_upstream = any(t.try_get_annotation(TaskDupOptimization) is not None for t in current_task.upstream_nodes)
        if not is_duppable and not can_dup_upstream: return

        if is_duppable:
            # TODO: Set a storage flag with CURRENT timestamp (real_task_start_time)
            pass
        elif can_dup_upstream:
            # TODO: eval if should dup and dup if so
            pass

        pass
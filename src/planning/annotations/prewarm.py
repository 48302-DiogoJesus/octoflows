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
class PreWarmOptimization(TaskAnnotation, WorkerExecutionLogic):
    """ Indicates that the upstream dependencies of a task annotated with this annotation should be downloaded as soon as possible 
    """

    target_resource_config: TaskWorkerResourceConfiguration

    def clone(self): return PreWarmOptimization(self.target_resource_config.clone())

    @staticmethod
    async def override_before_task_handling(this_worker, metadata_storage: Storage, subdag: SubDAG, current_task: DAGTaskNode):
        from src.workers.worker import Worker
        _this_worker: Worker = this_worker
        
        prewarm_annotation = current_task.try_get_annotation(PreWarmOptimization)
        if prewarm_annotation is None: return

        # "fire-and-forget" / non-blocking
        asyncio.create_task(_this_worker.warmup(prewarm_annotation.target_resource_config))

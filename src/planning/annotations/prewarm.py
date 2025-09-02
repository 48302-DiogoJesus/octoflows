import asyncio
from dataclasses import dataclass
from src.dag.dag import  SubDAG
from src.dag_task_annotation import TaskAnnotation
from src.dag_task_node import  DAGTaskNode
from src.planning.annotations.task_worker_resource_configuration import TaskWorkerResourceConfiguration
from src.storage.storage import Storage
from src.workers.worker_execution_logic import WorkerExecutionLogic
from src.utils.logger import create_logger

logger = create_logger(__name__)

@dataclass
class PreWarmOptimization(TaskAnnotation, WorkerExecutionLogic):
    """ Indicates that the upstream dependencies of a task annotated with this annotation should be downloaded as soon as possible 
    """

    target_resource_configs: list[TaskWorkerResourceConfiguration]

    def clone(self): return PreWarmOptimization([config.clone() for config in self.target_resource_configs])

    @staticmethod
    async def wel_before_task_handling(this_worker, metadata_storage: Storage, subdag: SubDAG, current_task: DAGTaskNode):
        from src.workers.worker import Worker
        _this_worker: Worker = this_worker
        
        prewarm_annotation = current_task.try_get_annotation(PreWarmOptimization)
        if prewarm_annotation is None: return

        # "fire-and-forget" / non-blocking
        asyncio.create_task(_this_worker.warmup(prewarm_annotation.target_resource_configs))

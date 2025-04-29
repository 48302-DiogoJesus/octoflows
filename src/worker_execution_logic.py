from abc import ABC, abstractmethod
from typing import Any

from src.dag import dag
from src.dag_task_node import DAGTaskNode
from src.storage.metrics.metrics_storage import TaskInputMetrics
from src.storage.storage import Storage
from src.worker_resource_configuration import TaskWorkerResourceConfiguration

class WorkerExecutionLogic():
    @staticmethod
    def override_handle_inputs(intermediate_storage: Storage, task: DAGTaskNode, subdag: dag.SubDAG, worker_resource_config: TaskWorkerResourceConfiguration | None) -> tuple[dict[str, Any], list[TaskInputMetrics], float]:
        raise NotImplementedError
    
    @staticmethod
    def override_handle_execution(): 
        raise NotImplementedError

    @staticmethod
    def override_handle_output(): 
        raise NotImplementedError

    @staticmethod
    def override_handle_downstream(): 
        raise NotImplementedError
from dataclasses import dataclass

from src.dag_task_node import TaskAnnotation


@dataclass
class TaskWorkerResourcesConfiguration(TaskAnnotation):
    cpus: float
    memory: int
from dataclasses import dataclass

from src.dag_task_node import TaskAnnotation


@dataclass
class TaskWorkerResourceConfiguration(TaskAnnotation):
    """ MANDATORY annotation that doesn't conflict with other annotations """
    cpus: float
    memory_mb: int